#!/bin/bash
# Deploy the SQL Server CDC source and the two JDBC sinks (PostgreSQL, Db2) to the
# two self-managed Connect clusters (source :8083, sinks :8084), which talk to Kafka + Schema Registry in Confluent Cloud.
# Usage: ./deploy-connectors.sh [source|sinks]   (default: all)
# With ENABLE_LIGHTNING_TABLES=true (.env) it then enables Lightning Tables (./lightning-tables.sh).
#
# Sturdiness:
#   * before the source is deployed, SQL Server is health-checked. SQL Server 2019 only ships an x86
#     image, emulated on Apple Silicon, and can hang (commits stuck on log flush). Debezium validates
#     the connection when the connector is created, so a hung server makes the REST call time out.
#     If a hang is detected the mssql container is restarted (MSSQL_AUTO_RESTART=0 to only report it);
#     the data generator reconnects by itself and only tops up missing rows.
#   * a REST call that times out while Connect validates the config is not treated as a failure: the
#     script waits for the connector to appear and retries once if it does not.
#   * at the end it waits until every connector and task is RUNNING and exits 1 if one FAILED.
set -e
cd "$(dirname "$0")"
source scripts/common.sh   # loads .env (ENABLE_LIGHTNING_TABLES, MSSQL_AUTO_RESTART)
CONNECT_SOURCE="${CONNECT_SOURCE_HOST:-localhost:8083}"
CONNECT_SINK="${CONNECT_SINK_HOST:-localhost:8084}"
MSSQL_AUTO_RESTART="${MSSQL_AUTO_RESTART:-1}"
RESP=$(mktemp); trap 'rm -f "$RESP"' EXIT

wait_for_connect() {  # host
  local CONNECT="$1"
  for i in $(seq 1 60); do curl -sf --max-time 5 "http://$CONNECT/" -o /dev/null && return 0; sleep 5; done
  echo "Kafka Connect not ready at $CONNECT" >&2; exit 1
}

# ---------------------------------------------------------------- SQL Server health
sqlcmd() {  # query  (login and query time out after 10 s instead of waiting forever)
  docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Password!123' -l 10 -t 10 \
    -d demo_db -h -1 -W -b -Q "SET NOCOUNT ON; $1"
}

# prints a reason and returns 1 if SQL Server is unreachable or has user requests stuck for > 60 s
mssql_problem() {
  if ! sqlcmd "SELECT 1" > /dev/null 2>&1; then
    echo "SQL Server does not answer 'SELECT 1' within 10 s"; return 0
  fi
  local stuck
  stuck=$(sqlcmd "SELECT COUNT(*) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id
                  WHERE s.is_user_process = 1 AND r.session_id <> @@SPID AND s.program_name NOT LIKE 'SQLAgent%'
                  AND r.total_elapsed_time > 60000" 2>/dev/null | tr -d '[:space:]')
  if [ -n "$stuck" ] && [ "$stuck" != 0 ]; then
    echo "$stuck SQL Server request(s) running for more than 60 s (hung commit / log flush)"; return 0
  fi
  return 1
}

ensure_mssql_healthy() {
  printf "  SQL Server health check ... "
  local why
  if ! why=$(mssql_problem); then echo "ok"; return 0; fi
  echo "PROBLEM: $why"
  if [ "$MSSQL_AUTO_RESTART" != 1 ]; then
    echo "  Restart it with 'docker restart mssql' and run this script again (MSSQL_AUTO_RESTART=0 is set)." >&2; exit 1
  fi
  echo "  Restarting the mssql container (data is kept in its volume)..."
  docker restart mssql > /dev/null
  for i in $(seq 1 36); do
    if ! mssql_problem > /dev/null; then
      echo "  SQL Server is back after $((i * 5)) s; giving the data generator 15 s to reconnect"
      sleep 15; return 0
    fi
    sleep 5
  done
  echo "  SQL Server is still not healthy after 3 minutes: check 'docker logs mssql'." >&2; exit 1
}

# ---------------------------------------------------------------- deploy one connector
post() {  # host file -> prints HTTP code, body in $RESP
  curl -s --max-time 150 -X POST -H "Content-Type: application/json" --data "@$2" "http://$1/connectors" \
    -o "$RESP" -w "%{http_code}" || true   # prints 000 on timeout / connection error
}

exists() { curl -sf --max-time 10 "http://$1/connectors/$2" -o /dev/null; }

deploy() {  # host file
  local CONNECT="$1" file="$2" name code attempt i
  name=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['name'])" "$file")
  echo "--- $name"
  if exists "$CONNECT" "$name"; then
    curl -s --max-time 30 -X DELETE "http://$CONNECT/connectors/$name" -o /dev/null
    for i in $(seq 1 15); do exists "$CONNECT" "$name" || break; sleep 2; done
  fi
  for attempt in 1 2; do
    code=$(post "$CONNECT" "$file")
    echo "  HTTP $code"
    case "$code" in
      201|409) return 0 ;;   # 409: already created by an earlier, timed-out request
      500|000)
        # timed out while Connect validates the config (e.g. connecting to the database): the
        # request may still complete in the background, so wait for the connector before retrying
        grep -q "timed out" "$RESP" 2>/dev/null && echo "  $(cat "$RESP")"
        echo "  waiting up to 2 min for the connector to appear..."
        for i in $(seq 1 24); do exists "$CONNECT" "$name" && { echo "  created"; return 0; }; sleep 5; done
        if [ "$attempt" = 1 ] && [ "$file" = connectors/mssql-source-connector.json ]; then ensure_mssql_healthy; fi
        ;;
      *) cat "$RESP"; echo; exit 1 ;;
    esac
  done
  echo "ERROR: could not create $name" >&2; exit 1
}

# ---------------------------------------------------------------- wait until RUNNING
wait_running() {  # host...
  local state
  for i in $(seq 1 24); do
    state=$(for CONNECT in "$@"; do curl -s --max-time 10 "http://$CONNECT/connectors?expand=status"; echo; done | python3 -c "
import sys, json
states = []
for line in sys.stdin:
    if line.strip():
        for i in json.loads(line).values():
            s = i['status']
            states += [s['connector']['state']] + [t['state'] for t in s['tasks']] + ([] if s['tasks'] else ['NO_TASKS'])
print('FAILED' if 'FAILED' in states else 'RUNNING' if states and all(x == 'RUNNING' for x in states) else 'PENDING')
" 2>/dev/null || echo PENDING)
    [ "$state" = PENDING ] || break
    sleep 5
  done
  echo; echo "=== Status ==="
  for CONNECT in "$@"; do
    curl -s --max-time 10 "http://$CONNECT/connectors?expand=status" | python3 -c "
import sys, json
for n, i in json.load(sys.stdin).items():
    s = i['status']
    print('  %-20s %-10s tasks: %s' % (n, s['connector']['state'], ','.join(t['state'] for t in s['tasks']) or '-'))
    for t in [s['connector']] + s['tasks']:
        if t['state'] == 'FAILED':
            print('     ', t.get('trace', '')[:500].replace(chr(10), ' '))
"
  done
  case "$state" in
    RUNNING) return 0 ;;
    FAILED) echo "ERROR: a connector or task FAILED (trace above)." >&2; exit 1 ;;
    *) echo "WARNING: not everything is RUNNING after 2 minutes; check again with the curl commands in the README." >&2 ;;
  esac
}

what="${1:-all}"
hosts=""
if [ "$what" = all ] || [ "$what" = source ]; then
  wait_for_connect "$CONNECT_SOURCE"
  ensure_mssql_healthy
  deploy "$CONNECT_SOURCE" connectors/mssql-source-connector.json
  hosts="$hosts $CONNECT_SOURCE"
fi
if [ "$what" = all ] || [ "$what" = sinks ]; then
  wait_for_connect "$CONNECT_SINK"
  deploy "$CONNECT_SINK" connectors/postgres-sink-connector.json
  deploy "$CONNECT_SINK" connectors/db2-sink-connector.json
  hosts="$hosts $CONNECT_SINK"
fi
# shellcheck disable=SC2086
wait_running $hosts

# ---------------------------------------------------------------- optional: Lightning Tables
if is_true "$ENABLE_LIGHTNING_TABLES" && { [ "$what" = all ] || [ "$what" = source ]; }; then
  echo; echo "=== Lightning Tables (ENABLE_LIGHTNING_TABLES=true)"
  # a topic needs a registered schema first: wait until the source has written to every topic
  set -a; source "$ENV_FILE"; set +a
  for t in ${TOPICS:-cdc.data_sensors cdc.customers cdc.orders cdc.products}; do
    printf "  waiting for schema %-28s " "$t-value"
    for i in $(seq 1 36); do
      curl -sf -u "$CC_SOURCE_SR_API_KEY:$CC_SOURCE_SR_API_SECRET" "$CC_SR_URL/subjects/$t-value/versions/latest" -o /dev/null && { echo ok; break; }
      [ "$i" = 36 ] && echo "not found after 3 min"; sleep 5
    done
  done
  ./lightning-tables.sh || echo "WARNING: enabling Lightning Tables failed; rerun ./lightning-tables.sh"
fi
