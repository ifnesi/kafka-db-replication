#!/bin/bash
# Deploy the SQL Server CDC source and the two JDBC sinks (PostgreSQL, Db2) to the
# self-managed Connect worker, which talks to Kafka + Schema Registry in Confluent Cloud.
# Usage: ./deploy-connectors.sh [source|sinks]   (default: all)
set -e
cd "$(dirname "$0")"
CONNECT="${CONNECT_HOST:-localhost:8083}"

wait_for_connect() {
  for i in $(seq 1 60); do curl -sf "http://$CONNECT/" -o /dev/null && return 0; sleep 5; done
  echo "Kafka Connect not ready" >&2; exit 1
}

deploy() {  # file
  local name; name=$(python3 -c "import json,sys; print(json.load(open('$1'))['name'])")
  echo "--- $name"
  curl -sf "http://$CONNECT/connectors/$name" -o /dev/null && { curl -sX DELETE "http://$CONNECT/connectors/$name"; sleep 2; }
  curl -s -X POST -H "Content-Type: application/json" --data "@$1" "http://$CONNECT/connectors" -o /tmp/connect-resp.json -w "  HTTP %{http_code}\n"
  grep -q error_code /tmp/connect-resp.json && cat /tmp/connect-resp.json && exit 1 || true
}

wait_for_connect
what="${1:-all}"
[ "$what" = all ] || [ "$what" = source ] && deploy connectors/mssql-source-connector.json
if [ "$what" = all ] || [ "$what" = sinks ]; then
  deploy connectors/postgres-sink-connector.json
  deploy connectors/db2-sink-connector.json
fi

sleep 10
echo; echo "=== Status ==="
curl -s "http://$CONNECT/connectors?expand=status" | python3 -c "
import sys, json
for n, i in json.load(sys.stdin).items():
    s = i['status']
    print('  %-20s %-10s tasks: %s' % (n, s['connector']['state'], ','.join(t['state'] for t in s['tasks'])))
    for t in s['tasks']:
        if t['state'] == 'FAILED':
            print('     ', t.get('trace', '')[:500].replace(chr(10), ' '))
"
