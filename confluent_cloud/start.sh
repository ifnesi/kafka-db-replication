#!/bin/bash
# Confluent Cloud variant: deploys Kafka (Standard) + Schema Registry (Essentials) with
# terraform, then starts the local side (SQL Server, Postgres, Db2, data generator,
# self-managed Kafka Connect, CloudBeaver) wired to it.
#
#   ./start.sh                  prompts for CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
#                               if they are not already set in the environment
#   ASSUME_YES=1 ./start.sh     don't ask for confirmation before creating cloud resources
#   SKIP_TERRAFORM=1 ./start.sh reuse the existing cloud environment (cloud.env) as is
#
# Every start wipes the local databases AND recreates the Kafka topics (empty), so the
# source, Kafka and targets always start from the same clean state.
set -e
cd "$(dirname "$0")"
source scripts/common.sh

colima_start   # no-op if Colima isn't installed (e.g. Docker Desktop)
docker info > /dev/null 2>&1 || { echo "ERROR: Docker is not running." >&2; exit 1; }

# Fixed container names/ports are shared with the local-only demo in the parent folder
for c in connect mssql data-generator postgres db2 cloudbeaver broker schema-registry control-center; do
  proj=$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "$c" 2>/dev/null || true)
  if [ -n "$proj" ] && [ "$proj" != "db-replication-cloud" ]; then
    echo "ERROR: container '$c' belongs to another compose project ('$proj'), probably the local-only demo." >&2
    echo "       Stop it first (from the parent folder):  KEEP_COLIMA=1 ./stop.sh" >&2
    exit 1
  fi
done

echo "Resetting previous local stack (volumes included)..."
dc down -v --remove-orphans 2> /dev/null || true

# ---------------------------------------------------------------------------
# 1. Confluent Cloud (terraform)
# ---------------------------------------------------------------------------
if [ -n "$SKIP_TERRAFORM" ]; then
  [ -f "$ENV_FILE" ] || { echo "ERROR: SKIP_TERRAFORM=1 but $ENV_FILE does not exist." >&2; exit 1; }
  echo "Skipping terraform, reusing $(basename "$ENV_FILE")."
else
  cat <<MSG

Terraform will create (or update) in your Confluent Cloud organization:
  - environment  db-replication-<random>   (Stream Governance: Essentials = Schema Registry)
  - Kafka cluster  Standard, AWS eu-central-1 (Frankfurt), single zone
  - 3 service accounts + role bindings + API keys
  - 24 topics (20 tables + Connect internal + Debezium schema history)
These resources are billed while they exist: remove them with ./stop.sh (answer 'y' to destroy).
MSG
  if [ -z "$ASSUME_YES" ] && [ -t 0 ]; then
    read -r -p "Continue? [Y/n] " ans
    case "$ans" in [nN]*) echo "Aborted."; exit 1 ;; esac
  fi
  require_cc_credentials

  echo; echo "=== terraform init"
  tf init -input=false
  echo; echo "=== terraform apply (first run takes a few minutes: cluster + role binding propagation)"
  tf apply -input=false -auto-approve -var "run_id=$(date +%s)"

  ( umask 077; tf output -json | python3 scripts/tf-outputs-to-env.py > "$ENV_FILE" )
  chmod 600 "$ENV_FILE"
  echo "Wrote $(basename "$ENV_FILE") (Kafka / Schema Registry endpoints and API keys, git-ignored)."
fi
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

# ---------------------------------------------------------------------------
# 2. Local stack
# ---------------------------------------------------------------------------
echo; echo "Building images (connect + data-generator)..."
dc build

echo "Starting local services..."
dc up -d

wait_for() {  # label, command, max attempts (5s each)
  local label="$1" cmd="$2" max="${3:-60}" i=0 start=$(date +%s)
  printf "  %-22s " "$label"
  while [ $i -lt "$max" ]; do
    if bash -c "$cmd" > /dev/null 2>&1; then echo "ready ($(( $(date +%s) - start ))s)"; return 0; fi
    i=$((i+1)); printf "."; sleep 5
  done
  echo " TIMED OUT"; return 1
}

echo; echo "Waiting for services (Db2 first boot takes a few minutes)..."
wait_for "Kafka Connect"   "curl -sf http://localhost:8083/"     60 || { echo "  Connect did not start (cannot reach Confluent Cloud?). Logs:"; docker logs --tail 20 connect; exit 1; }
wait_for "CloudBeaver"     "curl -sf http://localhost:8978/"     30
wait_for "PostgreSQL"      "docker exec postgres pg_isready -U postgres -d demo" 30
wait_for "SQL Server"      "docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Password!123' -Q 'SELECT 1'" 60
wait_for "Db2"             "docker exec db2 su - db2inst1 -c 'db2 connect to demo && db2 \"select count(*) from \\\"countries\\\"\"'" 180

# Make the pre-configured CloudBeaver connections visible without logging in
../scripts/cloudbeaver-setup.sh || echo "WARNING: CloudBeaver connection access setup failed (log in as cbadmin / Admin123)"

cat <<MSG

Confluent Cloud:
  Environment      $CC_ENV_NAME   ($CC_ENV_ID)
  Console          https://confluent.cloud/environments/$CC_ENV_ID/clusters/$CC_CLUSTER_ID
  Kafka            $CC_KAFKA_BOOTSTRAP
  Schema Registry  $CC_SR_URL   (package $CC_SR_PACKAGE, region $CC_SR_REGION)

Local services:
  CloudBeaver      http://localhost:8978  - SQL client for all 3 DBs (connections pre-loaded; admin: cbadmin / Admin123)
  Kafka Connect    http://localhost:8083  (self-managed, connected to Confluent Cloud)
  SQL Server       localhost:1433  (sa / Password!123, db demo_db)
  PostgreSQL       localhost:5432  (postgres / postgres, db demo)
  Db2              localhost:50000 (db2inst1 / db2inst1-pwd, db demo)

Next:  ./deploy-connectors.sh      then      ./verify.sh
Logs:  docker logs -f data-generator
MSG
