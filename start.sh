#!/bin/bash
# Start the stack (fresh: removes old volumes) and wait for the core services.
set -e
cd "$(dirname "$0")"
ROOT="$PWD"
source scripts/docker-env.sh
colima_start   # no-op if Colima isn't installed (e.g. Docker Desktop)

docker info > /dev/null 2>&1 || { echo "ERROR: Docker is not running." >&2; exit 1; }

echo "Resetting previous stack (volumes included)..."
$DC down -v --remove-orphans 2>/dev/null || true

echo "Building images (connect + data-generator)..."
$DC build

echo "Starting all services..."
$DC up -d

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
wait_for "Schema Registry" "curl -sf http://localhost:8081/"      60
wait_for "Kafka Connect"   "curl -sf http://localhost:8083/"      60
wait_for "Control Center"  "curl -sf http://localhost:9021/login" 90
wait_for "CloudBeaver"    "curl -sf http://localhost:8978/" 30
wait_for "PostgreSQL"      "docker exec postgres pg_isready -U postgres -d demo" 30
wait_for "SQL Server"      "docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Password!123' -Q 'SELECT 1'" 60
wait_for "Db2"             "docker exec db2 su - db2inst1 -c 'db2 connect to demo && db2 \"select count(*) from \\\"countries\\\"\"'" 180

# Make the pre-configured CloudBeaver connections visible without logging in
./scripts/cloudbeaver-setup.sh || echo "WARNING: CloudBeaver connection access setup failed (log in as cbadmin / Admin123)"

cat <<MSG

Services:
  Control Center   http://localhost:9021
  CloudBeaver      http://localhost:8978  - SQL client for all 3 DBs (connections pre-loaded; admin: cbadmin / Admin123)
  Kafka Connect    http://localhost:8083
  Schema Registry  http://localhost:8081
  SQL Server       localhost:1433  (sa / Password!123, db demo_db)
  PostgreSQL       localhost:5432  (postgres / postgres, db demo)
  Db2              localhost:50000 (db2inst1 / db2inst1-pwd, db demo)

Next:  ./deploy-connectors.sh      then      ./verify.sh
Logs:  docker compose logs -f data-generator
MSG
