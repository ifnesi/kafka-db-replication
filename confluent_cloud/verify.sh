#!/bin/bash
# Live view of data_sensors on SQL Server (source) and the two targets (PostgreSQL, Db2),
# newest update first, refreshed every 10 seconds (INTERVAL=5 ./verify.sh to change it).
#   ./verify.sh          live data_sensors view (Ctrl+C to stop)
#   ./verify.sh counts   one-shot per-table row counts for all 20 tables
cd "$(dirname "$0")"
if [ "$1" = counts ]; then shift; exec python3 ../scripts/verify.py "$@"; fi
exec python3 ../scripts/watch_sensors.py "$@"
