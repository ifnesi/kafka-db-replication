#!/bin/bash
# Compare per-table row counts: SQL Server (source) vs PostgreSQL vs Db2.
# Counts differ by a few rows while the generator is running (replication lag).
cd "$(dirname "$0")"
exec python3 scripts/verify.py "$@"
