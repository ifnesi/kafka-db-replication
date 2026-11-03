#!/bin/bash
# Per-table row counts: SQL Server vs PostgreSQL vs Db2 (same script as the local-only demo).
cd "$(dirname "$0")"
exec python3 ../scripts/verify.py "$@"
