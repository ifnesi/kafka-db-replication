#!/bin/bash
# Make the pre-configured CloudBeaver connections visible to everybody (incl. anonymous
# visitors): grants the built-in "user" team access to each connection.
# Safe to re-run (already-granted access is ignored).
# Admin login (cbadmin / Admin123) still works for the admin console.
CB="${CLOUDBEAVER_URL:-http://localhost:8978}"
PW_MD5=$(printf '%s' "${CB_ADMIN_PASSWORD:-Admin123}" | python3 -c 'import sys,hashlib;print(hashlib.md5(sys.stdin.read().encode()).hexdigest().upper())')
JAR=$(mktemp)
gql() { curl -s -b "$JAR" -c "$JAR" -H 'Content-Type: application/json' "$CB/api/gql" -d "$1"; }

gql '{"query":"query { authLogin(provider:\"local\", credentials:{user:\"'"${CB_ADMIN_NAME:-cbadmin}"'\", password:\"'"$PW_MD5"'\"}) { authStatus } }"}' | grep -q SUCCESS \
  || { echo "CloudBeaver admin login failed" >&2; rm -f "$JAR"; exit 1; }

out=$(gql '{"query":"query { addConnectionsAccess(projectId:\"g_GlobalConfiguration\", connectionIds:[\"mssql-source\",\"postgres-target\",\"db2-target\"], subjects:[\"user\"]) }"}')
echo "$out" | grep -q errors && ! echo "$out" | grep -q "23505" && { echo "CloudBeaver access grant failed: $out" >&2; rm -f "$JAR"; exit 1; }
echo "  CloudBeaver: 3 connections visible to all users"
rm -f "$JAR"
