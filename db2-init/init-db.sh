#!/bin/bash
# Runs once at Db2 container init (mounted at /var/custom): creates the target tables.
echo "=== Waiting for Db2 database 'demo' ==="
for i in $(seq 1 60); do
    if su - db2inst1 -c "db2 connect to demo > /dev/null 2>&1 && db2 connect reset > /dev/null 2>&1"; then
        echo "Db2 is ready."; break
    fi
    echo "  attempt $i/60 ..."; sleep 10
done
su - db2inst1 -c "db2 -tvf /opt/schema/schema.sql"
echo "=== Db2 tables created ==="
