#!/usr/bin/env python3
"""Per-table row counts for SQL Server, PostgreSQL and Db2 (via docker exec)."""
import os
import subprocess
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
tables = [t.strip() for t in open(os.path.join(HERE, "tables.txt")) if t.strip()]


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw).stdout


def parse(out):
    res = {}
    for line in out.splitlines():
        parts = line.replace("|", " ").split()
        if len(parts) == 2 and parts[1].isdigit():
            res[parts[0]] = int(parts[1])
    return res


# SQL Server
q = " UNION ALL ".join(f"SELECT '{t}' t, COUNT(*) c FROM dbo.{t}" for t in tables)
mssql = parse(run(["docker", "exec", "mssql", "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa",
                   "-P", "Password!123", "-d", "demo_db", "-h", "-1", "-W", "-Q", f"SET NOCOUNT ON; {q}"]))

# PostgreSQL
q = " UNION ALL ".join(f"SELECT '{t}', COUNT(*) FROM {t}" for t in tables)
pg = parse(run(["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "demo", "-At", "-c", q]))

# Db2 (table names are lowercase + quoted)
q = " UNION ALL ".join(f"SELECT '{t}', COUNT(*) FROM \"{t}\"" for t in tables)
with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
    f.write(q + ";\n")
os.chmod(f.name, 0o644)  # db2inst1 must be able to read it inside the container
subprocess.run(["docker", "cp", f.name, "db2:/tmp/counts.sql"], capture_output=True)
os.unlink(f.name)
db2 = parse(run(["docker", "exec", "db2", "su", "-", "db2inst1", "-c",
                 "db2 connect to demo > /dev/null && db2 -txf /tmp/counts.sql"]))

print(f"{'table':<22}{'sqlserver':>10}{'postgres':>10}{'db2':>8}   status")
bad = 0
for t in tables:
    a, b, c = mssql.get(t), pg.get(t), db2.get(t)
    ok = a is not None and a == b == c
    bad += not ok
    print(f"{t:<22}{a!s:>10}{b!s:>10}{c!s:>8}   {'OK' if ok else 'DIFF'}")
print(f"\n{len(tables) - bad}/{len(tables)} tables in sync" + ("" if not bad else "  (small diffs are normal while changes are in flight; re-run)"))
