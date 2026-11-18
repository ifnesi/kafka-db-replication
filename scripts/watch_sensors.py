#!/usr/bin/env python3
"""Live view of data_sensors on SQL Server (source), PostgreSQL and Db2 (targets), newest update
first, refreshed every INTERVAL seconds (default 10) until Ctrl+C.  Via docker exec, like verify.py."""
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime

INTERVAL = int(os.environ.get("INTERVAL", "1"))
COLS = ["sensor_id", "serial_number", "vendor_name", "temperature", "pressure", "online", "updated_at"]

MSSQL_Q = ("SET NOCOUNT ON; SELECT sensor_id, serial_number, vendor_name, temperature, pressure, "
           "CAST(online AS INT), CONVERT(VARCHAR(23), updated_at, 121) FROM dbo.data_sensors ORDER BY updated_at DESC, sensor_id")
PG_Q = ("SELECT sensor_id, serial_number, vendor_name, temperature, pressure, online::int, "
        "to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS.MS') FROM data_sensors ORDER BY updated_at DESC, sensor_id")
DB2_Q = ('SELECT TRIM(CHAR("sensor_id")) || \'|\' || "serial_number" || \'|\' || "vendor_name" || \'|\' || '
         'TRIM(CHAR("temperature")) || \'|\' || TRIM(CHAR("pressure")) || \'|\' || '
         'CASE WHEN "online" THEN \'1\' ELSE \'0\' END || \'|\' || VARCHAR_FORMAT("updated_at", \'YYYY-MM-DD HH24:MI:SS.FF3\') '
         'FROM "data_sensors" ORDER BY "updated_at" DESC, "sensor_id"')


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def rows(out, sep="|"):
    res = []
    for line in out.splitlines():
        p = [x.strip() for x in line.split(sep)]
        if len(p) == len(COLS) and p[0].isdigit():
            res.append(p)
    return res


def norm(r):  # compare numbers by value, not by formatting (e.g. 21.5 vs 21.50)
    return tuple(float(x) if i in (3, 4) else x for i, x in enumerate(r))


def fetch():
    mssql = rows(run(["docker", "exec", "mssql", "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa",
                      "-P", "Password!123", "-d", "demo_db", "-h", "-1", "-W", "-s", "|", "-Q", MSSQL_Q]))
    pg = rows(run(["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "demo", "-At", "-F", "|", "-c", PG_Q]))
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(DB2_Q + ";\n")
    os.chmod(f.name, 0o644)
    subprocess.run(["docker", "cp", f.name, "db2:/tmp/sensors.sql"], capture_output=True)
    os.unlink(f.name)
    db2 = rows(run(["docker", "exec", "db2", "su", "-", "db2inst1", "-c",
                    "db2 connect to demo > /dev/null && db2 -x -t -f /tmp/sensors.sql"]))
    return mssql, pg, db2


def table(title, data, ref):
    status = ""
    if ref is not None:
        same = [norm(r) for r in data] == [norm(r) for r in ref]
        status = "  [in sync with source]" if same else "  [DIFFERENT from source: replication in flight]"
    print(f"\n{title}  ({len(data)} rows){status}")
    widths = [max(len(c), *(len(r[i]) for r in data)) if data else len(c) for i, c in enumerate(COLS)]
    print("  " + "  ".join(c.ljust(w) for c, w in zip(COLS, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for r in data:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))


def main():
    try:
        while True:
            mssql, pg, db2 = fetch()
            print("\033[2J\033[H", end="")
            print(f"data_sensors, newest update first - {datetime.now():%Y-%m-%d %H:%M:%S} - refresh every {INTERVAL}s - Ctrl+C to stop")
            table("1) SQL Server (source)", mssql, None)
            table("2) PostgreSQL (target)", pg, mssql)
            table("3) IBM Db2 (target)", db2, mssql)
            sys.stdout.flush()
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
