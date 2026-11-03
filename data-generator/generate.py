"""
SQL Server data generator for the replication demo.

1. waits for SQL Server, creates the database + the tables, enables CDC
2. seeds ROWS_PER_TABLE (100) rows into every table (parents first);
   data_sensors is seeded with 10 rows
3. then performs ONE random change (insert / update / delete) every
   OPS_INTERVAL_SECONDS (10s) on a random one of the 19 business tables, forever.
4. in parallel, a second stream (sensor_loop) changes data_sensors every
   SENSOR_INTERVAL_SECONDS (10s): mostly updates; a delete is always paired with an
   insert in the same transaction, so the table always holds exactly 10 rows.

Only leaf tables are deletable (see schema_spec.py) so deletes never violate FKs.
"""

import os
import random
import sys
import threading
import time
from datetime import date, datetime
from decimal import Decimal

import pymssql
from faker import Faker

from schema_spec import SENSOR_TABLE, TABLES, ddl

HOST = os.environ.get("MSSQL_HOST", "mssql")
PORT = int(os.environ.get("MSSQL_PORT", "1433"))
USER = os.environ.get("MSSQL_USER", "sa")
PASSWORD = os.environ.get("MSSQL_PASSWORD", "Password!123")
DATABASE = os.environ.get("MSSQL_DATABASE", "demo_db")
ROWS = int(os.environ.get("ROWS_PER_TABLE", "100"))
INTERVAL = float(os.environ.get("OPS_INTERVAL_SECONDS", "10"))
SENSOR_INTERVAL = float(os.environ.get("SENSOR_INTERVAL_SECONDS", "10"))
SENSOR_REPLACE_PCT = float(os.environ.get("SENSOR_REPLACE_PCT", "0.3"))  # share of ops that delete+insert
# insert / update / delete weights (delete weight moves to insert on non-deletable tables)
W_INS, W_UPD, W_DEL = 0.35, 0.45, 0.20


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class SkipRow(Exception):
    """Row can't be generated right now (e.g. parent table has no rows)."""


class Ctx:
    def __init__(self):
        self.r = random.Random()
        self.f = Faker("en_US")
        self.f.seed_instance(self.r.randint(0, 2**31))
        self.keys = {t.name: [] for t in TABLES}  # table -> [ {pk_col: value} ]

    def pick(self, table, col, default=SkipRow):
        rows = self.keys[table]
        if not rows:
            if default is SkipRow:
                raise SkipRow(f"no rows in {table}")
            return default
        return self.r.choice(rows)[col]


# --------------------------------------------------------------------------- #
# DB helpers
# --------------------------------------------------------------------------- #
def adapt(v):
    """Python -> literal-friendly values for pymssql."""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.") + f"{v.microsecond // 1000:03d}"
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, bool):
        return 1 if v else 0
    return v


def connect(database=None):
    return pymssql.connect(server=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=database or "master", autocommit=False,
                           login_timeout=10, tds_version="7.3")


def wait_for_sqlserver():
    for i in range(1, 121):
        try:
            c = connect()
            c.close()
            log("SQL Server is up")
            return
        except Exception as e:  # noqa: BLE001
            if i % 5 == 1:
                log(f"waiting for SQL Server ({e.__class__.__name__}) attempt {i}/120")
            time.sleep(5)
    sys.exit("SQL Server did not become ready")


def init_schema():
    conn = connect()
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute(f"IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE {DATABASE}")
    conn.close()

    conn = connect(DATABASE)
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = %s", (DATABASE,))
    if not cur.fetchone()[0]:
        cur.execute("EXEC sys.sp_cdc_enable_db")
        log("CDC enabled on database")
    for t in TABLES:
        cur.execute(f"SELECT CASE WHEN OBJECT_ID('dbo.{t.name}', 'U') IS NULL THEN 0 ELSE 1 END")
        if not cur.fetchone()[0]:
            cur.execute(ddl(t, "mssql"))
            log(f"created table dbo.{t.name}")
        cur.execute("SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(%s)",
                    (f"dbo.{t.name}",))
        if cur.fetchone()[0] == 0:
            cur.execute("EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = %s, "
                        "@role_name = NULL, @supports_net_changes = 0", (t.name,))
    conn.close()
    log(f"schema ready, CDC enabled on {len(TABLES)} tables")


# --------------------------------------------------------------------------- #
# Row generation + DML
# --------------------------------------------------------------------------- #
def gen_value(ctx, col, row):
    if col.fk:
        if col.fk_null_pct and ctx.r.random() < col.fk_null_pct:
            return None
        return ctx.pick(*col.fk)
    return col.gen(ctx, row)


def gen_row(ctx, table):
    row = {}
    for col in table.cols:
        if col.identity:
            continue
        row[col.name] = gen_value(ctx, col, row)
    return row


def pk_of(table, row):
    return {c.name: row[c.name] for c in table.pk}


def insert_row(cur, table, row):
    """INSERT and return the PK dict (reads back identity value)."""
    cols = list(row)
    sql = f"INSERT INTO dbo.{table.name} ({', '.join(cols)})"
    ident = table.identity_col
    if ident:
        sql += f" OUTPUT INSERTED.{ident.name}"
    sql += f" VALUES ({', '.join(['%s'] * len(cols))})"
    cur.execute(sql, tuple(adapt(row[c]) for c in cols))
    key = pk_of(table, {**row, **({ident.name: cur.fetchone()[0]} if ident else {})})
    return key


def try_insert(ctx, cur, table, seen=None):
    """Generate + insert one row, retrying on PK / unique collisions."""
    for _ in range(15):
        row = gen_row(ctx, table)
        if seen is not None and not table.identity_col:
            k = tuple(row[c.name] for c in table.pk)
            if k in seen:
                continue
        try:
            key = insert_row(cur, table, row)
        except pymssql.IntegrityError:
            cur.connection.rollback()
            continue
        if seen is not None and not table.identity_col:
            seen.add(tuple(key.values()))
        return key
    raise SkipRow(f"could not find a free key for {table.name}")


def do_update(ctx, cur, table):
    ucols = [c for c in table.cols if c.upd]
    if not ucols or not ctx.keys[table.name]:
        raise SkipRow("nothing to update")
    key = ctx.r.choice(ctx.keys[table.name])
    row = {}
    # regenerate a random subset (always at least one) of the updatable columns
    chosen = [c for c in ucols if ctx.r.random() < 0.6] or [ctx.r.choice(ucols)]
    for c in chosen:
        row[c.name] = gen_value(ctx, c, row)
    if any(c.name == "updated_at" for c in ucols) and "updated_at" not in row:
        row["updated_at"] = ucols[[c.name for c in ucols].index("updated_at")].gen(ctx, row)
    sets = ", ".join(f"{k} = %s" for k in row)
    where = " AND ".join(f"{k} = %s" for k in key)
    cur.execute(f"UPDATE dbo.{table.name} SET {sets} WHERE {where}",
                tuple(adapt(v) for v in row.values()) + tuple(adapt(v) for v in key.values()))
    return key


def do_delete(ctx, cur, table):
    rows = ctx.keys[table.name]
    if len(rows) <= 5:
        raise SkipRow("table nearly empty")
    idx = ctx.r.randrange(len(rows))
    key = rows[idx]
    where = " AND ".join(f"{k} = %s" for k in key)
    cur.execute(f"DELETE FROM dbo.{table.name} WHERE {where}", tuple(adapt(v) for v in key.values()))
    rows.pop(idx)
    return key


def load_keys(ctx, cur):
    for t in TABLES:
        cols = ", ".join(c.name for c in t.pk)
        cur.execute(f"SELECT {cols} FROM dbo.{t.name}")
        ctx.keys[t.name] = [dict(zip([c.name for c in t.pk], r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Phases
# --------------------------------------------------------------------------- #
def seed(ctx, conn):
    cur = conn.cursor()
    load_keys(ctx, cur)
    for t in TABLES:
        target = t.rows or ROWS
        have = len(ctx.keys[t.name])
        if have >= target:
            log(f"seed: {t.name} already has {have} rows")
            continue
        seen = {tuple(k.values()) for k in ctx.keys[t.name]}
        for _ in range(target - have):
            ctx.keys[t.name].append(try_insert(ctx, cur, t, seen))
            conn.commit()  # per row: a unique-collision rollback must not lose earlier rows
        log(f"seed: {t.name:<20} {len(ctx.keys[t.name])} rows")
    log("seed complete - starting continuous change stream")


STREAM_TABLES = [t for t in TABLES if t.stream]


def stream(ctx, conn):
    cur = conn.cursor()
    stats = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    while True:
        started = time.time()
        table = ctx.r.choice(STREAM_TABLES)
        op = ctx.r.choices(["INSERT", "UPDATE", "DELETE"], [W_INS, W_UPD, W_DEL])[0]
        if op == "DELETE" and not table.deletable:
            op = "INSERT"
        try:
            if op == "INSERT":
                key = try_insert(ctx, cur, table)
                ctx.keys[table.name].append(key)
            elif op == "UPDATE":
                key = do_update(ctx, cur, table)
            else:
                key = do_delete(ctx, cur, table)
            conn.commit()
            stats[op] += 1
            log(f"{op:<6} {table.name:<20} {key}   (ins={stats['INSERT']} upd={stats['UPDATE']} del={stats['DELETE']})")
        except SkipRow as e:
            conn.rollback()
            log(f"skip   {table.name}: {e}")
        except pymssql.Error as e:
            conn.rollback()
            if op == "DELETE":  # e.g. unexpected FK reference: drop it from the cache's candidates
                log(f"{op} {table.name} failed ({e.__class__.__name__}), ignoring")
            else:
                log(f"{op} {table.name} failed: {e}")
        time.sleep(max(0.0, INTERVAL - (time.time() - started)))


def sensor_replace(ctx, cur, conn):
    """Delete one sensor and insert a new one in the same transaction (row count stays constant)."""
    t = SENSOR_TABLE
    rows = ctx.keys[t.name]
    idx = ctx.r.randrange(len(rows))
    old = rows[idx]
    where = " AND ".join(f"{k} = %s" for k in old)
    for _ in range(5):  # retry the whole unit on a (very unlikely) unique-key collision
        try:
            cur.execute(f"DELETE FROM dbo.{t.name} WHERE {where}", tuple(adapt(v) for v in old.values()))
            new = insert_row(cur, t, gen_row(ctx, t))
            conn.commit()
            rows[idx] = new
            return old, new
        except pymssql.IntegrityError:
            conn.rollback()
    raise SkipRow("could not insert replacement sensor")


def sensor_loop():
    """Dedicated stream for data_sensors: ~70% updates, ~30% delete+insert replacements."""
    ctx = Ctx()
    conn = connect(DATABASE)
    cur = conn.cursor()
    t = SENSOR_TABLE
    cols = ", ".join(c.name for c in t.pk)
    cur.execute(f"SELECT {cols} FROM dbo.{t.name}")
    ctx.keys[t.name] = [dict(zip([c.name for c in t.pk], r)) for r in cur.fetchall()]
    while len(ctx.keys[t.name]) < t.rows:  # defensive top-up
        ctx.keys[t.name].append(try_insert(ctx, cur, t)); conn.commit()
    log(f"sensor stream started ({len(ctx.keys[t.name])} sensors, replace share {SENSOR_REPLACE_PCT:.0%})")
    n_upd = n_rep = 0
    while True:
        started = time.time()
        try:
            if ctx.r.random() < SENSOR_REPLACE_PCT:
                old, new = sensor_replace(ctx, cur, conn)
                n_rep += 1
                log(f"SENSOR REPLACE data_sensors {old} -> {new}   (updates={n_upd} replaced={n_rep})")
            else:
                key = do_update(ctx, cur, t)
                conn.commit()
                n_upd += 1
                log(f"SENSOR UPDATE  data_sensors {key}   (updates={n_upd} replaced={n_rep})")
        except SkipRow as e:
            conn.rollback()
            log(f"sensor skip: {e}")
        except Exception as e:  # noqa: BLE001 - keep the sensor stream alive
            conn.rollback()
            log(f"sensor op failed: {e}")
        time.sleep(max(0.0, SENSOR_INTERVAL - (time.time() - started)))


def main():
    wait_for_sqlserver()
    init_schema()
    ctx = Ctx()
    conn = connect(DATABASE)
    seed(ctx, conn)
    threading.Thread(target=sensor_loop, daemon=True, name="sensors").start()
    stream(ctx, conn)


if __name__ == "__main__":
    main()
