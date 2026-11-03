"""
Single source of truth for the 20 demo tables (19 "business" tables + data_sensors).

* The data generator uses it to create the SQL Server schema, enable CDC,
  seed 100 rows per table and produce a continuous stream of changes.
* `gen_artifacts.py` uses it to emit the PostgreSQL / Db2 DDL, the connector
  configs and TABLES.md, so every system always agrees on the structure.

Design notes
------------
* Every table has a *different* primary key shape on purpose (identity INT /
  BIGINT / SMALLINT, CHAR(n) and VARCHAR(n) natural keys, UUID strings,
  composite keys incl. a DATE part) to show that the JDBC sink with
  pk.mode=record_key needs no per-table `pk.fields`.
* Column names are lower_snake_case and avoid SQL reserved words.
* Only "leaf" tables (nothing references them) are deletable, so the generator
  never hits FK violations on delete.
"""

import re
import string
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Optional


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #
@dataclass
class Col:
    name: str
    type: str                       # int|bigint|smallint|tinyint|char(n)|varchar(n)|decimal(p,s)|float|bool|date|datetime
    gen: Optional[Callable] = None  # gen(ctx, row_so_far) -> value
    pk: bool = False
    identity: int = 0               # >0 => IDENTITY(seed,1) in SQL Server
    null: bool = False
    upd: bool = False               # column is mutated by UPDATE operations
    unique: bool = False            # UNIQUE constraint (SQL Server only)
    fk: Optional[tuple] = None      # (parent_table, parent_column)
    fk_null_pct: float = 0.0        # chance of NULL for optional FKs


@dataclass
class Table:
    name: str
    cols: list
    deletable: bool = False
    desc: str = ""
    rows: Optional[int] = None      # seed row count (None = ROWS_PER_TABLE)
    stream: bool = True             # False = not part of the random change stream

    @property
    def pk(self):
        return [c for c in self.cols if c.pk]

    @property
    def identity_col(self):
        return next((c for c in self.cols if c.identity), None)


# --------------------------------------------------------------------------- #
# Value helpers (ctx.f = Faker, ctx.r = random.Random)
# --------------------------------------------------------------------------- #
def asc(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def digits(c, n):
    return "".join(c.r.choices(string.digits, k=n))


def letters(c, n):
    return "".join(c.r.choices(string.ascii_uppercase, k=n))


def money(c, lo, hi, places=2):
    return Decimal(str(round(c.r.uniform(lo, hi), places)))


def now(c=None, row=None):
    t = datetime.now(timezone.utc).replace(tzinfo=None)
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)


def past_dt(c, days):
    t = now() - timedelta(seconds=c.r.randint(0, days * 86400))
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)


def past_date(c, days):
    return (now() - timedelta(days=c.r.randint(0, days))).date()


def ch(*opts):
    return lambda c, row: c.r.choice(opts)


def txt(fn, n):
    return lambda c, row: asc(fn(c))[:n]


def maybe(p, fn):
    """Return NULL with probability p, else fn(c,row)."""
    return lambda c, row: None if c.r.random() < p else fn(c, row)


VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # no I, O, Q

# --------------------------------------------------------------------------- #
# The tables (ordered so parents come before children)
# --------------------------------------------------------------------------- #
TABLES = [
    Table("countries", [
        Col("country_code", "char(2)", lambda c, r: letters(c, 2), pk=True),
        Col("country_name", "varchar(80)", txt(lambda c: c.f.country(), 80)),
        Col("iso3_code", "char(3)", lambda c, r: letters(c, 3)),
        Col("region_name", "varchar(30)", ch("EMEA", "APAC", "LATAM", "NA")),
        Col("currency_code", "char(3)", ch("USD", "EUR", "GBP", "JPY", "BRL", "AUD", "CAD", "INR")),
        Col("calling_code", "varchar(6)", lambda c, r: "+" + digits(c, c.r.randint(1, 3))),
        Col("is_eu_member", "bool", lambda c, r: c.r.random() < 0.2, upd=True),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="Reference data, CHAR(2) natural key"),

    Table("departments", [
        Col("dept_code", "char(3)", lambda c, r: letters(c, 3), pk=True),
        Col("dept_name", "varchar(80)", lambda c, r: f"{c.r.choice(['Finance','Sales','Engineering','Support','Marketing','Legal','Logistics','HR','Procurement'])} - {c.r.choice(['EMEA','APAC','AMER','Global'])}"),
        Col("cost_center", "varchar(10)", lambda c, r: "CC-" + digits(c, 5)),
        Col("annual_budget", "decimal(14,2)", lambda c, r: money(c, 100_000, 9_000_000), upd=True),
        Col("head_count", "int", lambda c, r: c.r.randint(3, 250), upd=True),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="CHAR(3) natural key"),

    Table("customers", [
        Col("customer_id", "int", pk=True, identity=1),
        Col("first_name", "varchar(50)", txt(lambda c: c.f.first_name(), 50)),
        Col("last_name", "varchar(50)", txt(lambda c: c.f.last_name(), 50)),
        Col("email", "varchar(120)", lambda c, r: f"{digits(c, 3)}.{asc(c.f.user_name())}@{c.f.free_email_domain()}"[:120], unique=True),
        Col("phone", "varchar(30)", maybe(0.1, txt(lambda c: c.f.phone_number(), 30)), null=True, upd=True),
        Col("birth_date", "date", lambda c, r: past_date(c, 365 * 70) - timedelta(days=365 * 18)),
        Col("customer_tier", "varchar(10)", ch("BRONZE", "SILVER", "GOLD", "PLATINUM"), upd=True),
        Col("is_active", "bool", lambda c, r: c.r.random() < 0.9, upd=True),
        Col("credit_limit", "decimal(10,2)", lambda c, r: money(c, 500, 50_000), upd=True),
        Col("created_at", "datetime", lambda c, r: past_dt(c, 1000)),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="INT IDENTITY key"),

    Table("addresses", [
        Col("address_id", "bigint", pk=True, identity=1),
        Col("customer_id", "int", fk=("customers", "customer_id")),
        Col("address_type", "varchar(10)", ch("BILLING", "SHIPPING")),
        Col("street_line", "varchar(120)", txt(lambda c: c.f.street_address(), 120)),
        Col("city_name", "varchar(60)", txt(lambda c: c.f.city(), 60)),
        Col("postal_code", "varchar(12)", txt(lambda c: c.f.postcode(), 12)),
        Col("country_code", "char(2)", fk=("countries", "country_code")),
        Col("is_default", "bool", lambda c, r: c.r.random() < 0.5, upd=True),
        Col("updated_at", "datetime", now, upd=True),
    ], deletable=True, desc="BIGINT IDENTITY key"),

    Table("product_categories", [
        Col("category_id", "smallint", pk=True, identity=1),
        Col("category_name", "varchar(60)", lambda c, r: f"{c.r.choice(['Home','Garden','Outdoor','Kitchen','Office','Toys','Sports','Beauty','Pet','Auto','Tools','Books'])} {c.f.word().title()} {digits(c, 3)}"),
        Col("parent_category_id", "smallint", lambda c, r: None, null=True),
        Col("display_order", "int", lambda c, r: c.r.randint(1, 500), upd=True),
        Col("is_visible", "bool", lambda c, r: c.r.random() < 0.9, upd=True),
    ], desc="SMALLINT IDENTITY key"),

    Table("suppliers", [
        Col("supplier_code", "varchar(12)", lambda c, r: "SUP-" + digits(c, 6), pk=True),
        Col("company_name", "varchar(100)", txt(lambda c: c.f.company(), 100)),
        Col("contact_email", "varchar(120)", txt(lambda c: c.f.company_email(), 120)),
        Col("contact_phone", "varchar(30)", txt(lambda c: c.f.phone_number(), 30)),
        Col("payment_terms_days", "smallint", ch(15, 30, 45, 60, 90), upd=True),
        Col("rating_score", "decimal(3,2)", lambda c, r: money(c, 1, 5), upd=True),
        Col("is_preferred", "bool", lambda c, r: c.r.random() < 0.3, upd=True),
        Col("onboarded_on", "date", lambda c, r: past_date(c, 3000)),
    ], desc="VARCHAR(12) natural key ('SUP-123456')"),

    Table("products", [
        Col("sku", "varchar(20)", lambda c, r: "SKU-" + digits(c, 8), pk=True),
        Col("product_name", "varchar(120)", lambda c, r: asc(f"{c.f.word().title()} {c.f.word().title()} {digits(c, 2)}")),
        Col("category_id", "smallint", fk=("product_categories", "category_id")),
        Col("supplier_code", "varchar(12)", fk=("suppliers", "supplier_code")),
        Col("unit_price", "decimal(10,2)", lambda c, r: money(c, 1, 2500), upd=True),
        Col("unit_cost", "decimal(10,2)", lambda c, r: money(c, 0.5, 1500)),
        Col("weight_kg", "float", lambda c, r: round(c.r.uniform(0.05, 80), 3), upd=True),
        Col("is_discontinued", "bool", lambda c, r: c.r.random() < 0.05, upd=True),
        Col("launched_on", "date", lambda c, r: past_date(c, 2000)),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="VARCHAR(20) natural key (SKU)"),

    Table("warehouses", [
        Col("warehouse_code", "char(5)", lambda c, r: "WH" + digits(c, 3), pk=True),
        Col("warehouse_name", "varchar(80)", lambda c, r: asc(f"{c.f.city()} Distribution Center")[:80]),
        Col("city_name", "varchar(60)", txt(lambda c: c.f.city(), 60)),
        Col("country_code", "char(2)", fk=("countries", "country_code")),
        Col("capacity_units", "int", lambda c, r: c.r.randint(5_000, 500_000), upd=True),
        Col("is_operational", "bool", lambda c, r: c.r.random() < 0.95, upd=True),
        Col("opened_on", "date", lambda c, r: past_date(c, 6000)),
    ], desc="CHAR(5) natural key ('WH042')"),

    Table("inventory", [
        Col("warehouse_code", "char(5)", pk=True, fk=("warehouses", "warehouse_code")),
        Col("sku", "varchar(20)", pk=True, fk=("products", "sku")),
        Col("quantity_on_hand", "int", lambda c, r: c.r.randint(0, 5000), upd=True),
        Col("reorder_level", "int", lambda c, r: c.r.randint(10, 500)),
        Col("last_counted_at", "datetime", lambda c, r: past_dt(c, 90), upd=True),
        Col("updated_at", "datetime", now, upd=True),
    ], deletable=True, desc="Composite key (CHAR + VARCHAR)"),

    Table("orders", [
        Col("order_id", "bigint", pk=True, identity=1000),
        Col("customer_id", "int", fk=("customers", "customer_id")),
        Col("order_status", "varchar(12)", ch("NEW", "PAID", "PACKED", "SHIPPED", "DELIVERED", "CANCELLED"), upd=True),
        Col("order_total", "decimal(12,2)", lambda c, r: money(c, 5, 15_000), upd=True),
        Col("currency_code", "char(3)", ch("USD", "EUR", "GBP", "BRL")),
        Col("channel", "varchar(10)", ch("WEB", "MOBILE", "STORE", "PHONE")),
        Col("placed_at", "datetime", lambda c, r: past_dt(c, 120)),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="BIGINT IDENTITY(1000) key"),

    Table("order_items", [
        Col("order_id", "bigint", pk=True, fk=("orders", "order_id")),
        Col("line_number", "smallint", lambda c, r: c.r.randint(1, 12), pk=True),
        Col("sku", "varchar(20)", fk=("products", "sku")),
        Col("quantity", "smallint", lambda c, r: c.r.randint(1, 20), upd=True),
        Col("unit_price", "decimal(10,2)", lambda c, r: money(c, 1, 2500), upd=True),
        Col("discount_pct", "decimal(5,2)", lambda c, r: money(c, 0, 40), upd=True),
    ], deletable=True, desc="Composite key (BIGINT + SMALLINT)"),

    Table("payments", [
        Col("payment_id", "varchar(36)", lambda c, r: str(uuid.UUID(int=c.r.getrandbits(128), version=4)), pk=True),
        Col("order_id", "bigint", fk=("orders", "order_id")),
        Col("payment_method", "varchar(15)", ch("CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "PIX")),
        Col("amount", "decimal(12,2)", lambda c, r: money(c, 5, 15_000), upd=True),
        Col("payment_status", "varchar(10)", ch("PENDING", "CAPTURED", "FAILED", "REFUNDED"), upd=True),
        Col("card_last4", "char(4)", maybe(0.3, lambda c, r: digits(c, 4)), null=True),
        Col("processed_at", "datetime", lambda c, r: past_dt(c, 120)),
    ], deletable=True, desc="UUID string key VARCHAR(36)"),

    Table("shipments", [
        Col("shipment_id", "bigint", pk=True, identity=50000),
        Col("order_id", "bigint", fk=("orders", "order_id")),
        Col("warehouse_code", "char(5)", fk=("warehouses", "warehouse_code")),
        Col("carrier_name", "varchar(40)", ch("DHL", "FedEx", "UPS", "Correios", "Royal Mail", "DPD")),
        Col("tracking_number", "varchar(30)", lambda c, r: letters(c, 2) + digits(c, 12), unique=True),
        Col("shipment_status", "varchar(20)", ch("LABEL_CREATED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"), upd=True),
        Col("shipped_at", "datetime", lambda c, r: past_dt(c, 60)),
        Col("delivered_at", "datetime", maybe(0.5, lambda c, r: now()), null=True, upd=True),
        Col("package_weight_kg", "decimal(8,3)", lambda c, r: money(c, 0.1, 60, 3)),
    ], deletable=True, desc="BIGINT IDENTITY(50000) key"),

    Table("employees", [
        Col("employee_id", "int", pk=True, identity=10000),
        Col("first_name", "varchar(50)", txt(lambda c: c.f.first_name(), 50)),
        Col("last_name", "varchar(50)", txt(lambda c: c.f.last_name(), 50)),
        Col("work_email", "varchar(120)", lambda c, r: f"{digits(c, 3)}.{asc(c.f.user_name())}@acme-corp.example"[:120], unique=True),
        Col("dept_code", "char(3)", fk=("departments", "dept_code")),
        Col("job_title", "varchar(60)", txt(lambda c: c.f.job(), 60)),
        Col("hire_date", "date", lambda c, r: past_date(c, 365 * 15)),
        Col("annual_salary", "decimal(10,2)", lambda c, r: money(c, 28_000, 260_000), upd=True),
        Col("employment_status", "varchar(10)", ch("ACTIVE", "ACTIVE", "ACTIVE", "ON_LEAVE", "TERMINATED"), upd=True),
        Col("manager_id", "int", lambda c, r: c.pick("employees", "employee_id", default=None), null=True),
        Col("updated_at", "datetime", now, upd=True),
    ], desc="INT IDENTITY(10000) key"),

    Table("timesheets", [
        Col("employee_id", "int", pk=True, fk=("employees", "employee_id")),
        Col("work_date", "date", lambda c, r: past_date(c, 365), pk=True),
        Col("hours_worked", "decimal(4,2)", lambda c, r: money(c, 1, 12), upd=True),
        Col("project_code", "varchar(12)", lambda c, r: "PRJ-" + digits(c, 5)),
        Col("is_approved", "bool", lambda c, r: c.r.random() < 0.7, upd=True),
        Col("submitted_at", "datetime", lambda c, r: past_dt(c, 30)),
    ], deletable=True, desc="Composite key (INT + DATE)"),

    Table("invoices", [
        Col("invoice_number", "varchar(16)", lambda c, r: "INV-" + digits(c, 10), pk=True),
        Col("order_id", "bigint", fk=("orders", "order_id")),
        Col("invoice_date", "date", lambda c, r: past_date(c, 120)),
        Col("due_date", "date", lambda c, r: r["invoice_date"] + timedelta(days=30)),
        Col("subtotal_amount", "decimal(12,2)", lambda c, r: money(c, 5, 15_000)),
        Col("tax_amount", "decimal(12,2)", lambda c, r: money(c, 0, 3_000)),
        Col("invoice_status", "varchar(10)", ch("OPEN", "PAID", "OVERDUE", "VOID"), upd=True),
        Col("paid_on", "date", maybe(0.5, lambda c, r: now().date()), null=True, upd=True),
    ], deletable=True, desc="VARCHAR(16) natural key ('INV-0000012345')"),

    Table("support_tickets", [
        Col("ticket_id", "bigint", pk=True, identity=7000),
        Col("customer_id", "int", fk=("customers", "customer_id")),
        Col("assigned_employee_id", "int", fk=("employees", "employee_id"), null=True, fk_null_pct=0.25, upd=True),
        Col("subject_line", "varchar(150)", txt(lambda c: c.f.sentence(nb_words=7), 150)),
        Col("ticket_priority", "varchar(8)", ch("LOW", "MEDIUM", "HIGH", "URGENT"), upd=True),
        Col("ticket_status", "varchar(12)", ch("OPEN", "IN_PROGRESS", "WAITING", "RESOLVED", "CLOSED"), upd=True),
        Col("opened_at", "datetime", lambda c, r: past_dt(c, 60)),
        Col("resolved_at", "datetime", maybe(0.6, lambda c, r: now()), null=True, upd=True),
        Col("satisfaction_score", "tinyint", maybe(0.5, lambda c, r: c.r.randint(1, 5)), null=True, upd=True),
    ], deletable=True, desc="BIGINT IDENTITY(7000) key"),

    Table("product_reviews", [
        Col("review_id", "int", pk=True, identity=1),
        Col("sku", "varchar(20)", fk=("products", "sku")),
        Col("customer_id", "int", fk=("customers", "customer_id")),
        Col("rating_stars", "tinyint", lambda c, r: c.r.randint(1, 5), upd=True),
        Col("review_title", "varchar(100)", txt(lambda c: c.f.sentence(nb_words=5), 100)),
        Col("review_text", "varchar(500)", txt(lambda c: c.f.paragraph(nb_sentences=4), 500)),
        Col("is_verified_purchase", "bool", lambda c, r: c.r.random() < 0.8),
        Col("helpful_votes", "int", lambda c, r: c.r.randint(0, 200), upd=True),
        Col("posted_at", "datetime", lambda c, r: past_dt(c, 400)),
    ], deletable=True, desc="INT IDENTITY key, TINYINT column"),

    Table("vehicles", [
        Col("vin", "char(17)", lambda c, r: "".join(c.r.choices(VIN_CHARS, k=17)), pk=True),
        Col("license_plate", "varchar(10)", lambda c, r: letters(c, 3) + "-" + digits(c, 4), unique=True),
        Col("vehicle_make", "varchar(30)", ch("Volvo", "Scania", "Mercedes", "Iveco", "MAN", "Ford", "Renault")),
        Col("vehicle_model", "varchar(30)", lambda c, r: f"{c.r.choice(['FH','R','Actros','Daily','TGX','Transit','Master'])}-{c.r.randint(100, 999)}"),
        Col("model_year", "smallint", lambda c, r: c.r.randint(2012, 2026)),
        Col("fuel_type", "varchar(10)", ch("DIESEL", "ELECTRIC", "HYBRID", "CNG")),
        Col("home_warehouse_code", "char(5)", fk=("warehouses", "warehouse_code")),
        Col("odometer_km", "int", lambda c, r: c.r.randint(0, 900_000), upd=True),
        Col("is_in_service", "bool", lambda c, r: c.r.random() < 0.9, upd=True),
        Col("last_service_on", "date", lambda c, r: past_date(c, 365), upd=True),
    ], desc="CHAR(17) natural key (VIN)"),

    # Small, easy-to-watch table: always 10 rows, changed by its own dedicated stream
    # (see sensor_loop in generate.py): mostly updates; a delete is always paired with
    # an insert, so the row count stays at 10.
    Table("data_sensors", [
        Col("sensor_id", "int", pk=True, identity=1),
        Col("serial_number", "varchar(30)", lambda c, r: "SN-" + digits(c, 8), unique=True),
        Col("vendor_name", "varchar(50)", ch("SensorCorp", "IoTWorks", "DataFlow", "SmartTech", "CloudSense", "MeterMax")),
        Col("temperature", "decimal(5,2)", lambda c, r: money(c, -10, 45), upd=True),
        Col("pressure", "decimal(7,2)", lambda c, r: money(c, 950, 1050), upd=True),
        Col("online", "bool", lambda c, r: c.r.random() < 0.85, upd=True),
        Col("updated_at", "datetime", now, upd=True),
    ], deletable=True, rows=10, stream=False, desc="INT IDENTITY key; always 10 rows, replace-on-delete"),
]

TABLE_BY_NAME = {t.name: t for t in TABLES}
SENSOR_TABLE = TABLE_BY_NAME["data_sensors"]
assert len(TABLES) == 20


# --------------------------------------------------------------------------- #
# DDL builders
# --------------------------------------------------------------------------- #
_TYPE_RE = re.compile(r"(\w+)(?:\((.*)\))?$")

_MAP = {
    "mssql": {"tinyint": "TINYINT", "float": "FLOAT", "bool": "BIT", "datetime": "DATETIME2(3)"},
    "postgres": {"tinyint": "SMALLINT", "float": "DOUBLE PRECISION", "bool": "BOOLEAN", "datetime": "TIMESTAMP(3)"},
    "db2": {"tinyint": "SMALLINT", "float": "DOUBLE", "bool": "BOOLEAN", "datetime": "TIMESTAMP(3)"},
}


def sql_type(typ: str, dialect: str) -> str:
    base, arg = _TYPE_RE.match(typ).groups()
    if base in _MAP[dialect]:
        return _MAP[dialect][base]
    return base.upper() + (f"({arg})" if arg else "")


def ddl(table: Table, dialect: str) -> str:
    q = (lambda n: f'"{n}"') if dialect == "db2" else (lambda n: n)
    lines = []
    for c in table.cols:
        parts = [q(c.name), sql_type(c.type, dialect)]
        if dialect == "mssql" and c.identity:
            parts.append(f"IDENTITY({c.identity},1)")
        parts.append("NULL" if (c.null and not c.pk) else "NOT NULL")
        if dialect == "mssql" and c.unique:
            parts.append("UNIQUE")
        lines.append("    " + " ".join(parts))
    pk_cols = ", ".join(q(c.name) for c in table.pk)
    lines.append(f"    CONSTRAINT {'pk_' + table.name} PRIMARY KEY ({pk_cols})")
    if dialect == "mssql":
        for c in table.cols:
            if c.fk:
                lines.append(f"    CONSTRAINT fk_{table.name}_{c.name} FOREIGN KEY ({c.name}) "
                             f"REFERENCES dbo.{c.fk[0]} ({c.fk[1]})")
        name = f"dbo.{table.name}"
    else:
        name = q(table.name)
    return f"CREATE TABLE {name} (\n" + ",\n".join(lines) + "\n)"
