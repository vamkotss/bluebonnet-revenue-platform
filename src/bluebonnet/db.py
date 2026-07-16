"""Warehouse connection and the raw-layer schema.

WHERE THIS SITS IN THE PIPELINE
-------------------------------
This is the EL in ELT - Extract and Load, no transform yet. The job here is
narrow and deliberate: get the bytes from four messy sources into Postgres
_faithfully_, defects and all, without losing or duplicating anything. Cleaning
happens later, in dbt. Mixing loading and cleaning is how you lose the ability to
answer "was this wrong in the source, or did my pipeline break it?"

So the raw tables mirror the sources almost verbatim - a Shopify row lands looking
like a Shopify row. The one thing we add is provenance: which file each row came
from, so any row can be traced back to its source.

THE FILE MANIFEST - the idempotency mechanism
---------------------------------------------
The `file_manifest` table records every file we have ever processed, keyed by a
hash of its CONTENTS. Before loading a file, the loader asks the manifest "have I
already loaded a file with these exact bytes?" If yes, it skips. This is what
makes the whole loader idempotent: run it, kill it, run it again, and files
already loaded are not loaded twice.

Hashing CONTENTS rather than filename matters. A file that is re-sent unchanged
should be skipped. A file that keeps its name but whose contents changed is a new
version and must be loaded. The content hash gets both right; a filename check
gets both wrong.
"""

from __future__ import annotations

import os

from sqlalchemy import Engine, create_engine, text

# Connection parameters. Match the docker-compose warehouse. Overridable by env
# so the same code runs against the container locally and CI's Postgres service.
DB_USER = os.environ.get("BB_DB_USER", "bluebonnet")
DB_PASSWORD = os.environ.get("BB_DB_PASSWORD", "bluebonnet")
DB_HOST = os.environ.get("BB_DB_HOST", "localhost")
DB_PORT = os.environ.get("BB_DB_PORT", "5433")
DB_NAME = os.environ.get("BB_DB_NAME", "warehouse")


def get_engine() -> Engine:
    """A SQLAlchemy engine pointed at the warehouse.

    future=True gives 2.0-style behaviour; pool_pre_ping avoids handing out a
    dead connection after the container restarts.
    """
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, future=True, pool_pre_ping=True)


# The raw schema. Every table carries `source_file` and `loaded_at` for
# provenance, plus a natural key where one exists so we can enforce idempotency
# at the row level as a second line of defence behind the manifest.
DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

-- The idempotency ledger. One row per file ever processed.
CREATE TABLE IF NOT EXISTS raw.file_manifest (
    file_hash      TEXT PRIMARY KEY,          -- sha256 of file contents
    source_system  TEXT NOT NULL,             -- shopify | amazon | pos | product
    file_path      TEXT NOT NULL,             -- where it came from (may repeat)
    rows_loaded    INTEGER NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status         TEXT NOT NULL DEFAULT 'loaded'
);

-- Shopify orders: one row per line item, tagged with its order.
CREATE TABLE IF NOT EXISTS raw.shopify_orders (
    id             BIGSERIAL PRIMARY KEY,
    order_id       TEXT NOT NULL,
    created_at     TIMESTAMPTZ,
    sku            TEXT,
    quantity       INTEGER,
    price          NUMERIC(12,2),
    record_type    TEXT,                      -- 'order' | 'refund'
    refund_amount  NUMERIC(12,2),
    source_file    TEXT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Amazon settlements: normalised column names, but currency/date kept raw.
CREATE TABLE IF NOT EXISTS raw.amazon_settlements (
    id             BIGSERIAL PRIMARY KEY,
    amazon_order_id TEXT,
    sku            TEXT,
    quantity       INTEGER,
    gross_amount   NUMERIC(12,2),
    fee_amount     NUMERIC(12,2),
    net_amount     NUMERIC(12,2),
    currency       TEXT,
    settlement_date DATE,
    source_file    TEXT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- POS transactions: kept as-is including negative qty and txn_type, so the
-- returns-vs-training decision is made downstream, not silently at load.
CREATE TABLE IF NOT EXISTS raw.pos_transactions (
    id             BIGSERIAL PRIMARY KEY,
    store_id       TEXT,
    txn_timestamp  TIMESTAMPTZ,
    sku            TEXT,
    quantity       INTEGER,
    amount         NUMERIC(12,2),
    txn_type       TEXT,
    source_file    TEXT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Product master: one row per (possibly duplicated) SKU entry.
CREATE TABLE IF NOT EXISTS raw.product_master (
    id             BIGSERIAL PRIMARY KEY,
    sku            TEXT,
    product_name   TEXT,
    category       TEXT,
    unit_cost      NUMERIC(12,2),
    unit_price     NUMERIC(12,2),
    source_file    TEXT NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def init_schema(engine: Engine) -> None:
    """Create the raw schema and all tables. Safe to run repeatedly."""
    with engine.begin() as conn:
        for statement in DDL.split(";"):
            if statement.strip():
                conn.execute(text(statement))


def reset_schema(engine: Engine) -> None:
    """Drop and recreate the raw schema. For tests and clean reloads."""
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS raw CASCADE;"))
    init_schema(engine)
