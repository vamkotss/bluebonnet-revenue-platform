"""Idempotent ingestion: load the four messy sources into raw Postgres.

THE ONE PROPERTY THAT MATTERS: IDEMPOTENCY
------------------------------------------
Run the loader. Kill it halfway. Run it again. The warehouse must end up exactly
as if it had run once, cleanly - no missing files, no duplicated rows. Real
pipelines crash mid-run constantly (a network blip, an OOM, a deploy). A loader
that double-counts on retry is worse than useless, because the corruption is
silent and compounds every night.

We get idempotency from the file manifest. Each file's CONTENTS are hashed; the
loader skips any file whose hash is already recorded as loaded. The load of a
file and the recording of its manifest row happen in ONE transaction, so a crash
between loading rows and recording the file cannot leave a half-loaded file
marked done - the transaction rolls back and the next run redoes it cleanly.

WHAT THIS LOADER DOES AND DOES NOT DO
-------------------------------------
It faithfully lands the mess: cp1252 files are decoded, both Amazon schemas are
read, Shopify's two discount shapes are tolerated. It does NOT clean: duplicate
Shopify orders are loaded as-is, CAD rows land as CAD, training rows land as
training. Cleaning is dbt's job. The loader's contract is "get the bytes in
faithfully and exactly once", nothing more.

Run:
  python -m bluebonnet.ingest --source all
  python -m bluebonnet.ingest --source pos --raw-dir data/raw
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from bluebonnet.db import get_engine, init_schema


def file_hash(path: Path) -> str:
    """sha256 of a file's contents. The idempotency key."""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def already_loaded(engine: Engine, fhash: str) -> bool:
    """Has a file with these exact contents already been loaded successfully?"""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM raw.file_manifest WHERE file_hash = :h AND status = 'loaded'"),
            {"h": fhash},
        ).first()
    return row is not None


def _record_and_insert(
    engine: Engine, table: str, rows: list[dict], fhash: str,
    source_system: str, file_path: str,
) -> int:
    """Insert rows AND record the manifest, in a single transaction.

    The atomicity here is the whole game. If the process dies after the rows are
    inserted but before the manifest row is written, the transaction rolls back
    and the file is reloaded next run. If it dies after commit, the manifest says
    'loaded' and the file is skipped. There is no interleaving that leaves a file
    half-loaded and marked done.
    """
    if not rows:
        # Still record the file as loaded (zero rows) so we do not reprocess it.
        with engine.begin() as conn:
            conn.execute(
                text("""INSERT INTO raw.file_manifest
                        (file_hash, source_system, file_path, rows_loaded, status)
                        VALUES (:h, :s, :p, 0, 'loaded')
                        ON CONFLICT (file_hash) DO NOTHING"""),
                {"h": fhash, "s": source_system, "p": file_path},
            )
        return 0

    columns = list(rows[0].keys())
    col_list = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    insert_sql = text(f"INSERT INTO raw.{table} ({col_list}) VALUES ({placeholders})")

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
        conn.execute(
            text("""INSERT INTO raw.file_manifest
                    (file_hash, source_system, file_path, rows_loaded, status)
                    VALUES (:h, :s, :p, :n, 'loaded')
                    ON CONFLICT (file_hash) DO NOTHING"""),
            {"h": fhash, "s": source_system, "p": file_path, "n": len(rows)},
        )
    return len(rows)


# ---------------------------------------------------------------------------
# SOURCE PARSERS - faithfully land the mess, do not clean it
# ---------------------------------------------------------------------------


def parse_shopify(path: Path) -> list[dict]:
    """Flatten a Shopify JSON page into raw order-line rows.

    Tolerates BOTH discount shapes and both record types. A refund object becomes
    a single row with record_type='refund'; an order object explodes into one row
    per line item. No dedupe here - duplicates land, dbt removes them.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for obj in data.get("orders", []):
        if obj.get("type") == "refund":
            rows.append({
                "order_id": obj["order_id"],
                "created_at": obj.get("created_at"),
                "sku": None, "quantity": None, "price": None,
                "record_type": "refund",
                "refund_amount": obj.get("refund_amount"),
                "source_file": path.name,
            })
        else:
            for item in obj.get("line_items", []):
                rows.append({
                    "order_id": obj["order_id"],
                    "created_at": obj.get("created_at"),
                    "sku": item["sku"],
                    "quantity": item["quantity"],
                    "price": item["price"],
                    "record_type": "order",
                    "refund_amount": None,
                    "source_file": path.name,
                })
    return rows


def parse_amazon(path: Path) -> list[dict]:
    """Read an Amazon settlement CSV in EITHER schema into a common raw shape.

    The mid-year format change renamed the columns. Rather than break, we detect
    which schema the file uses and map both into the raw table's columns. This is
    exactly the "handle schema drift without a rewrite" skill a DE is hired for.
    """
    df = pd.read_csv(path)
    cols = set(df.columns)

    # Old schema vs new schema column mapping.
    if "amazon_order_id" in cols:
        rename = {}   # already the canonical names
    else:
        rename = {
            "order_id": "amazon_order_id",
            "item_sku": "sku",
            "units": "quantity",
            "revenue": "gross_amount",
            "commission": "fee_amount",
            "net_proceeds": "net_amount",
            "settle_date": "settlement_date",
            # 'currency' kept as-is (deliberately the same name in both)
        }
    df = df.rename(columns=rename)

    keep = ["amazon_order_id", "sku", "quantity", "gross_amount", "fee_amount",
            "net_amount", "currency", "settlement_date"]
    df = df[[c for c in keep if c in df.columns]]
    df["source_file"] = path.name

    return df.to_dict("records")


def parse_pos(path: Path) -> list[dict]:
    """Read a POS CSV, detecting encoding. Lands negative qty and txn_type as-is.

    The cp1252 files (store 11) fail a utf-8 read. We try utf-8 first, fall back
    to cp1252 - the standard, robust encoding-detection pattern. Returns and
    training rows are NOT distinguished here; both land with their txn_type
    intact so the decision is made downstream where it is documented and tested.
    """
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp1252")

    rows = []
    for r in df.itertuples(index=False):
        rows.append({
            "store_id": r.store_id,
            "txn_timestamp": r.timestamp,
            "sku": r.sku,
            "quantity": int(r.quantity),
            "amount": float(r.amount),
            "txn_type": r.txn_type,
            "source_file": path.name,
        })
    return rows


def parse_product_master(path: Path) -> list[dict]:
    """Read the product master, skipping the merged title row.

    The real header is on the second row (header=1) because a merged title cell
    occupies row 1. We read the visible sheet only - the hidden discontinued
    sheet is intentionally not loaded here (it is out-of-catalogue), but its
    existence is documented so a later step can decide to pull it.
    """
    df = pd.read_excel(path, header=1)
    df = df.rename(columns={
        "SKU": "sku", "Product Name": "product_name", "Category": "category",
        "Unit Cost": "unit_cost", "Unit Price": "unit_price",
    })
    df["source_file"] = path.name
    keep = ["sku", "product_name", "category", "unit_cost", "unit_price", "source_file"]
    return df[keep].to_dict("records")


# ---------------------------------------------------------------------------
# LOAD DRIVERS - one per source, each manifest-gated
# ---------------------------------------------------------------------------

SOURCE_CONFIG = {
    "shopify": ("shopify/*.json", parse_shopify, "shopify_orders"),
    "amazon": ("amazon/*.csv", parse_amazon, "amazon_settlements"),
    "pos": ("pos/*.csv", parse_pos, "pos_transactions"),
    "product": ("product_master.xlsx", parse_product_master, "product_master"),
}


def load_source(engine: Engine, source: str, raw_dir: Path) -> dict:
    """Load every file for one source, skipping any already in the manifest."""
    pattern, parser, table = SOURCE_CONFIG[source]
    files = sorted(raw_dir.glob(pattern))

    loaded_files = skipped_files = loaded_rows = 0
    for path in files:
        fhash = file_hash(path)
        if already_loaded(engine, fhash):
            skipped_files += 1
            continue
        rows = parser(path)
        n = _record_and_insert(engine, table, rows, fhash, source, str(path))
        loaded_files += 1
        loaded_rows += n

    return {
        "source": source, "files_seen": len(files),
        "loaded_files": loaded_files, "skipped_files": skipped_files,
        "loaded_rows": loaded_rows,
    }


def ingest(raw_dir: Path, source: str = "all", reset: bool = False) -> dict:
    """Top-level ingestion entry point."""
    engine = get_engine()
    init_schema(engine)
    if reset:
        from bluebonnet.db import reset_schema
        reset_schema(engine)

    sources = list(SOURCE_CONFIG) if source == "all" else [source]

    results = {}
    for src in sources:
        stats = load_source(engine, src, raw_dir)
        results[src] = stats
        print(f"  {src:<10} seen {stats['files_seen']:>4}  "
              f"loaded {stats['loaded_files']:>4}  "
              f"skipped {stats['skipped_files']:>4}  "
              f"rows {stats['loaded_rows']:>8,}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Load messy sources into raw Postgres.")
    parser.add_argument("--source", default="all",
                        choices=["all", "shopify", "amazon", "pos", "product"])
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--reset", action="store_true", help="drop and recreate raw schema")
    args = parser.parse_args()

    print(f"Ingesting from {args.raw_dir} (source={args.source})...")
    ingest(args.raw_dir, args.source, args.reset)
    print("Done.")


if __name__ == "__main__":
    main()
