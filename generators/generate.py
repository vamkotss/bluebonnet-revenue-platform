"""Generate all of Bluebonnet's data: ground truth, four messy sources, keys.

TWO MODES
---------
  history   Generate the full 18 months at once. This is what you load first to
            populate the warehouse with a backlog to transform and reconcile.

  drip      Generate a single day's files, as if it were last night's export.
            This is what the orchestrated pipeline consumes on a schedule - each
            run has a new day to process, so idempotency and late-arriving data
            actually matter.

WHAT GETS WRITTEN
-----------------
  data/raw/shopify/*.json          the messy sources an analyst is handed
  data/raw/amazon/*.csv
  data/raw/pos/*.csv
  data/raw/product_master.xlsx
  data/raw/_ground_truth_orders.parquet   the answer key (underscore = not input)
  data/raw/_bank_deposits.parquet          the reconciliation control
  data/raw/_defect_manifest.json           what was broken, and how much

The underscored files are the truth the pipeline is graded against. They would
never exist in a real project - here they are what lets every transformation be
tested rather than trusted.

Run:
  python -m generators.generate --mode history
  python -m generators.generate --mode drip --date 2025-06-15
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from generators.ground_truth import (
    SEED,
    _rng,
    build_bank_deposits,
    build_orders,
    build_products,
)
from generators.messy_sources import (
    write_amazon,
    write_pos,
    write_product_master,
    write_shopify,
)


def generate_history(out_dir: Path, seed: int = SEED) -> dict:
    """Generate the full 18-month backlog and all answer keys."""
    rng = _rng(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Seed: {seed}")
    print("Building ground truth...")
    products = build_products(rng)
    orders = build_orders(products, rng)
    bank = build_bank_deposits(orders)

    print(f"  {len(products)} products, {len(orders):,} order lines, "
          f"{orders['order_id'].nunique():,} orders")

    print("Writing messy sources...")
    shopify_stats = write_shopify(orders, out_dir, rng)
    amazon_stats = write_amazon(orders, out_dir, rng)
    pos_stats = write_pos(orders, out_dir, rng)
    product_stats = write_product_master(products, out_dir, rng)

    # Answer keys - the truth the pipeline is graded against.
    orders.to_parquet(out_dir / "_ground_truth_orders.parquet", index=False)
    bank.to_parquet(out_dir / "_bank_deposits.parquet", index=False)
    products.to_parquet(out_dir / "_products_truth.parquet", index=False)

    manifest = {
        "seed": seed,
        "generated_at": datetime.now().isoformat(),
        "ground_truth": {
            "products": len(products),
            "order_lines": len(orders),
            "orders": int(orders["order_id"].nunique()),
            "refunded_lines": int(orders["is_refunded"].sum()),
            "gross_revenue": round(float(orders["gross_revenue"].sum()), 2),
            "bank_net_total": round(float(bank["net_deposit"].sum()), 2),
        },
        "shopify": shopify_stats,
        "amazon": amazon_stats,
        "pos": pos_stats,
        "product_master": product_stats,
    }
    (out_dir / "_defect_manifest.json").write_text(json.dumps(manifest, indent=2))

    print("\nDefect manifest:")
    print(f"  Shopify: {shopify_stats['duplicate_objects']} dupes, "
          f"{shopify_stats['pages']} pages, schema drift mid-year")
    print(f"  Amazon:  {amazon_stats['cad_rows']} CAD rows, format change "
          f"{amazon_stats['format_change_date']}")
    print(f"  POS:     {pos_stats['missing_nights']} missing nights, "
          f"{pos_stats['encoding_files']} cp1252 files, "
          f"{pos_stats['training_rows']} training-mode rows")
    print(f"  Product: {product_stats['sku_format_changed']} SKU renames, "
          f"{product_stats['duplicate_skus']} dupes, hidden sheet")

    return manifest


def generate_drip(out_dir: Path, target: date, seed: int = SEED) -> dict:
    """Generate a single day's files - one night's export for the scheduler.

    Regenerates the full ground truth (deterministic from seed) but writes only
    the files whose business date falls on the target day. This is what makes the
    orchestrated pipeline have something new to do each run, and what makes
    idempotency testable: re-run the same day, get the same files, no dupes.
    """
    rng = _rng(seed)
    products = build_products(rng)
    orders = build_orders(products, rng)

    target_ts = pd.Timestamp(target)
    day_orders = orders[pd.to_datetime(orders["order_date"]) == target_ts]

    drip_dir = out_dir / "landing" / target.isoformat()
    drip_dir.mkdir(parents=True, exist_ok=True)

    # Only Shopify and POS are same-day; Amazon settles 14 days later, so a
    # drip for `target` includes Amazon orders whose SETTLEMENT lands today.
    write_shopify(day_orders, drip_dir, rng)
    write_pos(day_orders, drip_dir, rng)

    print(f"Drip for {target}: {len(day_orders)} order lines written to {drip_dir}")
    return {"date": target.isoformat(), "order_lines": len(day_orders)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Bluebonnet data.")
    parser.add_argument("--mode", choices=["history", "drip"], default="history")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD for drip mode")
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    started = datetime.now()

    if args.mode == "history":
        generate_history(args.out, args.seed)
    else:
        if not args.date:
            parser.error("--date is required for drip mode")
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        generate_drip(args.out.parent, target, args.seed)

    print(f"\nDone in {(datetime.now() - started).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
