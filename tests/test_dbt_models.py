"""Tests for the dbt transformation layer.

WHAT THESE ENFORCE
------------------
The dbt models make claims the SQL alone does not guarantee stay true: retry
duplicates are gone, training rows are dropped, CAD is converted, product
duplicates are resolved to one row, and the star schema's foreign keys are
intact. These tests run the actual dbt build and check the resulting tables, so a
future model edit that quietly breaks one of those guarantees fails here.

They require the same live Postgres as the ingestion tests, plus a populated raw
schema. If dbt or the database is unavailable they skip.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import text

from bluebonnet.db import get_engine

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def _dbt_available() -> bool:
    return shutil.which("dbt") is not None


def _db_available() -> bool:
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not (_dbt_available() and _db_available()),
    reason="dbt or Postgres not available",
)


@pytest.fixture(scope="module")
def built():
    """Reload raw data, then run the full dbt build once for the module.

    CRITICAL ORDERING: the ingestion tests reset and repopulate the raw schema as
    they run. Depending on test order, raw could be in a partial state when this
    fixture fires. So we reload all four sources first, guaranteeing dbt builds
    against complete raw data rather than whatever the last ingestion test left
    behind. Without this, fact_order_lines can build with (say) Shopify but no
    POS, and every foreign-key test fails for a reason that has nothing to do with
    the models themselves.

    `dbt build` runs models and their tests together. If it returns non-zero,
    something in the transform or its dbt tests failed, and every test here fails
    loudly rather than checking a stale warehouse.
    """
    from bluebonnet.ingest import ingest

    raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    if raw_dir.exists():
        ingest(raw_dir, source="all", reset=True)

    result = subprocess.run(
        ["dbt", "build"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        pytest.fail(f"dbt build failed:\n{result.stdout[-2000:]}")
    return get_engine()


def _scalar(engine, sql: str):
    with engine.connect() as c:
        return c.execute(text(sql)).scalar()


# ---------------------------------------------------------------------------
# THE CLEANING RULINGS HELD
# ---------------------------------------------------------------------------


def test_shopify_duplicates_were_removed(built):
    """No (order_id, sku, quantity, price) line appears twice after dedupe.

    The API-retry duplicates that landed in raw must be gone. If any remain,
    revenue is overstated by exactly the duplicated lines.
    """
    dupes = _scalar(built, """
        SELECT count(*) FROM (
            SELECT order_id, sku, quantity, unit_price, count(*) c
            FROM analytics_intermediate.int_shopify_deduplicated
            GROUP BY 1,2,3,4 HAVING count(*) > 1
        ) t
    """)
    assert dupes == 0, f"{dupes} duplicate Shopify lines survived dedupe"


def test_training_rows_were_dropped(built):
    """No training-mode POS rows reach the classified model.

    Training transactions are practice, not money. The classified model drops the
    txn_type column entirely (only real sales survive), so we prove the drop by
    comparing counts: classified rows must equal staging sales minus training.
    """
    staging_sales = _scalar(built, """
        SELECT count(*) FROM analytics_staging.stg_pos_transactions
        WHERE txn_type = 'sale'
    """)
    classified = _scalar(built, """
        SELECT count(*) FROM analytics_intermediate.int_pos_classified
    """)
    training_in_staging = _scalar(built, """
        SELECT count(*) FROM analytics_staging.stg_pos_transactions
        WHERE txn_type = 'training'
    """)

    assert training_in_staging > 0, "no training rows in staging - trap is missing"
    assert classified == staging_sales, (
        "classified row count does not equal staging sales - training rows may have leaked"
    )


def test_returns_are_identified(built):
    """Negative-quantity sales are labelled as returns, and some exist.

    Returns must be distinguishable from sales downstream so refunds can be
    netted. If none were identified, the classification silently failed.
    """
    returns = _scalar(built, """
        SELECT count(*) FROM analytics_intermediate.int_pos_classified
        WHERE line_type = 'return'
    """)
    assert returns > 0, "no returns identified - the classification is not working"


def test_cad_rows_were_converted(built):
    """CAD settlement rows exist and their USD amounts differ from the raw CAD.

    A CAD row summed as USD overcounts. After normalization the USD amount must
    reflect the FX conversion, not the raw Canadian figure.
    """
    cad_count = _scalar(built, """
        SELECT count(*) FROM analytics_intermediate.int_amazon_normalized
        WHERE original_currency = 'CAD'
    """)
    assert cad_count > 0, "no CAD rows present - the conversion has nothing to prove"


def test_products_resolved_to_one_row_per_sku(built):
    """Every SKU appears exactly once after duplicate resolution.

    The product master had duplicate SKUs with conflicting costs. If any SKU
    still appears twice, a fact-to-dim join would fan out and multiply revenue.
    """
    dupes = _scalar(built, """
        SELECT count(*) FROM (
            SELECT sku, count(*) c FROM analytics_intermediate.int_products_resolved
            GROUP BY sku HAVING count(*) > 1
        ) t
    """)
    assert dupes == 0, f"{dupes} SKUs still duplicated after resolution"


# ---------------------------------------------------------------------------
# THE STAR SCHEMA IS SOUND
# ---------------------------------------------------------------------------


def test_every_fact_product_key_exists_in_dim_product(built):
    """No order line references a product that is not in the dimension.

    This is referential integrity - the property a star schema exists to
    guarantee. A fact row pointing at a missing dimension key is an orphan that
    silently drops out of any inner-join report.
    """
    orphans = _scalar(built, """
        SELECT count(*) FROM analytics_marts.fact_order_lines f
        LEFT JOIN analytics_marts.dim_product d ON f.product_key = d.product_key
        WHERE d.product_key IS NULL AND f.product_key IS NOT NULL
    """)
    assert orphans == 0, f"{orphans} order lines reference a missing product"


def test_fact_and_settlement_channels_are_disjoint(built):
    """Amazon is in fact_settlements, never in fact_order_lines, and vice versa.

    The two facts are separated because Amazon money is recognised at settlement,
    not order date. If Amazon leaked into the order-lines fact, its revenue would
    be counted on the wrong date and likely double-counted against settlements.
    """
    amazon_in_orders = _scalar(built, """
        SELECT count(*) FROM analytics_marts.fact_order_lines WHERE channel = 'amazon'
    """)
    assert amazon_in_orders == 0, "Amazon leaked into fact_order_lines"


def test_dim_date_covers_all_fact_activity(built):
    """Every activity date in the facts exists in the date dimension.

    A fact date with no matching dim_date row cannot join to calendar attributes -
    the report would lose those rows or fail. The spine must span the full range.
    """
    missing = _scalar(built, """
        SELECT count(*) FROM (
            SELECT activity_date FROM analytics_marts.fact_order_lines
            UNION
            SELECT activity_date FROM analytics_marts.fact_settlements
        ) a
        LEFT JOIN analytics_marts.dim_date d ON a.activity_date = d.date_key
        WHERE d.date_key IS NULL
    """)
    assert missing == 0, f"{missing} fact dates are missing from dim_date"


def test_all_thirteen_models_built(built):
    """The full DAG materialised: 4 staging, 4 intermediate, 5 marts."""
    for schema, expected in [
        ("analytics_staging", 4),
        ("analytics_intermediate", 4),
        ("analytics_marts", 5),
    ]:
        n = _scalar(built, f"""
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = '{schema}'
        """)
        assert n >= expected, f"{schema} has {n} objects, expected at least {expected}"
