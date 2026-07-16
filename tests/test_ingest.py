"""Tests for the idempotent ingestion loader.

THE CENTRAL TEST is test_crash_midway_then_rerun_equals_clean_run. Everything
else supports it. A data engineer's loader lives or dies on this property: a
process that crashes mid-run and is restarted must converge to the same state as
a clean run - no lost files, no duplicated rows.

These tests require a live Postgres on localhost:5433 (the docker-compose
warehouse, or CI's Postgres service). If none is reachable they skip, so the
suite still runs where a DB is not available - but the idempotency guarantee is
only actually proven where Postgres is present.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from bluebonnet.db import get_engine, reset_schema
from bluebonnet.ingest import (
    already_loaded,
    file_hash,
    load_source,
    parse_pos,
    _record_and_insert,
)

RAW_DIR = Path("data/raw")


def _db_available() -> bool:
    try:
        eng = get_engine()
        with eng.connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_available(),
    reason="no Postgres on localhost:5433 - start the docker-compose warehouse",
)


@pytest.fixture()
def clean_db():
    """A freshly reset raw schema for each test."""
    engine = get_engine()
    reset_schema(engine)
    return engine


@pytest.fixture(scope="module")
def raw_present():
    if not (RAW_DIR / "pos").exists():
        pytest.skip("data/raw not generated - run the generator first")


# ---------------------------------------------------------------------------
# THE HASH IS A CONTENT HASH
# ---------------------------------------------------------------------------


def test_file_hash_is_content_based(tmp_path):
    """Same bytes -> same hash; changed bytes -> different hash.

    Idempotency keys on contents, not filenames. A re-sent identical file must be
    skipped; a changed file keeping its name must be reloaded. Only a content
    hash gets both right.
    """
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("identical")
    b.write_text("identical")
    assert file_hash(a) == file_hash(b), "identical contents hashed differently"

    b.write_text("different")
    assert file_hash(a) != file_hash(b), "different contents hashed the same"


# ---------------------------------------------------------------------------
# THE CENTRAL PROPERTY: IDEMPOTENCY
# ---------------------------------------------------------------------------


def test_second_run_loads_nothing(clean_db, raw_present):
    """Running the same load twice loads every file once, then skips all.

    The basic idempotency guarantee: the second run is a no-op on the data.
    """
    first = load_source(clean_db, "amazon", RAW_DIR)
    assert first["loaded_files"] > 0

    second = load_source(clean_db, "amazon", RAW_DIR)
    assert second["loaded_files"] == 0, "second run loaded files it should have skipped"
    assert second["skipped_files"] == first["loaded_files"]

    # Row count is unchanged after the second run.
    with clean_db.connect() as c:
        rows = c.execute(text("SELECT count(*) FROM raw.amazon_settlements")).scalar()
    assert rows == first["loaded_rows"], "the second run changed the row count"


def test_crash_midway_then_rerun_equals_clean_run(clean_db, raw_present):
    """THE test. A mid-run crash, then a restart, converges to the clean state.

    We load only part of the POS files (simulating a crash), then run the real
    loader to completion. The final row count must equal what a single clean run
    produces - proving the restart neither lost the unprocessed files nor
    re-loaded the processed ones.
    """
    pos_files = sorted((RAW_DIR / "pos").glob("*.csv"))
    assert len(pos_files) > 100, "need a meaningful number of files to split"

    crash_at = len(pos_files) // 2

    # Partial load - the "crash".
    for path in pos_files[:crash_at]:
        fh = file_hash(path)
        if already_loaded(clean_db, fh):
            continue
        rows = parse_pos(path)
        _record_and_insert(clean_db, "pos_transactions", rows, fh, "pos", str(path))

    with clean_db.connect() as c:
        partial = c.execute(text("SELECT count(*) FROM raw.pos_transactions")).scalar()

    # Restart: the full loader picks up the rest.
    rerun = load_source(clean_db, "pos", RAW_DIR)
    assert rerun["skipped_files"] == crash_at, "restart did not skip the already-loaded files"

    with clean_db.connect() as c:
        after_rerun = c.execute(text("SELECT count(*) FROM raw.pos_transactions")).scalar()

    # A clean single run, for comparison.
    reset_schema(clean_db)
    load_source(clean_db, "pos", RAW_DIR)
    with clean_db.connect() as c:
        clean = c.execute(text("SELECT count(*) FROM raw.pos_transactions")).scalar()

    assert partial < clean, "the partial load was not actually partial"
    assert after_rerun == clean, (
        f"crash+rerun produced {after_rerun:,} rows, clean run produced {clean:,} - "
        "the loader is not idempotent"
    )


def test_the_manifest_records_every_loaded_file(clean_db, raw_present):
    """Every file that contributed rows has a manifest entry.

    The manifest is the ledger the idempotency check reads. If a loaded file were
    missing from it, that file would be reloaded next run and its rows
    duplicated.
    """
    load_source(clean_db, "amazon", RAW_DIR)

    amazon_files = sorted((RAW_DIR / "amazon").glob("*.csv"))
    with clean_db.connect() as c:
        manifest_count = c.execute(
            text("SELECT count(*) FROM raw.file_manifest WHERE source_system = 'amazon'")
        ).scalar()

    assert manifest_count == len(amazon_files), (
        f"manifest has {manifest_count} amazon files, {len(amazon_files)} exist on disk"
    )


# ---------------------------------------------------------------------------
# THE LOADER FAITHFULLY LANDS THE MESS
# ---------------------------------------------------------------------------


def test_both_amazon_schemas_load(clean_db, raw_present):
    """Files in both the old and new Amazon column layout land in one table.

    The mid-year format change must not break ingestion. Both schemas map into
    the common raw shape.
    """
    load_source(clean_db, "amazon", RAW_DIR)

    with clean_db.connect() as c:
        # Every row has an amazon_order_id, regardless of source schema.
        null_ids = c.execute(
            text("SELECT count(*) FROM raw.amazon_settlements WHERE amazon_order_id IS NULL")
        ).scalar()
        total = c.execute(text("SELECT count(*) FROM raw.amazon_settlements")).scalar()

    assert total > 0
    assert null_ids == 0, "some Amazon rows lost their order id - a schema was not mapped"


def test_cp1252_pos_file_loads_without_crashing(clean_db, raw_present):
    """The Windows-1252 POS files load - the encoding fallback works.

    A loader that assumed utf-8 would crash on store 11. Landing those rows at all
    proves the encoding detection works.
    """
    load_source(clean_db, "pos", RAW_DIR)

    with clean_db.connect() as c:
        store11 = c.execute(
            text("SELECT count(*) FROM raw.pos_transactions WHERE store_id = 'store_11'")
        ).scalar()

    assert store11 > 0, "no store_11 rows loaded - the cp1252 files failed to ingest"


def test_pos_training_rows_survive_ingestion(clean_db, raw_present):
    """Training-mode rows land with their txn_type intact, not silently dropped.

    The loader must not make the returns-vs-training decision - it lands both so
    the documented ruling can be applied and tested downstream. If the loader
    dropped or relabelled them, that decision would be made invisibly at load.
    """
    load_source(clean_db, "pos", RAW_DIR)

    with clean_db.connect() as c:
        training = c.execute(
            text("SELECT count(*) FROM raw.pos_transactions WHERE txn_type = 'training'")
        ).scalar()

    assert training > 0, "training rows did not survive ingestion"


def test_duplicate_shopify_orders_land_as_is(clean_db, raw_present):
    """Duplicate Shopify orders are NOT deduped at load - that is dbt's job.

    The loader's contract is faithful landing. Deduplication is a documented
    transformation, made and tested in dbt where it is visible. If the loader
    silently deduped, the raw layer would no longer be a faithful copy of the
    source and 'was it wrong in the source or did my pipeline break it?' would be
    unanswerable.
    """
    load_source(clean_db, "shopify", RAW_DIR)

    with clean_db.connect() as c:
        # There exists at least one order_id appearing more than once.
        max_dupes = c.execute(
            text("""SELECT max(cnt) FROM (
                        SELECT order_id, count(*) cnt
                        FROM raw.shopify_orders
                        WHERE record_type = 'order'
                        GROUP BY order_id
                    ) t""")
        ).scalar()

    assert max_dupes > 1, "no duplicate orders in raw - the loader deduped when it should not"
