"""Tests for the pipeline orchestration functions.

These test the WORK the DAG orchestrates, without needing Airflow running - the
whole reason the logic lives in pipeline.py rather than in the DAG. Airflow itself
is validated separately (the DAG is parsed and its structure checked); the
behaviour that could actually break a nightly run is tested here.

The scenarios mirror the Phase 6 checkpoint: a normal night publishes, a re-run is
idempotent, a missing store is tolerated, and a missing source halts.
"""

from __future__ import annotations

import glob
import shutil
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text

from bluebonnet.db import get_engine, reset_schema
from bluebonnet.pipeline import (
    PipelineError,
    is_published,
    publish,
    run_load,
    sense_files,
)

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def _db_available() -> bool:
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason="Postgres unavailable")


@pytest.fixture()
def clean_db():
    engine = get_engine()
    reset_schema(engine)
    # reset_schema only drops the raw schema; the publish ledger lives in ops and
    # persists across runs. Clear it too so publish-gating tests start clean and
    # do not see a date published by an earlier run.
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS ops CASCADE;"))
    return engine


@pytest.fixture()
def raw_present():
    if not (RAW_DIR / "pos").exists():
        pytest.skip("data/raw not generated")


# ---------------------------------------------------------------------------
# SENSE
# ---------------------------------------------------------------------------


def test_sense_passes_when_all_sources_present(raw_present):
    """A complete raw directory senses clean."""
    result = sense_files(RAW_DIR)

    assert all(count > 0 for count in result["present"].values())


def test_sense_tolerates_a_missing_store(raw_present, tmp_path):
    """One store's POS files missing does NOT halt the run.

    Missing store nights are normal and handled downstream as a documented
    residual. The pipeline must not treat them as a fatal error - if it did, a
    single flaky store would block the entire network's revenue every night.
    """
    # Build a raw dir with every POS file EXCEPT store_04.
    for sub in ["shopify", "amazon"]:
        shutil.copytree(RAW_DIR / sub, tmp_path / sub)
    shutil.copy(RAW_DIR / "product_master.xlsx", tmp_path / "product_master.xlsx")
    (tmp_path / "pos").mkdir()
    for f in glob.glob(str(RAW_DIR / "pos" / "*.csv")):
        if "store_04" not in f:
            shutil.copy(f, tmp_path / "pos")

    # Sense still passes - pos has files, just not that store.
    result = sense_files(tmp_path)
    assert result["present"]["pos"] > 0


def test_sense_halts_when_a_whole_source_is_missing(tmp_path):
    """A source with NO files stops the run before any load effort.

    An entirely absent source is a real problem - a broken export, a wrong path -
    not a normal missing night. Failing here, early and clearly, beats a cryptic
    error halfway through loading.
    """
    (tmp_path / "shopify").mkdir()  # empty

    with pytest.raises(PipelineError, match="no files found"):
        sense_files(tmp_path)


# ---------------------------------------------------------------------------
# LOAD IDEMPOTENCY UNDER ORCHESTRATION
# ---------------------------------------------------------------------------


def test_a_normal_night_loads_and_publishes(clean_db, raw_present):
    """The happy path: load the night's data, then publish it."""
    load = run_load(RAW_DIR)
    assert load["loaded_rows"] > 0

    publish(date(2025, 6, 13), load["loaded_rows"])
    assert is_published(date(2025, 6, 13))


def test_rerunning_a_night_is_idempotent(clean_db, raw_present):
    """A retried or re-run load adds no rows the second time.

    This is what makes the DAG safe to retry. If a task fails after loading but
    before publishing, Airflow retries it - and the retry must not double the
    data. The manifest guarantees it.
    """
    first = run_load(RAW_DIR)
    assert first["loaded_rows"] > 0

    second = run_load(RAW_DIR)
    assert second["loaded_rows"] == 0, "re-run loaded rows - not idempotent"
    assert second["skipped_files"] > 0


# ---------------------------------------------------------------------------
# PUBLISH IS THE GATE
# ---------------------------------------------------------------------------


def test_publish_is_the_downstream_signal(clean_db):
    """A date is published only after an explicit publish call.

    Consumers read published dates. A date that never published - because its
    tests failed, say - is simply absent, so bad data cannot surface downstream.
    """
    assert not is_published(date(2025, 1, 1))

    publish(date(2025, 1, 1), 1000)
    assert is_published(date(2025, 1, 1))


def test_publish_is_idempotent(clean_db):
    """Re-publishing a date updates its row rather than erroring.

    A backfill might re-publish a date. That must be a clean upsert, not a
    primary-key crash that fails the whole backfill.
    """
    publish(date(2025, 2, 1), 500)
    publish(date(2025, 2, 1), 750)   # re-publish, new row count

    engine = get_engine()
    with engine.connect() as c:
        rows = c.execute(
            text("SELECT rows_loaded FROM ops.publish_log WHERE run_date = :d"),
            {"d": date(2025, 2, 1)},
        ).all()

    assert len(rows) == 1, "re-publish created a duplicate row"
    assert rows[0][0] == 750, "re-publish did not update the row"
