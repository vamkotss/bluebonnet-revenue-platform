"""Pipeline task functions - the work the Airflow DAG orchestrates.

WHY THE LOGIC LIVES HERE, NOT IN THE DAG
----------------------------------------
An Airflow DAG should be wiring, not business logic. If the real work - sensing
files, loading, running dbt, publishing - lives in plain functions here, then:

  - it is unit-testable without spinning up Airflow at all;
  - the same functions run identically in a backfill script, a local debug, or a
    scheduled DAG;
  - the DAG file stays short and readable - a diagram of the flow, not a program.

So the DAG (airflow/dags/bluebonnet_daily.py) imports these functions and arranges
them with retries and alerts. Everything that could actually go wrong is here,
where a test can reach it.

THE NIGHTLY FLOW
----------------
  sense   -> is last night's data present? (fail early and loudly if not)
  load    -> idempotent ingest into raw (Phase 3 - safe to re-run)
  dbt_run -> build staging -> intermediate -> marts
  dbt_test-> run all dbt tests including reconciliation guardrails
  publish -> record a successful, reconciled run in an audit table

The publish step is the contract with downstream: a date is only 'published' if
its data loaded, transformed, AND passed the reconciliation tests. Consumers read
published dates only, so a night that failed quality never reaches a dashboard.
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

from sqlalchemy import text

from bluebonnet.db import get_engine, init_schema
from bluebonnet.ingest import ingest

# Where the dbt project lives, relative to the repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = REPO_ROOT / "dbt"


class PipelineError(RuntimeError):
    """A pipeline step failed in a way that should stop the run."""


# ---------------------------------------------------------------------------
# THE PUBLISH LEDGER - the contract with downstream consumers
# ---------------------------------------------------------------------------

PUBLISH_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.publish_log (
    run_date       DATE PRIMARY KEY,
    published_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_loaded    INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'published'
);
"""


def _ensure_ops_schema() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        for stmt in PUBLISH_DDL.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


# ---------------------------------------------------------------------------
# STEP 1 - SENSE
# ---------------------------------------------------------------------------


def sense_files(raw_dir: Path, required_sources: list[str] | None = None) -> dict:
    """Check the expected sources are present before spending effort on them.

    A sensor that fails early with a clear message beats a load step that dies
    halfway with a cryptic file error. We check each source directory has at least
    one file. POS is allowed to be partially missing (stores drop nights - that is
    normal and handled downstream), but a source with NOTHING is a real problem
    worth stopping for.
    """
    required = required_sources or ["shopify", "amazon", "pos", "product"]
    raw_dir = Path(raw_dir)

    presence = {}
    patterns = {
        "shopify": "shopify/*.json",
        "amazon": "amazon/*.csv",
        "pos": "pos/*.csv",
        "product": "product_master.xlsx",
    }

    missing = []
    for src in required:
        files = list(raw_dir.glob(patterns[src]))
        presence[src] = len(files)
        if len(files) == 0:
            missing.append(src)

    if missing:
        raise PipelineError(
            f"sense failed: no files found for source(s) {missing}. "
            f"Expected under {raw_dir}. Aborting before load."
        )

    return {"present": presence, "raw_dir": str(raw_dir)}


# ---------------------------------------------------------------------------
# STEP 2 - LOAD
# ---------------------------------------------------------------------------


def run_load(raw_dir: Path, source: str = "all") -> dict:
    """Idempotent ingest into raw. Safe to re-run - the manifest skips loaded files.

    Because this is idempotent (Phase 3), a retried task or a re-run backfill does
    not double-count. That property is what makes the whole DAG safe to retry.
    """
    engine = get_engine()
    init_schema(engine)

    results = ingest(Path(raw_dir), source=source)
    total_rows = sum(r["loaded_rows"] for r in results.values())
    total_skipped = sum(r["skipped_files"] for r in results.values())

    return {"loaded_rows": total_rows, "skipped_files": total_skipped, "by_source": results}


# ---------------------------------------------------------------------------
# STEP 3 & 4 - DBT RUN, DBT TEST
# ---------------------------------------------------------------------------


def _run_dbt(command: list[str], step: str) -> dict:
    """Run a dbt command, raising PipelineError with readable output on failure."""
    result = subprocess.run(
        command,
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        # Surface the tail of dbt's own output - the readable error the checkpoint
        # asks for, not a bare "exit code 1".
        raise PipelineError(
            f"{step} failed (exit {result.returncode}):\n{result.stdout[-1500:]}"
        )
    return {"step": step, "returncode": 0}


def run_dbt_run() -> dict:
    """Build the models: staging -> intermediate -> marts."""
    return _run_dbt(["dbt", "run"], "dbt run")


def run_dbt_test() -> dict:
    """Run all dbt tests, including the reconciliation guardrails.

    If reconciliation fails - a channel wildly off, Shopify not tying - this
    raises and the DAG stops BEFORE publish. That is the point: bad data never
    gets published.
    """
    return _run_dbt(["dbt", "test"], "dbt test")


# ---------------------------------------------------------------------------
# STEP 5 - PUBLISH
# ---------------------------------------------------------------------------


def publish(run_date: date, rows_loaded: int) -> dict:
    """Record a successful, reconciled run. The signal downstream waits for.

    Reached only if load, transform, AND tests all passed. Consumers read
    published dates; a night that failed quality is simply absent from this table,
    so it never surfaces in a report. Idempotent: re-publishing a date updates the
    row rather than erroring.
    """
    _ensure_ops_schema()
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ops.publish_log (run_date, rows_loaded, status)
                VALUES (:d, :n, 'published')
                ON CONFLICT (run_date)
                DO UPDATE SET rows_loaded = EXCLUDED.rows_loaded,
                              published_at = now(),
                              status = 'published'
            """),
            {"d": run_date, "n": rows_loaded},
        )
    return {"run_date": str(run_date), "rows_loaded": rows_loaded, "status": "published"}


def is_published(run_date: date) -> bool:
    """Has this date been published? Downstream consumers gate on this."""
    _ensure_ops_schema()
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM ops.publish_log WHERE run_date = :d AND status = 'published'"),
            {"d": run_date},
        ).first()
    return row is not None


# ---------------------------------------------------------------------------
# THE WHOLE PIPELINE - runnable outside Airflow (backfill, debug, tests)
# ---------------------------------------------------------------------------


def run_pipeline(raw_dir: Path, run_date: date, skip_dbt: bool = False) -> dict:
    """Run the full nightly flow end to end. This is what the DAG mirrors.

    Having the entire flow callable as one function means a backfill script is a
    for-loop over dates calling this, and a test is one call with assertions - no
    Airflow required to exercise the real pipeline.
    """
    sense = sense_files(raw_dir)
    load = run_load(raw_dir)

    if not skip_dbt:
        run_dbt_run()
        run_dbt_test()

    pub = publish(run_date, load["loaded_rows"])

    return {"sense": sense, "load": load, "publish": pub}
