"""Backfill: reprocess a range of dates through the pipeline.

WHEN YOU NEED THIS
------------------
A source system resends corrected files for last week. A bug in a transform is
fixed and the affected dates must be rebuilt. A new store's history arrives late.
In all of these, you re-run the pipeline for a bounded date range - a backfill.

WHY IT IS SAFE
--------------
The pipeline is idempotent end to end: load skips already-loaded files (manifest),
and publish upserts. So a backfill over dates that were already processed does not
double-count - it reconciles them to the corrected inputs. This is the pay-off of
building idempotency in from Phase 3.

Run:
  python -m bluebonnet.backfill --start 2025-06-01 --end 2025-06-07
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from bluebonnet.pipeline import run_pipeline


def backfill(raw_dir: Path, start: date, end: date, skip_dbt: bool = False) -> list[dict]:
    """Run the pipeline for each date in [start, end], oldest first.

    dbt run/test happen once per date here for simplicity; a production backfill
    would often load all dates then transform once at the end. We keep it
    per-date so each night is independently verifiable and publishable.
    """
    results = []
    day = start
    while day <= end:
        print(f"\n{'=' * 60}\nBackfilling {day}\n{'=' * 60}")
        try:
            result = run_pipeline(raw_dir, day, skip_dbt=skip_dbt)
            results.append({"date": str(day), "status": "ok",
                            "rows": result["load"]["loaded_rows"]})
        except Exception as e:  # noqa: BLE001 - a backfill logs and continues
            print(f"  FAILED {day}: {e}")
            results.append({"date": str(day), "status": "failed", "error": str(e)})
        day += timedelta(days=1)

    ok = sum(1 for r in results if r["status"] == "ok")
    print(f"\nBackfill complete: {ok}/{len(results)} dates succeeded.")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill the Bluebonnet pipeline.")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--skip-dbt", action="store_true",
                        help="load only, skip transform/test (load many dates, transform once)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    backfill(args.raw_dir, start, end, args.skip_dbt)


if __name__ == "__main__":
    main()
