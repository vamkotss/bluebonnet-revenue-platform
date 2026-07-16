# Runbook: Backfilling the Revenue Pipeline

**When to use this:** a source resent corrected files, a transform bug was fixed,
or late historical data arrived, and one or more past dates must be reprocessed.

**Why it is safe:** the pipeline is idempotent. Load skips already-loaded files
via the content-hash manifest; publish upserts. Reprocessing a date that was
already done reconciles it to the corrected inputs rather than duplicating it.

---

## Before you start

1. Confirm the warehouse is up:
   ```
   docker compose ps          # warehouse should be "Up"
   ```
2. Confirm the corrected source files are in place under `data/raw/`.
3. Note the date range you need to reprocess.

## Option A - backfill via the script (most common)

Reprocess a contiguous range, oldest first:

```
python -m bluebonnet.backfill --start 2025-06-01 --end 2025-06-07
```

Each date runs the full flow (load -> dbt run -> dbt test -> publish). A date that
fails is logged and the backfill continues to the next - so one bad day does not
block the rest. Review the summary line at the end for any failures.

**Large ranges:** loading every date then transforming once is faster. Load-only,
then transform manually:

```
python -m bluebonnet.backfill --start 2025-01-01 --end 2025-06-30 --skip-dbt
cd dbt && dbt build && cd ..
```

## Option B - backfill via Airflow

If the DAG is deployed, trigger a backfill from the CLI inside the scheduler
container:

```
docker exec -it <scheduler_container> \
  airflow dags backfill bluebonnet_daily \
  --start-date 2025-06-01 --end-date 2025-06-07
```

## After the backfill

1. Check the reconciliation still holds for the affected channels:
   ```
   docker exec bluebonnet_warehouse psql -U bluebonnet -d warehouse \
     -c "SELECT * FROM analytics_marts.fct_revenue_reconciliation;"
   ```
2. Confirm the dates published:
   ```
   docker exec bluebonnet_warehouse psql -U bluebonnet -d warehouse \
     -c "SELECT * FROM ops.publish_log ORDER BY run_date DESC LIMIT 10;"
   ```

## If a backfill date fails

- **`sense failed: no files found`** - a whole source is missing for that range.
  Check the corrected files were placed under the right `data/raw/` subfolder.
- **`dbt test failed`** - the reconciliation or a data-quality test caught a
  problem in the corrected data. Read the dbt output in the error; the failing
  test names the issue. Fix the source data and re-run the backfill for that date.
- **A single date failed, others succeeded** - the backfill continues past
  failures by design. Re-run just the failed date once fixed:
  ```
  python -m bluebonnet.backfill --start 2025-06-04 --end 2025-06-04
  ```
