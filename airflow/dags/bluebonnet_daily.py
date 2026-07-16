"""Bluebonnet nightly revenue pipeline - Airflow DAG.

WHAT THIS DAG DOES
------------------
Every night it turns the previous day's raw channel files into published,
reconciled revenue:

    sense -> load -> dbt_run -> dbt_test -> publish

The DAG itself is deliberately thin. All the real work lives in
`bluebonnet.pipeline`, which is unit-tested independently of Airflow. This file
is the wiring diagram: it arranges the steps, sets retries, and routes failures
to an alert. If you want to understand what each step DOES, read pipeline.py; if
you want to understand the ORDER and the failure handling, read here.

WHY THIS ORDER, AND WHY PUBLISH IS LAST
---------------------------------------
Publish is the contract with downstream consumers: a date is only visible to
reports once it has loaded, transformed, AND passed the reconciliation tests. By
putting dbt_test before publish, a night whose books do not reconcile never gets
published - the bad data stops here instead of reaching a dashboard.

RETRIES AND ALERTS
------------------
Every task retries twice with a short delay - transient failures (a momentary DB
blip) self-heal. A task that still fails after retries calls on_failure_callback,
which is where a real deployment would page Slack/PagerDuty. Here it logs a clear,
actionable message.

IDEMPOTENCY MAKES RETRIES SAFE
------------------------------
Because load is idempotent (the file manifest skips already-loaded files) and
publish upserts, a retried task never double-counts. This is what lets us retry
aggressively without corrupting the warehouse.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from bluebonnet import pipeline

# The raw data directory, inside the Airflow container. docker-compose mounts the
# repo's data/ here.
RAW_DIR = Path("/opt/airflow/data/raw")


def _alert_on_failure(context) -> None:
    """Route a failed task to an alert. Replace the log line with Slack/PagerDuty.

    In production this posts to an on-call channel. The key is that a silent
    failure is impossible: a task that fails after its retries always lands here.
    """
    task = context.get("task_instance")
    exc = context.get("exception")
    print(
        f"[ALERT] Bluebonnet pipeline task FAILED: {task.task_id} "
        f"on {context.get('ds')} - {exc}"
    )


default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _alert_on_failure,
}


with DAG(
    dag_id="bluebonnet_daily",
    description="Nightly: sense -> load -> dbt run -> dbt test -> publish",
    schedule="0 6 * * *",          # 6am daily, after overnight files land
    start_date=datetime(2024, 7, 1),
    catchup=False,                 # backfills are run explicitly, not auto
    default_args=default_args,
    tags=["bluebonnet", "revenue", "elt"],
) as dag:

    sense = PythonOperator(
        task_id="sense_files",
        python_callable=pipeline.sense_files,
        op_kwargs={"raw_dir": RAW_DIR},
    )

    load = PythonOperator(
        task_id="load_raw",
        python_callable=pipeline.run_load,
        op_kwargs={"raw_dir": RAW_DIR},
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=pipeline.run_dbt_run,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=pipeline.run_dbt_test,
    )

    def _publish(**context):
        # rows loaded is pushed by the load task via XCom.
        ti = context["ti"]
        load_result = ti.xcom_pull(task_ids="load_raw")
        rows = load_result["loaded_rows"] if load_result else 0
        run_date = context["logical_date"].date()
        return pipeline.publish(run_date, rows)

    publish = PythonOperator(
        task_id="publish",
        python_callable=_publish,
    )

    # The flow. Read left to right: this is the whole pipeline.
    sense >> load >> dbt_run >> dbt_test >> publish
