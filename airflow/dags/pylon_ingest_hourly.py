"""The steady-state pipeline: ingest, verify, model, verify again.

Runs at :17 rather than :00 so it does not queue behind every other cron on the
box, and because Pylon's own rate limits make a stampede on the hour pointless.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule
from common import (
    DEFAULT_ARGS,
    INGEST_POOL,
    NOTHING_TO_BUILD_EXIT,
    ingest_command,
    record_ops_command,
    run_verdict,
)

with DAG(
    dag_id="pylon_ingest_hourly",
    description="Pylon -> raw_pylon -> quality -> analytics transforms -> quality.",
    schedule="17 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["pylon", "ingest"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command=ingest_command(),
        pool=INGEST_POOL,
        # Comfortably inside the hour: the messages fetch carries its own 25
        # minute budget and stops cleanly, resuming next run.
        execution_timeout=pendulum.duration(minutes=55),
    )

    verify_raw = BashOperator(
        task_id="verify_raw",
        bash_command="set -euo pipefail\ndq run --checkpoint raw_pylon\n",
        execution_timeout=pendulum.duration(minutes=10),
    )

    transform = BashOperator(
        task_id="transform",
        # `mbx transforms` rather than a Metabase transform-job: it builds in
        # manifest (dependency) order and asserts each table's declared grain
        # immediately after building it.
        bash_command="set -euo pipefail\nmbx transforms\n",
        # 99 is `mbx transforms` reporting an empty manifest. Modeling is
        # stop-gated on the docs/ deliverables, so until they are done there is
        # genuinely nothing to build — skip rather than fail, or every hourly run
        # is red for a known reason and the alerts stop meaning anything.
        skip_on_exit_code=NOTHING_TO_BUILD_EXIT,
        execution_timeout=pendulum.duration(minutes=30),
    )

    verify_marts = BashOperator(
        task_id="verify_marts",
        bash_command="set -euo pipefail\ndq run --checkpoint marts\n",
        execution_timeout=pendulum.duration(minutes=10),
    )

    record_ops = BashOperator(
        task_id="record_ops",
        bash_command=record_ops_command(),
        # Runs whatever happened upstream: a failed run is the one most worth
        # having in ops.pipeline_runs.
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=pendulum.duration(minutes=5),
        retries=0,
    )

    ingest >> verify_raw >> transform >> verify_marts >> record_ops
    [verify_marts, record_ops] >> run_verdict()
