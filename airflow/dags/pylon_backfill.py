"""Load a historical window on demand.

Window mode asks the API for issues *created* in [start, end) — that is the only
filter GET /issues supports. It is the right tool for "we are missing March" and
the wrong tool for "catch up on recent edits"; the hourly DAG owns that.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule
from airflow.sdk.definitions.param import Param
from common import DEFAULT_ARGS, INGEST_POOL, ingest_command, record_ops_command, run_verdict

with DAG(
    dag_id="pylon_backfill",
    description="Load a date range of Pylon issues on demand (window mode).",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start": Param(
            "2026-01-01",
            type="string",
            format="date",
            title="Start (inclusive)",
            description="Fetch issues created at or after this UTC date.",
        ),
        "end": Param(
            None,
            type=["null", "string"],
            format="date",
            title="End (exclusive)",
            description="Leave empty for 'now'.",
        ),
        "resources": Param(
            "all",
            type="string",
            title="Resources",
            description="Comma-separated subset, or 'all'.",
        ),
        "budget_minutes": Param(
            0,
            type="integer",
            minimum=0,
            title="Message fetch budget (minutes)",
            description="0 means unlimited, which is usually what a backfill wants.",
        ),
    },
    tags=["pylon", "backfill"],
) as dag:

    backfill = BashOperator(
        task_id="backfill",
        bash_command=ingest_command(
            "--start {{ params.start }} "
            "{% if params.end %}--end {{ params.end }} {% endif %}"
            "--resources {{ params.resources }} "
            "{% if params.budget_minutes %}--budget-minutes {{ params.budget_minutes }}{% endif %}"
        ),
        pool=INGEST_POOL,
        # A wide window is many 30-day chunks at 10 requests/minute.
        execution_timeout=pendulum.duration(hours=12),
    )

    verify_raw = BashOperator(
        task_id="verify_raw",
        # A backfill loads old rows, so the freshness expectation is about the
        # tenant, not this run. Record the verdict, do not fail the backfill on it.
        bash_command="set -euo pipefail\ndq run --checkpoint raw_pylon --no-fail-on-error\n",
        execution_timeout=pendulum.duration(minutes=15),
    )

    record_ops = BashOperator(
        task_id="record_ops",
        bash_command=record_ops_command(),
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=pendulum.duration(minutes=5),
        retries=0,
    )

    backfill >> verify_raw >> record_ops
    [verify_raw, record_ops] >> run_verdict()
