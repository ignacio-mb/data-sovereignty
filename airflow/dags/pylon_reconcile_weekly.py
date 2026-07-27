"""Weekly full-history reconcile: catch deletions the incremental path cannot see.

The incremental cursor only ever learns about rows that still exist. Something
deleted in Pylon simply stops appearing, and merge-on-id leaves the stale copy
in the warehouse forever. Once a week we re-fetch everything and tombstone what
did not come back.

`--mark-deleted` is the sharpest tool in the pipeline: its predicate is "absent
from this run's loads", so running it after a partial fetch tombstones the
difference. The CLI's own guard only lets it touch `issues` when the run
genuinely covered the full history, which is why this DAG passes an explicit
--start at the backfill epoch and no --end.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule
from common import DEFAULT_ARGS, INGEST_POOL, ingest_command, record_ops_command

# Matches BACKFILL_START in the pipeline settings. Passing a later date would
# silently disqualify issues from the soft-delete pass.
BACKFILL_EPOCH = "2019-01-01"

with DAG(
    dag_id="pylon_reconcile_weekly",
    description="Full-history re-fetch that tombstones rows deleted in Pylon.",
    schedule="0 3 * * 6",  # Saturday 03:00 UTC — quiet, and a full day of slack
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},  # a partial retry is the risk here
    tags=["pylon", "reconcile"],
) as dag:

    reconcile = BashOperator(
        task_id="reconcile",
        # Messages are excluded deliberately: they are fetched per issue, so a
        # full "absence" scan is never available for them and they are only ever
        # appended. Including them would add hours for no reconciliation value.
        bash_command=ingest_command(
            f"--start {BACKFILL_EPOCH} "
            "--resources issues,accounts,users,teams,contacts "
            "--mark-deleted"
        ),
        pool=INGEST_POOL,
        # Years of 30-day windows at 10 requests/minute. The timeout must exceed
        # the worst case or the tombstone pass never runs and nothing reconciles.
        execution_timeout=pendulum.duration(hours=20),
    )

    verify_raw = BashOperator(
        task_id="verify_raw",
        # The tombstone-fraction expectation is the point of running this here:
        # it is what catches a soft-delete pass that went too wide.
        bash_command="set -euo pipefail\ndq run --checkpoint raw_pylon\n",
        execution_timeout=pendulum.duration(minutes=15),
    )

    record_ops = BashOperator(
        task_id="record_ops",
        bash_command=record_ops_command(),
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=pendulum.duration(minutes=5),
        retries=0,
    )

    reconcile >> verify_raw >> record_ops
