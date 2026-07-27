"""Shared pieces of the Pylon DAGs.

The DAGs shell out to `pylon`, `dq` and `mbx` rather than importing them. Those
tools live in their own virtualenv at /opt/data-venv with dlt and Great
Expectations behind them; importing that tree into Airflow's own environment
would mean reconciling two large, tightly-pinned dependency graphs forever.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TriggerRule

# Serializes every dlt run. Two concurrent ingests share one pipeline working
# directory and one incremental cursor, and would interleave into nonsense.
INGEST_POOL = "pylon_pipeline"

# `mbx transforms` exits with this when the manifest declares nothing to build.
# Duplicated rather than imported: these DAGs deliberately do not import the
# pipeline packages, so the constant is restated here and kept in step with
# mb_tools.run_transforms.NOTHING_TO_BUILD_EXIT by the DAG integrity test.
NOTHING_TO_BUILD_EXIT = 99

# Written by the ingest task, read by the ops task. Per-run so a backfill and an
# hourly run can never read each other's summary.
SUMMARY_PATH = "/tmp/pylon-summary-{{ run_id | replace('/', '_') | replace(':', '-') }}.json"

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
}


def ingest_command(extra_args=""):
    """`pylon ingest`, always writing a summary for the ops task to record.

    `set -o pipefail` matters: without it the exit status would come from tee.
    """
    return (
        "set -euo pipefail\n"
        f"pylon ingest --destination postgres --summary-json '{SUMMARY_PATH}' {extra_args}\n"
    )


def run_verdict():
    """The leaf task whose state becomes the DAG run's state.

    Airflow derives dag_run state from leaf tasks alone. record_ops is
    deliberately ALL_DONE so that a failed run still gets recorded — which also
    made its unconditional success the verdict for the entire run, and every run
    green no matter what broke upstream. Ingest died with a missing API key three
    hours running and the DAG reported success each time.

    NONE_FAILED rather than ALL_SUCCESS: a skipped stop gate — an empty transform
    manifest — is an expected state, not a failure, and has to stay green.

    Wire it downstream of the real work *and* of record_ops, never downstream of
    record_ops alone: trigger rules look only at direct upstream tasks, so a
    verdict hanging off the recorder would inherit the same lie.
    """
    return EmptyOperator(
        task_id="run_verdict",
        trigger_rule=TriggerRule.NONE_FAILED,
        # Never executes. Set only to hold the "every task has a timeout"
        # invariant, which is really about tasks that can pin the ingest pool.
        execution_timeout=pendulum.duration(minutes=1),
    )


def record_ops_command():
    """Record the run even when an upstream task failed — a failed run is exactly
    the one worth having in the history. Missing summary means ingest never got
    far enough to write one, which is itself the finding."""
    return (
        "set -uo pipefail\n"
        f"if [ -f '{SUMMARY_PATH}' ]; then\n"
        f"  dq record-run '{SUMMARY_PATH}' --status \"${{INGEST_STATUS:-succeeded}}\"\n"
        f"  rm -f '{SUMMARY_PATH}'\n"
        "else\n"
        "  echo 'no ingest summary — the ingest task did not complete'\n"
        "fi\n"
        "dq ops-sync || echo 'transform run sync failed (Metabase may be down); continuing'\n"
    )
