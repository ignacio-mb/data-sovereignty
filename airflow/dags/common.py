"""Shared pieces of the generated source DAGs.

The DAGs shell out to `ingest` and `dq` rather than importing them. Those tools
live in their own virtualenv at /opt/data-venv with dlt and Great Expectations
behind them; importing that tree into Airflow's own environment would mean
reconciling two large, tightly-pinned dependency graphs forever.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TriggerRule

# Written by the ingest task, read by the ops task. Keyed by dag_id as well as
# run_id: one source's backfill and its hourly run are different DAGs, and two
# sources ingesting at once are different again — none of them may read each
# other's summary.
#
# On the dlt-state volume, not /tmp: /tmp is the container's own writable layer, so
# a recreate between ingest and record_ops loses the summary and the
# ops.pipeline_runs row with it, while the DAG still reports success.
SUMMARY_PATH = (
    "/opt/dlt-state/run-summaries/"
    "run-summary-{{ dag.dag_id }}-{{ run_id | replace('/', '_') | replace(':', '-') }}.json"
)

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
}


def ingest_command(source, extra_args=""):
    """`ingest run`, always writing the summary the ops task records.

    Every fetch goes through here so that `--summary-json` cannot be forgotten on
    one of the three DAGs: a run with no summary file leaves no row in
    ops.pipeline_runs, and "the pipeline has not run" and "the pipeline ran and
    recorded nothing" then look identical.

    `--destination` is deliberately not passed. The CLI already defaults to the
    production warehouse, and a destination named here as well would be a second
    copy to go stale — which it did once already, during the ClickHouse
    migration, when the hourly DAG spent a day failing with
    `'postgres' is not one of 'clickhouse', 'duckdb'`.

    `set -o pipefail` matters: without it the exit status would come from tee.
    """
    return (
        "set -euo pipefail\n"
        f"ingest run --source {source} --summary-json '{SUMMARY_PATH}' {extra_args}\n"
    )


def run_verdict():
    """The leaf task whose state becomes the DAG run's state.

    Airflow derives dag_run state from leaf tasks alone. record_ops is
    deliberately ALL_DONE so that a failed run still gets recorded — which also
    made its unconditional success the verdict for the entire run, and every run
    green no matter what broke upstream. Ingest died with a missing API key three
    hours running and the DAG reported success each time.

    NONE_FAILED rather than ALL_SUCCESS: the two agree on everything that matters
    here — upstream_failed still counts as failed, so a dead ingest reddens the
    run either way — and NONE_FAILED leaves room for a task that skips for a
    reason the pipeline expects.

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
    )
