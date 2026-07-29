"""The DAGs must parse, and the invariants that protect the warehouse must hold.

A DAG that fails to import does not fail loudly — it just stops being scheduled,
and nobody notices until someone asks why the data is three days old.

Needs Airflow, which is not in the default environment:
    uv sync --group dag-tests && uv run pytest airflow/tests
"""

from __future__ import annotations

import sys
from itertools import pairwise
from pathlib import Path

import pytest

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"

pytest.importorskip("airflow", reason="install the dag-tests group to run these")


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    # Airflow puts the dags folder on sys.path at runtime; replicate that so the
    # shared `common` module resolves the same way it will in the scheduler.
    sys.path.insert(0, str(DAGS_FOLDER))
    try:
        yield DagBag(dag_folder=str(DAGS_FOLDER))
    finally:
        sys.path.remove(str(DAGS_FOLDER))


EXPECTED_DAGS = {
    "stack_smoke",
    "pylon_ingest_hourly",
    "pylon_backfill",
    "pylon_reconcile_weekly",
}


def test_every_dag_imports(dagbag):
    assert not dagbag.import_errors, f"DAGs failed to import: {dagbag.import_errors}"


def test_the_expected_dags_are_all_present(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED_DAGS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_every_task_has_a_timeout(dagbag, dag_id):
    """An ingest without a timeout can hold the pool open indefinitely, which
    silently stops every later run from starting."""
    for task in dagbag.dags[dag_id].tasks:
        assert task.execution_timeout is not None, f"{dag_id}.{task.task_id}"


@pytest.mark.parametrize("dag_id", ["pylon_ingest_hourly", "pylon_backfill", "pylon_reconcile_weekly"])
def test_dlt_runs_are_serialized_by_the_pool(dagbag, dag_id):
    """Two concurrent dlt runs share a working directory and a cursor. The pool
    of one is the only thing preventing that."""
    from common import INGEST_POOL

    dag = dagbag.dags[dag_id]
    ingest_tasks = [task for task in dag.tasks if task.pool == INGEST_POOL]
    assert len(ingest_tasks) == 1, f"{dag_id} should have exactly one pooled ingest task"
    assert dag.max_active_runs == 1


PIPELINE_DAGS = ["pylon_ingest_hourly", "pylon_backfill", "pylon_reconcile_weekly"]


@pytest.mark.parametrize("dag_id", PIPELINE_DAGS)
def test_a_failed_task_turns_the_run_red(dagbag, dag_id):
    """Airflow derives dag_run state from leaf tasks only. record_ops runs on
    ALL_DONE and effectively cannot fail, so while it was the sole leaf every run
    reported success — ingest died with a missing API key three hours running and
    the DAG called it green each time."""
    from airflow.task.trigger_rule import TriggerRule

    dag = dagbag.dags[dag_id]
    leaves = [task for task in dag.tasks if not task.downstream_task_ids]

    assert [task.task_id for task in leaves] == ["run_verdict"], \
        f"{dag_id}: the run's verdict must come from one task that reflects real failures"
    assert leaves[0].trigger_rule == TriggerRule.NONE_FAILED, \
        "ALL_SUCCESS would turn the skipped transform stop gate into a failure"

    # A verdict hanging off record_ops alone would inherit the same lie: trigger
    # rules see direct upstream tasks only.
    upstream = leaves[0].upstream_task_ids
    assert upstream - {"record_ops"}, f"{dag_id}: verdict must depend on real work, not just record_ops"


def test_ops_is_recorded_even_when_the_run_fails(dagbag):
    """A failed run is the one most worth having in ops.pipeline_runs."""
    from airflow.task.trigger_rule import TriggerRule

    for dag_id in ["pylon_ingest_hourly", "pylon_backfill", "pylon_reconcile_weekly"]:
        task = dagbag.dags[dag_id].get_task("record_ops")
        assert task.trigger_rule == TriggerRule.ALL_DONE, dag_id


def test_the_hourly_dag_verifies_before_and_after_modeling(dagbag):
    dag = dagbag.dags["pylon_ingest_hourly"]
    order = ["ingest", "verify_raw", "transform", "verify_marts", "record_ops"]
    for upstream, downstream in pairwise(order):
        assert downstream in dag.get_task(upstream).downstream_task_ids, \
            f"{upstream} should run before {downstream}"


def test_the_empty_manifest_stop_gate_is_a_skip_not_a_failure(dagbag):
    """Modeling is stop-gated until the docs/ deliverables are done, so until
    then `mbx transforms` has nothing to build. Failing on it would paint every
    hourly run red for an expected state, which is how alerts stop being read."""
    from common import NOTHING_TO_BUILD_EXIT

    task = dagbag.dags["pylon_ingest_hourly"].get_task("transform")
    assert NOTHING_TO_BUILD_EXIT in _skip_exit_codes(task)


def test_the_skip_code_matches_the_one_mbx_actually_exits_with(dagbag):
    """common.py restates the constant rather than importing it — these DAGs do
    not import the pipeline packages. That makes drift possible, so it is checked
    here instead: a mismatch turns the skip back into a hard failure."""
    from common import NOTHING_TO_BUILD_EXIT

    source = (
        Path(__file__).resolve().parents[2]
        / "metabase" / "src" / "mb_tools" / "run_transforms.py"
    ).read_text()
    assert f"NOTHING_TO_BUILD_EXIT = {NOTHING_TO_BUILD_EXIT}" in source


def _skip_exit_codes(task):
    codes = getattr(task, "skip_on_exit_code", None)
    if codes is None:
        return ()
    return (codes,) if isinstance(codes, int) else tuple(codes)


def test_only_the_reconcile_dag_can_tombstone(dagbag):
    """--mark-deleted after a partial fetch tombstones everything not re-fetched.
    Only the full-history reconcile is allowed to pass it."""
    for dag_id in EXPECTED_DAGS:
        dag = dagbag.dags[dag_id]
        commands = " ".join(
            str(getattr(task, "bash_command", "")) for task in dag.tasks
        )
        if dag_id == "pylon_reconcile_weekly":
            assert "--mark-deleted" in commands
            assert "--start 2019-01-01" in commands, \
                "a later --start silently disqualifies issues from the soft-delete guard"
        else:
            assert "--mark-deleted" not in commands, dag_id


def test_nothing_writes_to_a_smoke_destination(dagbag):
    """`--destination duckdb` in a scheduled DAG would look like it worked while
    loading nothing into the warehouse."""
    for dag_id in EXPECTED_DAGS:
        for task in dagbag.dags[dag_id].tasks:
            assert "duckdb" not in str(getattr(task, "bash_command", "")), \
                f"{dag_id}.{task.task_id}"


def test_the_dag_ingests_into_the_destination_the_cli_actually_accepts():
    """The DAG's destination string must match the pipeline's production one.

    DAGs shell out and never import the pipeline packages — they live in a
    separate virtualenv — so common.py carries its own copy of the destination
    name. That copy silently went stale during the ClickHouse migration and the
    hourly DAG failed with:

        Invalid value for '--destination': 'postgres' is not one of
        'clickhouse', 'duckdb'

    A structural DAG test cannot catch that; only comparing the two can. This
    test may import the pipeline package precisely because it is not a DAG.
    """
    import common
    from ingest_runtime.ingest.settings import PRODUCTION_DESTINATION as cli_destination

    assert common.PRODUCTION_DESTINATION == cli_destination
    assert f"--destination {cli_destination}" in common.ingest_command()
