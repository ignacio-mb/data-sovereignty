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
