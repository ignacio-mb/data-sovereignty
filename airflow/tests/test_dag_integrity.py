"""The DAGs are generated, so these test the generator, not a list of files.

Two things are being protected. First, that a fresh checkout schedules nothing —
a repo that quietly arrives running someone else's connector is worse than one
that arrives running none. Second, that the safety invariants survive being
generated: a pooled task without a timeout pins the pool forever, and a tombstone
pass on a partial fetch wipes the warehouse. Neither can be left to whoever
writes the next spec.

A DAG that fails to import does not fail loudly — it just stops being scheduled,
and nobody notices until someone asks why the data is three days old.

Needs Airflow, which is not in the default environment:
    uv run --group dag-tests pytest airflow/tests
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DAGS_FOLDER = REPO / "airflow" / "dags"
REFERENCE = REPO / ".claude" / "skills" / "add-source" / "reference"

pytest.importorskip("airflow", reason="install the dag-tests group to run these")

# Airflow puts the dags folder on sys.path at runtime; replicate that so the
# shared `common` module resolves the same way it will in the scheduler.
if str(DAGS_FOLDER) not in sys.path:
    sys.path.insert(0, str(DAGS_FOLDER))


def _bag(sources_dir, monkeypatch):
    from airflow.dag_processing.dagbag import DagBag

    monkeypatch.setenv("DS_SOURCES_DIR", str(sources_dir))
    bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert not bag.import_errors, bag.import_errors
    return bag


@pytest.fixture
def empty_bag(monkeypatch, tmp_path):
    """The shipped state: no specs, so no ingest DAGs."""
    return _bag(tmp_path, monkeypatch)


@pytest.fixture
def connected_bag(monkeypatch, tmp_path):
    """One source connected, using the skill's own worked example.

    Deliberately the reference spec rather than a minimal fixture: if the example
    the skill hands people cannot produce a valid DAG, the skill is broken.
    """
    shutil.copy(REFERENCE / "pylon.yml", tmp_path / "pylon.yml")
    return _bag(tmp_path, monkeypatch)


class TestEmptyByDefault:
    def test_a_fresh_checkout_schedules_no_ingestion(self, empty_bag):
        assert set(empty_bag.dag_ids) == {"stack_smoke"}

    def test_the_smoke_dag_is_never_scheduled(self, empty_bag):
        # It proves the plumbing on demand; on a timer it would just add noise.
        assert empty_bag.dags["stack_smoke"].schedule is None

    def test_a_malformed_spec_does_not_unschedule_the_good_ones(self, monkeypatch, tmp_path):
        (tmp_path / "broken.yml").write_text("name: [not a name\n")
        shutil.copy(REFERENCE / "pylon.yml", tmp_path / "pylon.yml")
        bag = _bag(tmp_path, monkeypatch)
        assert "pylon_ingest" in bag.dag_ids


class TestGeneratedDags:
    def test_a_connected_source_gets_its_dags(self, connected_bag):
        assert {"pylon_ingest", "pylon_backfill", "pylon_reconcile"} <= set(connected_bag.dag_ids)

    def test_every_task_has_an_execution_timeout(self, connected_bag):
        """A pooled task without one holds the pool forever, and every later run
        of that source queues behind it."""
        for dag_id in connected_bag.dag_ids:
            for task in connected_bag.dags[dag_id].tasks:
                assert task.execution_timeout is not None, f"{dag_id}.{task.task_id}"

    @pytest.mark.parametrize("dag_id", ["pylon_ingest", "pylon_backfill", "pylon_reconcile"])
    def test_exactly_one_task_holds_the_source_pool(self, connected_bag, dag_id):
        dag = connected_bag.dags[dag_id]
        pooled = [t.task_id for t in dag.tasks if t.pool == "pylon_pipeline"]
        assert len(pooled) == 1, f"{dag_id}: {pooled}"
        assert dag.max_active_runs == 1

    def test_the_pool_is_per_source_not_shared(self, connected_bag):
        """A shared pool serialises connectors that have no reason to wait for
        each other."""
        pools = {t.pool for d in connected_bag.dag_ids
                 for t in connected_bag.dags[d].tasks} - {"default_pool"}
        assert pools == {"pylon_pipeline"}

    def test_the_hourly_ingest_cannot_outlive_its_own_interval(self, connected_bag):
        import pendulum

        dag = connected_bag.dags["pylon_ingest"]
        assert dag.schedule == "17 * * * *"
        assert dag.get_task("ingest").execution_timeout < pendulum.duration(hours=1)

    def test_record_ops_runs_even_when_the_run_failed(self, connected_bag):
        """A failed run is the one most worth having in ops.pipeline_runs."""
        from airflow.task.trigger_rule import TriggerRule

        for dag_id in connected_bag.dag_ids:
            dag = connected_bag.dags[dag_id]
            if "record_ops" in dag.task_ids:
                assert dag.get_task("record_ops").trigger_rule == TriggerRule.ALL_DONE, dag_id

    def test_only_the_reconcile_dag_tombstones(self, connected_bag):
        """--mark-deleted after a partial fetch tombstones everything not
        re-fetched, so only the full-history run may pass it."""
        for dag_id in connected_bag.dag_ids:
            for task in connected_bag.dags[dag_id].tasks:
                command = getattr(task, "bash_command", "") or ""
                if "--mark-deleted" in command:
                    assert dag_id == "pylon_reconcile", f"{dag_id} must not tombstone"
                    assert "--start 2019-01-01" in command

    def test_no_dag_writes_to_the_smoke_destination(self, connected_bag):
        """duckdb is the local smoke destination. A scheduled DAG pointing at it
        would silently stop loading the real warehouse."""
        for dag_id in connected_bag.dag_ids:
            for task in connected_bag.dags[dag_id].tasks:
                assert "duckdb" not in (getattr(task, "bash_command", "") or ""), dag_id

    def test_the_transform_step_skips_on_an_empty_manifest(self, connected_bag):
        """The manifest ships empty. A permanently red run for a known reason is
        how people learn to stop reading red runs."""
        from common import NOTHING_TO_BUILD_EXIT

        skip = connected_bag.dags["pylon_ingest"].get_task("transform").skip_on_exit_code
        assert skip in (NOTHING_TO_BUILD_EXIT, [NOTHING_TO_BUILD_EXIT])

    def test_the_skip_code_matches_what_mbx_exits_with(self):
        """Read from the source rather than imported: DAGs may not import the
        packages, so the constant is duplicated and has to be cross-checked."""
        from common import NOTHING_TO_BUILD_EXIT

        source = (REPO / "metabase/src/mb_tools/run_transforms.py").read_text()
        assert str(NOTHING_TO_BUILD_EXIT) in source

    def test_a_reconcile_dag_requires_a_declared_history_floor(self, monkeypatch, tmp_path):
        """Tombstoning compares the run's window against backfill_start. Without
        one there is nothing to compare, so no reconcile DAG should exist."""
        text = (REFERENCE / "pylon.yml").read_text()
        text = text.replace('backfill_start: "2019-01-01"', "").replace("name: pylon", "name: nofloor")
        (tmp_path / "nofloor.yml").write_text(text)
        bag = _bag(tmp_path, monkeypatch)
        assert "nofloor_ingest" in bag.dag_ids
        assert "nofloor_reconcile" not in bag.dag_ids
