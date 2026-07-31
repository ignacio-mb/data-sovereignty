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
    """No specs, so no ingest DAGs.

    This is a property of the GENERATOR, not of the checkout: tmp_path is empty
    whatever `sources/` contains. It used to be described as "the shipped state",
    which made it read as a guarantee about the repo — and CLAUDE.md repeated
    that reading. It never was one: the assertion passed identically with zero
    specs committed or fifty. What the repo actually ships is asserted in
    TestWhatThisCheckoutShips, against the real directory.
    """
    return _bag(tmp_path, monkeypatch)


@pytest.fixture
def connected_bag(monkeypatch, tmp_path):
    """One source connected, using the skill's own worked example.

    Deliberately the reference spec rather than a minimal fixture: if the example
    the skill hands people cannot produce a valid DAG, the skill is broken.
    """
    shutil.copy(REFERENCE / "pylon.yml", tmp_path / "pylon.yml")
    return _bag(tmp_path, monkeypatch)


class TestWhatThisCheckoutShips:
    """What `sources/` actually holds, and what that schedules.

    Every spec committed here becomes three DAGs on anyone's stack the moment
    they `make up`, and `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` is false, so
    the ingest DAG is live immediately. That is a deliberate choice per source,
    not a detail — so it is asserted against the real directory rather than a
    temp one, and adding or removing a spec has to come here and say so.
    """

    def test_the_connected_sources_are_the_ones_we_expect(self):
        specs = sorted(p.stem for p in (REPO / "sources").glob("*.yml"))
        assert specs == ["swoogo"], (
            f"sources/ holds {specs}. Every spec here ships connected: it schedules "
            f"an unpaused hourly DAG and needs its token_env set, on every clone of "
            f"this repo. If that is intended, update this list and the description "
            f"in CLAUDE.md and README.md; if not, move the spec to "
            f".claude/skills/add-source/reference/ where pylon.yml lives."
        )

    def test_every_shipped_spec_names_a_credential_and_parses(self):
        """A committed spec that cannot load takes the whole DAG folder down."""
        from ingest_runtime.spec import load

        for path in sorted((REPO / "sources").glob("*.yml")):
            spec = load(path.stem)
            assert spec.token_env, f"{path.name} names no token_env"


class TestEmptyByDefault:
    def test_no_specs_means_no_ingestion_dags(self, empty_bag):
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

    def test_a_failed_task_turns_every_generated_run_red(self, connected_bag):
        """Airflow derives dag_run state from leaf tasks alone.

        record_ops is ALL_DONE and its command cannot fail — a missing summary is
        an echo and exit 0 — so wherever it is the only leaf, the run reports
        success no matter what broke upstream. That is not hypothetical: ingest
        once died on a missing API key three hours running while the DAG called it
        green. Asserted for EVERY generated DAG rather than the hourly one,
        because the generator grew the backfill and reconcile DAGs without a
        verdict and only the hourly one was being checked.
        """
        from airflow.task.trigger_rule import TriggerRule

        for dag_id in connected_bag.dag_ids:
            dag = connected_bag.dags[dag_id]
            if "record_ops" not in dag.task_ids:
                continue  # stack_smoke records nothing
            leaves = [task for task in dag.tasks if not task.downstream_task_ids]
            assert [task.task_id for task in leaves] == ["run_verdict"], (
                f"{dag_id}: the run's verdict must come from one task that "
                f"reflects real failures, not from record_ops"
            )
            assert leaves[0].trigger_rule == TriggerRule.NONE_FAILED, dag_id
            # A verdict hanging off record_ops alone would inherit the same lie:
            # trigger rules look at direct upstream tasks only.
            assert leaves[0].upstream_task_ids - {"record_ops"}, \
                f"{dag_id}: the verdict must depend on the real work too"

    def test_every_fetch_writes_the_summary_the_recorder_reads(self, connected_bag):
        """record_ops records nothing unless the fetch was asked for a summary.

        The two halves are written in different functions, so nothing but this
        checks that they agree on the path: a fetch with no `--summary-json` still
        succeeds, still runs record_ops, and still turns the DAG green — while
        ops.pipeline_runs stays empty and "never ran" becomes indistinguishable
        from "ran and recorded nothing".
        """
        from common import SUMMARY_PATH

        for dag_id in connected_bag.dag_ids:
            dag = connected_bag.dags[dag_id]
            if "record_ops" not in dag.task_ids:
                continue
            fetches = [t for t in dag.tasks if t.task_id in ("ingest", "backfill", "reconcile")]
            assert fetches, f"{dag_id}: no fetch task"
            for task in fetches:
                command = task.bash_command
                assert "--summary-json" in command, f"{dag_id}.{task.task_id}"
                assert SUMMARY_PATH in command, f"{dag_id}.{task.task_id}: not the recorded path"
            assert SUMMARY_PATH in dag.get_task("record_ops").bash_command, dag_id

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
        would silently stop loading the real warehouse.

        No DAG names a destination at all — the CLI's default is the production
        warehouse. That is deliberate: the destination used to be restated in
        common.py, went stale during the ClickHouse migration, and the hourly DAG
        spent a day failing with `'postgres' is not one of 'clickhouse', 'duckdb'`.
        """
        for dag_id in connected_bag.dag_ids:
            for task in connected_bag.dags[dag_id].tasks:
                assert "--destination" not in (getattr(task, "bash_command", "") or ""), dag_id
                assert "duckdb" not in (getattr(task, "bash_command", "") or ""), dag_id

    def test_no_dag_builds_or_validates_a_model(self, connected_bag):
        """This repo ingests and orchestrates. Modelling belongs to the project
        that owns the warehouse's meaning, and a task here that built a transform
        would be this pipeline scheduling someone else's work."""
        for dag_id in connected_bag.dag_ids:
            for task in connected_bag.dags[dag_id].tasks:
                command = getattr(task, "bash_command", "") or ""
                for forbidden in ("mbx", "transform", "--checkpoint marts"):
                    assert forbidden not in command, f"{dag_id}.{task.task_id}: {forbidden}"

    def test_a_reconcile_dag_requires_a_declared_history_floor(self, monkeypatch, tmp_path):
        """Tombstoning compares the run's window against backfill_start. Without
        one there is nothing to compare, so no reconcile DAG should exist."""
        text = (REFERENCE / "pylon.yml").read_text()
        text = text.replace('backfill_start: "2019-01-01"', "").replace("name: pylon", "name: nofloor")
        (tmp_path / "nofloor.yml").write_text(text)
        bag = _bag(tmp_path, monkeypatch)
        assert "nofloor_ingest" in bag.dag_ids
        assert "nofloor_reconcile" not in bag.dag_ids
