"""The DAGs are generated, so these test the generator, not a list of files.

Three things are being protected. That what this checkout schedules is what it
says it schedules — every connected spec becomes live DAGs on anyone's stack
the moment they `make up`. That the safety invariants survive being generated:
a pooled task without a timeout pins the pool forever, and a tombstone pass on
a partial fetch wipes the warehouse. And that the generator's own derivation of
pools, DAG ids and timeouts still agrees with the spec parser's — they are two
implementations of the same defaults, deliberately, because the DAG processor
runs in Airflow's virtualenv and the parser lives in the runtime's.

A DAG that fails to import does not fail loudly — it just stops being
scheduled, and nobody notices until someone asks why the data is three days old.

Needs Airflow, which is not in the default environment:
    uv run --group dag-tests pytest airflow/tests
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DAGS_FOLDER = REPO / "airflow" / "dags"
SOURCES = REPO / "sources"
# The worked example: validated, built by the contract suite, and scheduled by
# nothing. Copied into a tmp directory here so the generator can be tested on a
# real spec without this suite depending on what the checkout happens to ship.
REFERENCE = SOURCES / "pylon"

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


def _connector(tmp_path, name="pylon", status=None, edit=None):
    """A connector directory in `tmp_path`, from the reference spec.

    A directory, not a file: the directory name is the identity everything else
    is derived from, and the generator checks the spec's `name` against it
    rather than trusting either alone.
    """
    text = (REFERENCE / "source.yml").read_text()
    if status is not None:
        text = text.replace("status: reference", f"status: {status}")
    if edit is not None:
        text = edit(text)
    (tmp_path / name).mkdir(parents=True, exist_ok=True)
    (tmp_path / name / "source.yml").write_text(text.replace("name: pylon", f"name: {name}"))
    return tmp_path


@pytest.fixture
def empty_bag(monkeypatch, tmp_path):
    """No specs, so no ingest DAGs.

    This is a property of the GENERATOR, not of the checkout: tmp_path is empty
    whatever `sources/` contains. It used to be described as "the shipped
    state", which made it read as a guarantee about the repo — and CLAUDE.md
    repeated that reading. It never was one: the assertion passed identically
    with zero specs committed or fifty. What the repo actually ships is
    asserted in TestWhatThisCheckoutShips, against the real directory.
    """
    return _bag(tmp_path, monkeypatch)


@pytest.fixture
def connected_bag(monkeypatch, tmp_path):
    """One source connected, using the repo's own worked example.

    Deliberately the reference spec rather than a minimal fixture: if the
    example the add-source skill hands people cannot produce a valid DAG, the
    example is broken.
    """
    return _bag(_connector(tmp_path, status="connected"), monkeypatch)


class TestWhatThisCheckoutShips:
    """What `sources/` actually holds, and what that schedules.

    Every connected spec becomes three DAGs on anyone's stack the moment they
    `make up`, and `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` is false, so the
    ingest DAG is live immediately. That is a deliberate choice per source, so
    it is written down twice — once as `status: connected` in the spec, once as
    a line in sources/CONNECTED — and this is where the two are made to agree.
    """

    @staticmethod
    def declared_connected():
        """sources/CONNECTED, one name per line, comments and blanks ignored.

        Parsed here rather than imported so this test still means something if
        the parser is the thing that broke.
        """
        names = []
        for line in (SOURCES / "CONNECTED").read_text().splitlines():
            stripped = line.split("#", 1)[0].strip()
            if stripped:
                names.append(stripped)
        return sorted(names)

    def test_the_connected_specs_are_exactly_the_ones_acknowledged(self):
        import yaml

        connected = sorted(
            path.parent.name for path in SOURCES.glob("*/source.yml")
            if (yaml.safe_load(path.read_text()) or {}).get("status") == "connected")
        assert connected == self.declared_connected(), (
            f"sources/ marks {connected} as connected and sources/CONNECTED lists "
            f"{self.declared_connected()}. Every connected spec schedules an unpaused "
            f"DAG and needs its token_env set, on every clone of this repo — so the "
            f"two lists are kept in step deliberately. If the change is intended, "
            f"edit sources/CONNECTED in the same commit."
        )

    def test_every_shipped_spec_names_a_credential_and_parses(self):
        """A committed spec that cannot load takes the whole DAG folder down."""
        from ingest_runtime.spec import load_all

        specs = load_all()
        assert specs, "sources/ should hold at least the reference connector"
        for spec in specs:
            assert spec.token_env, f"{spec.name} names no token_env"

    def test_the_real_directory_generates_the_dag_ids_the_specs_predict(self, monkeypatch):
        """One derivation of what exists, so the deploy's verification and the
        manifest cannot disagree with what Airflow actually registered."""
        from ingest_runtime.spec import load_all

        bag = _bag(SOURCES, monkeypatch)
        expected = {dag_id for spec in load_all() for dag_id in spec.dag_ids}
        assert set(bag.dag_ids) - {"stack_smoke"} == expected


class TestStatusDecidesWhatIsScheduled:
    """Required, with no default, precisely so that scheduling is never
    something a spec falls into by omission."""

    def test_a_reference_spec_generates_nothing_at_all(self, monkeypatch, tmp_path):
        """This is what lets the worked example live in sources/ beside real
        connectors instead of somewhere no test could reach it."""
        bag = _bag(_connector(tmp_path, status="reference"), monkeypatch)
        assert set(bag.dag_ids) == {"stack_smoke"}

    def test_a_paused_spec_keeps_its_dags_and_loses_its_schedule(self, monkeypatch, tmp_path):
        """Still triggerable by hand, ticking for nobody. That is what makes
        "we are rotating this credential" something other than deleting the
        spec."""
        bag = _bag(_connector(tmp_path, status="paused"), monkeypatch)
        assert {"pylon_ingest", "pylon_backfill"} <= set(bag.dag_ids)
        for dag_id in bag.dag_ids:
            if dag_id != "stack_smoke":
                assert bag.dags[dag_id].schedule is None, dag_id

    def test_a_paused_spec_builds_no_reconcile_dag(self, monkeypatch, tmp_path):
        """The reconcile DAG exists to run on its cron; without one there is
        nothing left of it but a tombstone pass somebody could trigger."""
        bag = _bag(_connector(tmp_path, status="paused"), monkeypatch)
        assert "pylon_reconcile" not in bag.dag_ids

    def test_a_spec_with_no_status_is_skipped_not_defaulted(self, monkeypatch, tmp_path):
        _connector(tmp_path, name="nostatus",
                   edit=lambda text: text.replace("status: reference\n", ""))
        bag = _bag(tmp_path, monkeypatch)
        assert set(bag.dag_ids) == {"stack_smoke"}

    def test_the_directory_name_is_the_identity(self, monkeypatch, tmp_path):
        """A spec whose `name` disagrees with its directory would generate DAG
        ids nothing else predicts, so it is skipped rather than trusted."""
        (tmp_path / "elsewhere").mkdir()
        (tmp_path / "elsewhere" / "source.yml").write_text(
            (REFERENCE / "source.yml").read_text().replace(
                "status: reference", "status: connected"))
        bag = _bag(tmp_path, monkeypatch)
        assert set(bag.dag_ids) == {"stack_smoke"}


class TestTheTwoDerivationsAgree:
    """The generator re-derives orchestration from YAML rather than importing
    the spec parser: the DAG processor runs in Airflow's virtualenv and the
    parser lives in the runtime's, which is the whole reason DAGs shell out.

    The cost is two implementations of the same defaults. This is what stops
    them drifting silently — and drift here is a pool that is never created, or
    a timeout that outlives its own schedule.
    """

    @staticmethod
    def derivations():
        import yaml
        from ingest_runtime.spec import load_all
        from source_dags import _orchestration

        for spec in load_all():
            document = yaml.safe_load((spec.dir / "source.yml").read_text())
            yield spec, _orchestration(document)

    def test_the_pool_is_the_same_name(self):
        for spec, conf in self.derivations():
            assert conf["pool"] == spec.pool, spec.name

    def test_the_schedules_are_the_same(self):
        for spec, conf in self.derivations():
            if not spec.schedules_dags:
                continue
            assert conf["schedule"] == spec.schedule, spec.name
            assert conf["reconcile"] == spec.reconcile_schedule, spec.name

    def test_the_backfill_floor_is_the_same(self):
        for spec, conf in self.derivations():
            assert conf["backfill_start"] == spec.backfill_start, spec.name

    def test_every_timeout_default_is_the_same(self):
        """The defaults differ per task — 55 minutes for an hourly ingest, 12
        hours for a backfill — so a mismatch is invisible until the day a spec
        stops declaring one."""
        for spec, conf in self.derivations():
            assert conf["ingest_timeout"] == spec.timeout_minutes("ingest", 55), spec.name
            assert conf["backfill_timeout"] == spec.timeout_minutes("backfill", 720), spec.name
            assert conf["reconcile_timeout"] == spec.timeout_minutes("reconcile", 1200), spec.name

    def test_the_dag_ids_are_the_same(self, monkeypatch, tmp_path):
        """Whether a reconcile DAG exists was re-decided in three places from
        the same two keys, and they disagreed."""
        from ingest_runtime.spec import load

        for status in ("connected", "paused"):
            directory = _connector(tmp_path, name="pylon", status=status)
            bag = _bag(directory, monkeypatch)
            spec = load("pylon", directory=directory)
            assert set(bag.dag_ids) - {"stack_smoke"} == set(spec.dag_ids), status


class TestEmptyByDefault:
    def test_no_specs_means_no_ingestion_dags(self, empty_bag):
        assert set(empty_bag.dag_ids) == {"stack_smoke"}

    def test_the_smoke_dag_is_never_scheduled(self, empty_bag):
        # It proves the plumbing on demand; on a timer it would just add noise.
        assert empty_bag.dags["stack_smoke"].schedule is None

    def test_a_malformed_spec_does_not_unschedule_the_good_ones(self, monkeypatch, tmp_path):
        (tmp_path / "broken").mkdir()
        (tmp_path / "broken" / "source.yml").write_text("name: [not a name\n")
        bag = _bag(_connector(tmp_path, status="connected"), monkeypatch)
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
        directory = _connector(
            tmp_path, name="nofloor", status="connected",
            edit=lambda text: text.replace('backfill_start: "2019-01-01"', ""))
        bag = _bag(directory, monkeypatch)
        assert "nofloor_ingest" in bag.dag_ids
        assert "nofloor_reconcile" not in bag.dag_ids


def test_the_reference_connector_is_a_directory_with_its_extension_beside_it():
    """A connector is a directory now, and the generator globs `*/source.yml`.

    A flat `sources/<name>.yml` left behind by a half-done migration would be
    silently ignored — the connector would simply stop being scheduled.
    """
    assert (REFERENCE / "source.yml").is_file()
    assert (REFERENCE / "extension.py").is_file()
    assert not list(SOURCES.glob("*.yml")), \
        f"flat specs are no longer read: {[p.name for p in SOURCES.glob('*.yml')]}"
