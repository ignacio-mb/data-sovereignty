"""The source contract must reject a spec it cannot be trusted to act on.

Mostly negative tests, deliberately. A spec that loads but means something
other than what its author wrote produces a connector that runs, looks healthy,
and lands the wrong data — and every downstream check in this stack is built on
the assumption that the spec is what the source actually is.

The fixtures here write into a real sources DIRECTORY, schema and all, because
that is now part of the contract: the shape lives in sources/source.schema.json
and is enforced on load rather than only in CI. A spec that reached the runtime
unvalidated is one whose typo becomes a silently missing column.
"""

from __future__ import annotations

import pathlib
import shutil

import pytest

from ingest_runtime import spec

MINIMAL = """
name: probe
status: reference
api:
  base_url: https://example.test
  auth: {type: bearer, token_env: PROBE_TOKEN}
resources:
  - {name: things, primary_key: id}
"""

REAL_SCHEMA = spec.sources_dir() / spec.SCHEMA_FILENAME


def write(tmp_path, text, name="probe", with_schema=True):
    """A connector directory, with the same schema the real one is validated against."""
    if with_schema:
        shutil.copy(REAL_SCHEMA, tmp_path / spec.SCHEMA_FILENAME)
    (tmp_path / name).mkdir(parents=True, exist_ok=True)
    (tmp_path / name / spec.SPEC_FILENAME).write_text(text)
    return tmp_path


class TestRejects:
    @pytest.mark.parametrize(
        "label, text",
        [
            ("unknown top-level key", MINIMAL + "quallity: {}\n"),
            (
                "unknown resource key",
                MINIMAL.replace("{name: things, primary_key: id}",
                                "{name: things, primary_key: id, timestamp_column: [x]}"),
            ),
            (
                "unknown endpoint key",
                MINIMAL.replace("{name: things, primary_key: id}",
                                "{name: things, primary_key: id, endpoint: {pageSize: 10}}"),
            ),
            (
                "unknown incremental strategy",
                MINIMAL.replace("{name: things, primary_key: id}",
                                "{name: things, primary_key: id, incremental: {strategy: magic}}"),
            ),
            (
                "a cursor strategy with nothing to push into the API",
                MINIMAL.replace("{name: things, primary_key: id}",
                                "{name: things, primary_key: id, "
                                "incremental: {strategy: cursor, cursor_field: updated_at}}"),
            ),
            ("duplicate resource", MINIMAL + "  - {name: things, primary_key: id}\n"),
            ("resource without a primary key",
             MINIMAL.replace("{name: things, primary_key: id}", "{name: things}")),
            ("auth without a token env var",
             MINIMAL.replace("{type: bearer, token_env: PROBE_TOKEN}", "{type: bearer}")),
            ("an auth type nothing implements",
             MINIMAL.replace("type: bearer", "type: hmac_nonce")),
            ("a credential written into the spec",
             MINIMAL.replace("token_env: PROBE_TOKEN", "token_env: sk-live-1234")),
            ("no resources at all", MINIMAL.replace("  - {name: things, primary_key: id}\n", "")),
            ("no status", MINIMAL.replace("status: reference\n", "")),
            ("a status nothing knows what to do with", MINIMAL.replace("reference", "draft")),
            ("soft_delete as a bare boolean",
             MINIMAL.replace("{name: things, primary_key: id}",
                             "{name: things, primary_key: id, soft_delete: true}")),
            ("reference to an undeclared table",
             MINIMAL + "quality:\n  references: [{child: things.x, parent: ghost.id}]\n"),
            ("reference that is not table.column",
             MINIMAL + "quality:\n  references: [{child: things, parent: things.id}]\n"),
            ("freshness on a table this source does not land",
             MINIMAL + "quality:\n  freshness: {table: ghost, column: updated_at}\n"),
            ("unknown quality key", MINIMAL + "quality:\n  freshnes: {}\n"),
        ],
    )
    def test_a_spec_that_cannot_be_trusted_raises(self, tmp_path, label, text):
        with pytest.raises(spec.SpecError):
            spec.load("probe", directory=write(tmp_path, text))

    def test_the_directory_and_the_name_must_agree(self, tmp_path):
        """The directory is the identity everything else is derived from — the
        database, the pool, the DAG ids — so a mismatch produces DAG ids
        nothing else predicts."""
        with pytest.raises(spec.SpecError, match="directory is"):
            spec.load("elsewhere", directory=write(tmp_path, MINIMAL, name="elsewhere"))

    def test_the_old_flat_layout_says_where_the_spec_should_go(self, tmp_path):
        """A connector is a directory now, so its extension, fixtures, research
        and reviewed schemas can live beside the contract they belong to."""
        (tmp_path / "probe.yml").write_text(MINIMAL)
        with pytest.raises(spec.SpecError, match="old flat layout"):
            spec.load("probe", directory=tmp_path)

    def test_a_missing_spec_names_the_sources_that_do_exist(self, tmp_path):
        write(tmp_path, MINIMAL)
        with pytest.raises(spec.SpecError, match="probe"):
            spec.load("nope", directory=tmp_path)

    def test_malformed_yaml_says_so_rather_than_traceback(self, tmp_path):
        with pytest.raises(spec.SpecError, match="not valid YAML"):
            spec.load("probe", directory=write(tmp_path, "name: probe\n  bad indent: ["))


class TestAccepts:
    def test_the_minimal_spec_loads(self, tmp_path):
        loaded = spec.load("probe", directory=write(tmp_path, MINIMAL))
        assert loaded.resource_names == ("things",)
        assert loaded.status == "reference"

    def test_freshness_may_be_a_list(self, tmp_path):
        """A source with an append-only event table and a slowly-changing
        entity table has two answers to "is this stale", and allowing only one
        meant declaring the less useful of them."""
        text = MINIMAL + (
            "  - {name: events, primary_key: id}\n"
            "quality:\n"
            "  freshness:\n"
            "    - {table: things, column: updated_at, hours: 24}\n"
            "    - {table: events, column: occurred_at, hours: 2, severity: error}\n"
        )
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert [entry["table"] for entry in loaded.freshness_checks] == ["things", "events"]

    def test_one_freshness_mapping_still_reads_as_a_list(self, tmp_path):
        text = MINIMAL + "quality:\n  freshness: {table: things, column: updated_at}\n"
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert len(loaded.freshness_checks) == 1

    def test_no_freshness_at_all_is_an_empty_tuple(self, tmp_path):
        """Never None: every caller iterates it."""
        assert spec.load("probe", directory=write(tmp_path, MINIMAL)).freshness_checks == ()

    def test_a_directory_with_no_schema_still_validates_semantically(self, tmp_path):
        """The schema is optional — a tmp sources dir in a test legitimately has
        none — but the checks that need Python are not."""
        directory = write(tmp_path, MINIMAL + "quality:\n"
                          "  references: [{child: things.x, parent: ghost.id}]\n",
                          with_schema=False)
        with pytest.raises(spec.SpecError, match="ghost"):
            spec.load("probe", directory=directory)


class TestStatus:
    """Required, with no default, so scheduling is never fallen into."""

    @pytest.mark.parametrize(
        "status, connected, schedules",
        [("connected", True, True), ("paused", False, True), ("reference", False, False)])
    def test_what_each_status_means(self, tmp_path, status, connected, schedules):
        loaded = spec.load("probe", directory=write(
            tmp_path, MINIMAL.replace("status: reference", f"status: {status}")))
        assert loaded.is_connected is connected
        assert loaded.schedules_dags is schedules

    def test_a_reference_spec_generates_no_dag_ids(self, tmp_path):
        text = MINIMAL + ("orchestration:\n  schedule: '17 * * * *'\n"
                          "  reconcile: '0 3 * * 6'\n  backfill_start: '2020-01-01'\n")
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert loaded.dag_ids == ()

    def test_a_reconcile_dag_needs_a_declared_history_floor(self, tmp_path):
        """Tombstoning compares the run's window against backfill_start.
        Without one there is nothing to compare against."""
        base = MINIMAL.replace("status: reference", "status: connected")
        with_floor = base + ("orchestration:\n  schedule: '17 * * * *'\n"
                             "  reconcile: '0 3 * * 6'\n  backfill_start: '2020-01-01'\n")
        without = base + "orchestration:\n  schedule: '17 * * * *'\n  reconcile: '0 3 * * 6'\n"
        assert spec.load("probe", directory=write(tmp_path, with_floor)).dag_ids == (
            "probe_ingest", "probe_backfill", "probe_reconcile")
        assert spec.load("probe", directory=write(tmp_path, without)).dag_ids == (
            "probe_ingest", "probe_backfill")


class TestDerivedIdentity:
    def test_the_database_is_derived_not_configurable(self, tmp_path):
        """Two sources sharing one raw database would share one soft-delete
        pass, so there is no key that lets a spec name it."""
        assert spec.load("probe", directory=write(tmp_path, MINIMAL)).dataset == "raw_probe"

    def test_the_pool_defaults_to_one_per_source(self, tmp_path):
        assert spec.load("probe", directory=write(tmp_path, MINIMAL)).pool == "probe_pipeline"

    def test_the_runtime_tier_can_be_sized_per_task(self, tmp_path):
        text = MINIMAL + "orchestration:\n  runtime: {backfill: heavy}\n"
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert loaded.runtime_tier("backfill") == "heavy"
        assert loaded.runtime_tier("ingest") == "standard"

    def test_a_bare_runtime_string_sizes_every_task(self, tmp_path):
        text = MINIMAL + "orchestration:\n  runtime: long\n"
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert {loaded.runtime_tier(task) for task in ("ingest", "backfill", "reconcile")} == \
            {"long"}


class TestTheShippedDirectory:
    def test_it_is_where_it_should_be(self):
        assert spec.sources_dir().is_dir(), spec.sources_dir()
        assert isinstance(spec.REPO_ROOT, pathlib.Path)

    def test_every_connector_on_disk_loads(self):
        """A committed spec that cannot load takes the whole DAG folder down."""
        assert spec.available(), "sources/ should hold at least the reference connector"
        for loaded in spec.load_all():
            assert loaded.token_env, f"{loaded.name} names no credential"

    def test_the_reference_connector_lives_in_sources_like_the_rest(self):
        """`reference` is the status that lets the worked example live beside
        real connectors instead of somewhere no test could reach it — which is
        how it came to declare an extension module that did not exist."""
        pylon = spec.load("pylon")
        assert pylon.status == "reference"
        assert not pylon.schedules_dags
        assert pylon.dir == spec.sources_dir() / "pylon"
        assert pylon.extension_path.is_file()

    def test_the_awkward_parts_of_the_example_are_delegated(self, ):
        """Pylon's glitch paginator and its warehouse-watermark worklist cannot
        be expressed declaratively. The spec must say so rather than pretend."""
        pylon = spec.load("pylon")
        assert pylon.uses_extension
        assert {r.name for r in pylon.delegated_resources} == {"issues", "issue_messages"}
        assert pylon.resource("issue_messages").strategy == "parent_watermark"

    def test_the_ingest_timeout_fits_inside_its_own_schedule(self):
        """An hourly ingest allowed to run longer than an hour queues the next
        run behind the pool it is still holding."""
        for loaded in spec.load_all():
            schedule = loaded.schedule or ""
            if schedule.split()[1:2] == ["*"]:
                assert loaded.timeout_minutes("ingest", 55) < 60, loaded.name

    def test_every_rate_limit_family_is_routed_by_some_endpoint(self):
        """A budget for a family nothing uses is a budget that does nothing."""
        for loaded in spec.load_all():
            routed = {family for r in loaded.resources for family in r.families}
            assert set(loaded.rate_limits) <= routed, loaded.name


class TestResourceShape:
    def test_hint_columns_default_to_the_timestamps(self, tmp_path):
        text = MINIMAL.replace(
            "{name: things, primary_key: id}",
            "{name: things, primary_key: id, timestamp_columns: [created_at, updated_at]}")
        resource = spec.load("probe", directory=write(tmp_path, text)).resource("things")
        assert resource.hint_columns == ("created_at", "updated_at")

    def test_a_resource_may_narrow_them(self, tmp_path):
        """A resource that parses more timestamps than it compares keeps the
        rest inferred, so a column nothing depends on is not a commitment."""
        text = MINIMAL.replace(
            "{name: things, primary_key: id}",
            "{name: things, primary_key: id, timestamp_columns: [created_at, updated_at], "
            "hint_columns: [updated_at]}")
        resource = spec.load("probe", directory=write(tmp_path, text)).resource("things")
        assert resource.hint_columns == ("updated_at",)

    def test_the_data_selector_falls_back_to_the_source_wide_one(self, tmp_path):
        """One API can wrap two endpoints differently, which cost Customer.io
        its declaration entirely — a single selector is wrong for one of them."""
        text = (MINIMAL + "pagination:\n  data_selector: data\n"
                "  kind: cursor\n").replace(
            "  - {name: things, primary_key: id}",
            "  - {name: things, primary_key: id}\n"
            "  - {name: others, primary_key: id, endpoint: {data_selector: records}}")
        loaded = spec.load("probe", directory=write(tmp_path, text))
        assert loaded.resource("things").data_selector(loaded) == "data"
        assert loaded.resource("others").data_selector(loaded) == "records"

    def test_every_endpoint_a_resource_may_call_is_visible(self, tmp_path):
        """search_window calls two endpoints, separately rate-limited in every
        API that offers both. Reading only the primary one meant the others
        were invisible to the pacer: Pylon declared three budgets and all three
        were routed to a family nothing billed."""
        issues = spec.load("pylon").resource("issues")
        assert set(issues.families) == {"issues_list", "issues_search"}

    def test_a_resource_declaring_no_endpoint_still_has_a_route(self, tmp_path):
        """The declarative path defaults the path to /<name>, so the route
        table has to make the same assumption or the requests go unmatched."""
        resource = spec.load("probe", directory=write(tmp_path, MINIMAL)).resource("things")
        assert resource.all_endpoints == (({"path": "/things"}, "things"),)

    def test_only_the_two_declarative_strategies_build_without_python(self):
        assert spec.DECLARATIVE_STRATEGIES == {"full_refresh", "cursor"}
