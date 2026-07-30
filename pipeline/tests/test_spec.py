"""The source contract must reject a spec it cannot be trusted to act on.

These are mostly negative tests, deliberately. A spec that loads but means
something other than what its author wrote produces a connector that runs, looks
healthy, and lands the wrong data — and every downstream check in this stack is
built on the assumption that the spec is what the source actually is.
"""

import pathlib

import pytest

from ingest_runtime import spec

MINIMAL = """
name: t
api:
  base_url: https://example.test
  auth: {type: bearer, token_env: T_TOKEN}
resources:
  - {name: a, primary_key: id}
"""


def write(tmp_path, text, name="t"):
    (tmp_path / f"{name}.yml").write_text(text)
    return tmp_path


class TestRejects:
    @pytest.mark.parametrize(
        "label, text",
        [
            ("unknown top-level key", MINIMAL + "quallity: {}\n"),
            (
                "unknown resource key",
                MINIMAL.replace("{name: a, primary_key: id}", "{name: a, primary_key: id, timestamp_column: [x]}"),
            ),
            (
                "unknown incremental strategy",
                MINIMAL.replace("{name: a, primary_key: id}", "{name: a, primary_key: id, incremental: {strategy: magic}}"),
            ),
            ("duplicate resource", MINIMAL + "  - {name: a, primary_key: id}\n"),
            ("resource without a primary key", "name: t\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\nresources:\n  - {name: a}\n"),
            ("auth without a token env var", "name: t\napi:\n  base_url: https://e.test\n  auth: {type: bearer}\nresources:\n  - {name: a, primary_key: id}\n"),
            ("no resources at all", "name: t\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\nresources: []\n"),
            ("reference to an undeclared table", MINIMAL + "quality:\n  references: [{child: a.x, parent: ghost.id}]\n"),
            ("reference that is not table.column", MINIMAL + "quality:\n  references: [{child: a, parent: a.id}]\n"),
            ("unknown quality key", MINIMAL + "quality:\n  freshnes: {}\n"),
        ],
    )
    def test_a_spec_that_cannot_be_trusted_raises(self, tmp_path, label, text):
        with pytest.raises(spec.SpecError):
            spec.load("t", directory=write(tmp_path, text))

    def test_a_missing_spec_names_the_sources_that_do_exist(self, tmp_path):
        with pytest.raises(spec.SpecError, match="no source spec"):
            spec.load("nope", directory=tmp_path)

    def test_malformed_yaml_says_so_rather_than_traceback(self, tmp_path):
        with pytest.raises(spec.SpecError, match="not valid YAML"):
            spec.load("t", directory=write(tmp_path, "name: t\n  bad indent: ["))


REFERENCE_DIR = pathlib.Path(__file__).resolve().parents[2] / ".claude/skills/add-source/reference"


class TestReferenceExample:
    """The worked example the add-source skill points at must stay valid.

    sources/ is empty by default, so this spec is not loaded by the stack — which
    is exactly why it needs a test. An example that has quietly drifted out of
    sync with the loader teaches every future connector something wrong.
    """

    @pytest.fixture
    def pylon(self):
        return spec.load("pylon", directory=REFERENCE_DIR)

    def test_the_dataset_is_derived_not_configurable(self, pylon):
        # Two sources sharing one raw database would share one soft-delete pass.
        assert pylon.dataset == "raw_pylon"

    def test_it_declares_every_resource_the_pipeline_loads(self, pylon):
        assert set(pylon.resource_names) == {
            "issues", "issue_messages", "accounts", "users", "teams", "contacts",
        }

    def test_only_the_directory_tables_are_tombstoned(self, pylon):
        # issue_messages is append-only and has no cross-issue endpoint, so a
        # full absence scan is never available for it.
        assert set(pylon.soft_delete_tables) == {"accounts", "users", "teams", "contacts"}

    def test_issues_carries_both_fetch_modes(self, pylon):
        """GET /issues filters on created_at and would miss an old issue updated
        today; the search endpoint filters on updated_at. Losing either mode
        silently loses rows."""
        issues = pylon.resource("issues")
        assert issues.strategy == "search_window"
        assert issues.incremental["window"]["filters_on"] == "created_at"
        assert issues.incremental["window"]["max_window_days"] == 30
        assert issues.incremental["search"]["cursor_in"] == "body"

    def test_the_ingest_timeout_fits_inside_its_own_schedule(self, pylon):
        """An hourly ingest allowed to run longer than an hour queues the next
        run behind the pool it is still holding."""
        assert pylon.orchestration["schedule"] == "17 * * * *"
        assert pylon.timeout_minutes("ingest", 0) < 60

    def test_the_backfill_floor_is_declared(self, pylon):
        # The tombstone guard compares against this; a later value silently
        # disqualifies issues from the soft-delete pass.
        assert pylon.backfill_start == "2019-01-01"

    def test_the_awkward_parts_are_delegated_to_an_extension(self, pylon):
        """Pylon's glitch paginator and its warehouse-watermark worklist cannot
        be expressed declaratively. The spec must say so rather than pretend."""
        assert pylon.extensions == "pylon"
        assert pylon.resource("issue_messages").strategy == "parent_watermark"

    def test_every_rate_limit_family_belongs_to_a_resource(self, pylon):
        """A budget for a family nothing uses is a budget that does nothing."""
        families = {r.family for r in pylon.resources}
        # issues declares its families inside `incremental`, not `endpoint`.
        declared = set(pylon.rate_limits)
        assert declared, "the spec should carry researched rate limits"
        assert "directory" in families


def test_the_repo_ships_with_no_sources_connected():
    """A fresh checkout ingests nothing. Someone connects a source deliberately;
    the stack does not arrive pretending to have one."""
    assert spec.available() == [], f"unexpected specs in {spec.sources_dir()}"


def test_the_sources_directory_exists_and_is_where_it_should_be():
    assert spec.sources_dir().is_dir(), spec.sources_dir()
    assert isinstance(spec.REPO_ROOT, pathlib.Path)


def test_every_reference_example_loads():
    """Whatever the skill ships as an example must parse under today's loader."""
    examples = sorted(REFERENCE_DIR.glob("*.yml"))
    assert examples, "the add-source skill should ship at least one worked example"
    for path in examples:
        spec.load(path.stem, directory=REFERENCE_DIR)
