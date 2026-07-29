"""The spec-driven runtime must reproduce the hand-written Pylon code exactly.

This is the safety net for the whole generalisation. `sources/pylon.yml` claims
to describe what ingest/hints.py and ingest/transform.py do; if it only nearly
does, the connector platform silently lands different data than the pipeline
that has been running in production, and every downstream expectation and
transform is written against the old shape.

So these tests compare the two implementations rather than asserting the new one
looks reasonable. When the hand-written modules are eventually deleted, this
file is what licenses that.
"""

import typing

import pendulum
import pytest

from pylon_pipeline import runtime, spec
from pylon_pipeline.ingest import hints as hardcoded_hints
from pylon_pipeline.ingest import transform as hardcoded_transform
from pylon_pipeline.ingest.settings import RATE_LIMITS, SOFT_DELETE_DIRECTORY_TABLES


@pytest.fixture
def pylon():
    return spec.load("pylon")


def declarations(hints):
    """Just the type declarations, dropping anything dlt added in place.

    ingest/hints.py shares one `_TS` dict object across several columns, and dlt
    writes a `name` key into the hint dicts it is handed — so once any pipeline
    has run in the same process, the hardcoded module's dicts carry an extra key
    that has nothing to do with what their author declared. The runtime hands
    dlt copies, which is why it does not drift this way. Comparing declarations
    keeps this test about the contract rather than about test ordering.
    """
    return {
        column: {k: v for k, v in hint.items() if k in ("data_type", "precision")}
        for column, hint in hints.items()
    }


class TestHints:
    """dlt types the cursor columns from these. A hint the spec forgets is a
    cursor compared as text — which sorts correctly right up until it doesn't."""

    def test_issue_hints_match(self, pylon):
        derived = runtime.column_hints(pylon.resource("issues"))
        assert declarations(derived) == declarations(hardcoded_hints.ISSUE_HINTS)

    def test_message_hints_cover_the_cursor_column(self, pylon):
        derived = runtime.column_hints(pylon.resource("issue_messages"))
        # The hand-written version also hints issue_id as text. The spec does
        # not, because that is a value dlt infers correctly and hinting it adds
        # a maintenance burden without a failure it prevents.
        assert declarations(derived)["timestamp"] == declarations(hardcoded_hints.MESSAGE_HINTS)["timestamp"]

    @pytest.mark.parametrize("table", ["accounts", "users", "teams", "contacts"])
    def test_every_directory_table_is_hinted_for_tombstoning(self, pylon, table):
        derived = runtime.column_hints(pylon.resource(table))
        assert declarations(derived)["_deleted"] == declarations(hardcoded_hints.DIRECTORY_HINTS)["_deleted"]


class TestTransform:
    ISSUE: typing.ClassVar = {
        "id": "iss_1",
        "title": "Something broke",
        "account": {"id": "acc_1", "name": "Acme"},
        "assignee": {"id": "usr_1", "email": "agent@example.com"},
        "requester": {"id": "con_1", "email": "customer@example.com"},
        "team": {"id": "team_1"},
        "custom_fields": {"priority": {"slug": "priority", "value": "high"}},
        "tags": ["billing", "urgent"],
        "created_at": "2026-01-01T10:00:00Z",
        "updated_at": "2026-01-02T11:00:00Z",
        "latest_message_time": "2026-01-02T11:30:00Z",
    }

    def test_an_issue_lands_identically(self, pylon):
        derived = runtime.make_transformer(pylon.resource("issues"))(dict(self.ISSUE))
        expected = hardcoded_transform.flatten_issue(dict(self.ISSUE))
        assert derived == expected

    def test_the_promoted_scalars_are_the_ones_that_were_promoted(self, pylon):
        row = runtime.make_transformer(pylon.resource("issues"))(dict(self.ISSUE))
        assert row["account_id"] == "acc_1"
        assert row["assignee_id"] == "usr_1"
        assert row["assignee_email"] == "agent@example.com"
        assert row["requester_id"] == "con_1"
        assert row["requester_email"] == "customer@example.com"
        assert row["team_id"] == "team_1"

    def test_nested_objects_stay_json_rather_than_becoming_columns(self, pylon):
        """A new custom field on the source side must not mint a warehouse
        column, or a child table, on the next run."""
        row = runtime.make_transformer(pylon.resource("issues"))(dict(self.ISSUE))
        assert isinstance(row["custom_fields"], str)
        assert isinstance(row["tags"], str)

    def test_timestamps_are_parsed_not_left_as_strings(self, pylon):
        row = runtime.make_transformer(pylon.resource("issues"))(dict(self.ISSUE))
        assert isinstance(row["created_at"], pendulum.DateTime)
        assert isinstance(row["latest_message_time"], pendulum.DateTime)

    def test_a_missing_nested_parent_promotes_to_none_not_a_crash(self, pylon):
        """An unassigned ticket has no assignee object at all."""
        row = runtime.make_transformer(pylon.resource("issues"))({"id": "i", "created_at": None})
        assert row["assignee_id"] is None
        assert row["account_id"] is None

    def test_a_message_lands_identically(self, pylon):
        message = {"id": "msg_1", "message_html": 'He said <b>"hi"</b>',
                   "timestamp": "2026-01-02T11:30:00Z"}
        derived = runtime.make_transformer(pylon.resource("issue_messages"))(dict(message))
        expected = hardcoded_transform.enrich_message(dict(message), issue_id="iss_1")
        # enrich_message additionally stamps issue_id, which the runtime does at
        # fetch time because only the caller knows which parent it asked for.
        expected.pop("issue_id")
        assert derived == expected

    def test_stripped_html_carries_no_escaping(self, pylon):
        """A regression guard inherited from the pipeline this replaced: the
        legacy tooling stored escaped quotes and made every text search wrong."""
        row = runtime.make_transformer(pylon.resource("issue_messages"))(
            {"id": "m", "message_html": 'He said <b>"hi"</b>'})
        assert "\\" not in row["message_text"]
        assert row["message_text"] == 'He said "hi"'

    def test_a_directory_record_lands_identically(self, pylon):
        account = {"id": "acc_1", "name": "Acme", "domains": ["acme.test"],
                   "created_at": "2026-01-01T00:00:00Z"}
        derived = runtime.make_transformer(pylon.resource("accounts"))(dict(account))
        expected = hardcoded_transform.flatten_directory_record(dict(account))
        assert derived == expected
        assert derived["_deleted"] is False


class TestOrchestrationFacts:
    """The spec is also the operational contract, so the values the DAGs and the
    soft-delete guard depend on must survive the round trip."""

    def test_rate_limits_match_what_the_pacer_was_built_with(self, pylon):
        assert pylon.rate_limits == RATE_LIMITS

    def test_the_tombstoned_tables_match(self, pylon):
        assert set(pylon.soft_delete_tables) == set(SOFT_DELETE_DIRECTORY_TABLES)

    def test_the_pacer_is_built_from_the_declared_budgets(self, pylon):
        paced = runtime.pacer(pylon)
        # Requesting an unknown family must not silently pace at infinity.
        paced.wait("issues_list")
        assert paced.requests_made["issues_list"] == 1


class TestExtensions:
    def test_a_spec_pointing_at_a_missing_module_says_which_one(self, tmp_path):
        (tmp_path / "x.yml").write_text(
            "name: x\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\n"
            "resources:\n  - {name: a, primary_key: id}\nextensions: nonexistent\n"
        )
        loaded = spec.load("x", directory=tmp_path)
        with pytest.raises(RuntimeError, match="nonexistent"):
            runtime.extensions(loaded)

    def test_a_source_without_extensions_needs_no_python(self, tmp_path):
        (tmp_path / "y.yml").write_text(
            "name: y\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\n"
            "resources:\n  - {name: a, primary_key: id}\n"
        )
        assert runtime.extensions(spec.load("y", directory=tmp_path)) is None


class TestSoftDeleteModes:
    """Absence means different things depending on what the run fetched.

    This distinction was flattened into a bool in the first draft of the spec,
    and the equivalence test above is what caught it: `issues` carries a
    _deleted column and IS tombstoned, but only by a run that covered all of
    history. Applying the directory tables' `always` mode to it would tombstone
    every issue outside the window on the first incremental run.
    """

    def test_the_two_modes_are_kept_apart(self, pylon):
        assert set(pylon.soft_delete_tables) == {"accounts", "users", "teams", "contacts"}
        assert set(pylon.full_history_soft_delete_tables) == {"issues"}

    def test_everything_tombstoned_under_either_mode_gets_the_column(self, pylon):
        for table in pylon.tombstoned_tables:
            assert "_deleted" in runtime.column_hints(pylon.resource(table)), table

    def test_messages_are_never_tombstoned(self, pylon):
        """Append-only, and with no cross-issue endpoint there is never a full
        fetch to compute absence from."""
        assert pylon.resource("issue_messages").soft_delete is None

    def test_a_bare_boolean_is_rejected(self, tmp_path):
        (tmp_path / "b.yml").write_text(
            "name: b\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\n"
            "resources:\n  - {name: a, primary_key: id, soft_delete: true}\n"
        )
        loaded = spec.load("b", directory=tmp_path)
        with pytest.raises(spec.SpecError, match="soft_delete must be one of"):
            _ = loaded.resource("a").soft_delete
