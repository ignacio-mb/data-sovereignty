"""The suite builder must turn a spec into valid GX objects without a database.

The fixture is the reference connector rather than a minimal stub: the
expectations are generated from the contract, so if the worked example the
add-source skill hands people does not produce a sensible contract, the example
is wrong.

It is loaded from the real `sources/` directory, where `status: reference` is
what lets it live beside the connected specs without scheduling anything. It
used to sit inside the skill, three directories from everything that reads it,
which is how it came to declare an extension module that did not exist.
"""

import types

import pytest
from ingest_runtime.spec import SPEC_FILENAME, load, sources_dir
from quality_runtime import config, results
from quality_runtime.suites import raw

REFERENCE_SPEC = sources_dir() / "pylon" / SPEC_FILENAME


def write(tmp_path, text, name):
    """A connector directory, which is what the loader now expects."""
    (tmp_path / name).mkdir(parents=True, exist_ok=True)
    (tmp_path / name / SPEC_FILENAME).write_text(text)
    return load(name, directory=tmp_path)


@pytest.fixture
def spec():
    return load("pylon")


def expectation_types(expectations):
    return [expectation.expectation_type for expectation in expectations]


def descriptions(expectations):
    return [expectation.description or "" for expectation in expectations]


class TestGeneratedFromTheSpec:
    def test_covers_every_resource_the_spec_declares(self, spec):
        suites = raw.build(spec)
        assert {table for _, table in suites} == set(raw.entity_tables(spec))
        assert all(schema == spec.dataset for schema, _ in suites)

    def test_the_database_comes_from_the_source_name(self, spec):
        assert spec.dataset == "raw_pylon"
        assert all(schema == "raw_pylon" for schema, _ in raw.build(spec))

    def test_every_table_asserts_its_merge_key(self, spec):
        """A duplicate primary key means the merge key broke, which is the one
        failure that silently corrupts an incremental load."""
        for (_, table), suite in raw.build(spec).items():
            texts = descriptions(suite)
            assert f"{table}.id is unique" in texts, table
            assert f"{table}.id is never null" in texts, table

    def test_identity_checks_are_not_opt_in(self, spec):
        """The spec's `quality` block says nothing about primary keys, and every
        table still gets them — a spec author cannot forget these."""
        assert "primary_key" not in str(spec.quality)
        for (_, table), suite in raw.build(spec).items():
            assert any("is unique" in text for text in descriptions(suite)), table

    def test_only_soft_deleted_tables_get_the_tombstone_check(self, spec):
        tombstoned = set(spec.tombstoned_tables)
        assert tombstoned, "the reference spec should exercise soft delete"
        for (_, table), suite in raw.build(spec).items():
            has_check = any("is tombstoned" in text for text in descriptions(suite))
            assert has_check == (table in tombstoned), table

    def test_the_tombstone_fraction_comes_from_the_spec(self, spec):
        assert spec.quality["max_deleted_fraction"] == 0.5
        suite = raw.build(spec)[(spec.dataset, "accounts")]
        assert any("at most 50% of accounts is tombstoned" in text
                   for text in descriptions(suite))

    def test_declared_not_null_columns_are_checked(self, spec):
        suites = raw.build(spec)
        for table, columns in spec.quality["not_null"].items():
            texts = descriptions(suites[(spec.dataset, table)])
            for column in columns:
                assert f"{table}.{column} is never null" in texts

    def test_freshness_reads_its_table_column_and_hours_from_the_spec(self, spec):
        declared = spec.quality["freshness"]
        suite = raw.build(spec)[(spec.dataset, declared["table"])]
        freshness = [text for text in descriptions(suite) if "within the last" in text]
        assert len(freshness) == 1
        assert freshness[0].startswith(
            f"{declared['table']}.{declared['column']} is within the last {declared['hours']}h")

    def test_an_operator_can_override_the_freshness_slo(self, spec, monkeypatch):
        """One loose run without editing the contract."""
        monkeypatch.setenv("GX_FRESHNESS_HOURS", "6")
        suite = raw.build(spec)[(spec.dataset, "issues")]
        freshness = [text for text in descriptions(suite) if "within the last" in text]
        assert freshness[0].startswith("issues.updated_at is within the last 6h")

    def test_freshness_is_advisory_and_everything_else_is_not(self, spec):
        """A quiet source must not redden the DAG, but nothing else may opt out.

        Freshness measures upstream activity, not pipeline health, so it warns.
        Making it advisory is only safe if advisory stays the exception.
        """
        advisory, gating = [], []
        for suite in raw.build(spec).values():
            for expectation in suite:
                meta = expectation.meta or {}
                target = advisory if meta.get("severity") == "warn" else gating
                target.append(expectation.description or expectation.expectation_type)

        assert advisory, "freshness should be marked advisory"
        assert all("within the last" in text for text in advisory), advisory
        assert not any("within the last" in text for text in gating)

    def test_a_spec_can_gate_on_freshness_instead(self, spec, tmp_path):
        """A source genuinely expected to change hourly should be able to fail."""
        strict = write(tmp_path, REFERENCE_SPEC.read_text().replace(
            "severity: warn", "severity: error"), "pylon")
        suite = raw.build(strict)[(strict.dataset, "issues")]
        freshness = [e for e in suite if "within the last" in (e.description or "")]
        assert freshness[0].meta["severity"] == "error"

    def test_children_are_checked_against_their_parents(self, spec):
        suites = raw.build(spec)
        for edge in spec.quality["references"]:
            child_table, child_column = edge["child"].split(".")
            parent_table = edge["parent"].split(".")[0]
            texts = descriptions(suites[(spec.dataset, child_table)])
            assert any(f"{child_table}.{child_column} resolves to a loaded {parent_table}" in text
                       for text in texts), edge

    def test_an_orphan_check_joins_on_the_declared_parent_column(self, spec):
        """The parent column comes from the spec, not assumed to be `id`."""
        suite = raw.build(spec)[(spec.dataset, "issue_messages")]
        query = next(e.unexpected_rows_query for e in suite
                     if "resolves to a loaded issues" in (e.description or ""))
        assert "LEFT ANTI JOIN raw_pylon.issues AS parent" in query
        assert "ON parent.id = child.issue_id" in query


class TestASourceWithMoreThanOneClock:
    """A source can have two answers to "is this stale".

    An append-only event table and a slowly-changing entity table go quiet on
    completely different timescales, and allowing only one freshness contract
    meant declaring the less useful of them — or gating an hourly event feed on
    a check tuned for a table that changes weekly.
    """

    TWO = (
        "name: pair\n"
        "status: reference\n"
        "api:\n"
        "  base_url: https://example.test\n"
        "  auth: {type: bearer, token_env: PAIR_TOKEN}\n"
        "resources:\n"
        "  - {name: events, primary_key: id}\n"
        "  - {name: accounts, primary_key: id}\n"
        "quality:\n"
        "  freshness:\n"
        "    - {table: events, column: occurred_at, hours: 2, severity: error}\n"
        "    - {table: accounts, column: updated_at, hours: 168}\n"
    )

    @pytest.fixture
    def pair(self, tmp_path):
        return write(tmp_path, self.TWO, "pair")

    def test_each_entry_becomes_its_own_expectation(self, pair):
        suites = raw.build(pair)
        for table, column, hours in (("events", "occurred_at", 2),
                                     ("accounts", "updated_at", 168)):
            freshness = [text for text in descriptions(suites[(pair.dataset, table)])
                         if "within the last" in text]
            assert len(freshness) == 1, table
            assert freshness[0].startswith(f"{table}.{column} is within the last {hours}h")

    def test_each_entry_keeps_its_own_severity(self, pair):
        """Advisory is the default because a quiet source is not a broken
        pipeline — but a feed genuinely expected to move hourly can gate."""
        suites = raw.build(pair)
        severity = {
            table: next(e.meta["severity"] for e in suites[(pair.dataset, table)]
                        if "within the last" in (e.description or ""))
            for table in ("events", "accounts")
        }
        assert severity == {"events": "error", "accounts": "warn"}

    def test_an_entry_whose_table_never_landed_is_left_out(self, pair):
        suites = raw.build(pair, present={"accounts"})
        assert (pair.dataset, "events") not in suites
        assert any("within the last" in text
                   for text in descriptions(suites[(pair.dataset, "accounts")]))


class TestNarrowedToWhatLanded:
    """A resource that never yielded a row has no table: dlt creates it on first
    write. Validating the tables that exist beats failing all of them."""

    def test_a_table_that_never_landed_is_left_out(self, spec):
        present = set(raw.entity_tables(spec)) - {"teams"}
        suites = raw.build(spec, present=present)
        assert (spec.dataset, "teams") not in suites
        assert {table for _, table in suites} == present

    def test_the_tables_that_landed_keep_their_full_suite(self, spec):
        present = set(raw.entity_tables(spec)) - {"teams"}
        everything = raw.build(spec)
        landed = raw.build(spec, present=present)
        for key, suite in landed.items():
            assert expectation_types(suite) == expectation_types(everything[key])

    def test_an_orphan_check_goes_when_its_parent_does(self, spec):
        # The check names the parent table in SQL, so keeping it would trade one
        # missing-table error for another at query time.
        suites = raw.build(spec, present={"issues", "issue_messages"})
        issues = descriptions(suites[(spec.dataset, "issues")])
        assert not any("loaded accounts" in text for text in issues)
        messages = descriptions(suites[(spec.dataset, "issue_messages")])
        assert any("loaded issues" in text for text in messages)

    def test_a_child_check_goes_when_the_child_does(self, spec):
        suites = raw.build(spec, present={"issues", "accounts"})
        assert (spec.dataset, "issue_messages") not in suites

    def test_an_empty_warehouse_builds_nothing_rather_than_erroring(self, spec):
        assert raw.build(spec, present=set()) == {}

    def test_the_required_tables_come_from_the_spec(self, spec):
        # Absence is normally a fact about the source rather than a fault; the
        # spec names the tables without which the rest means nothing.
        assert raw.required_tables(spec) == ("issues",)


class TestACompositeKey:
    """Not every API keys on a single column."""

    @staticmethod
    def composite(tmp_path):
        return write(
            tmp_path,
            "name: two\n"
            "status: reference\n"
            "api:\n"
            "  base_url: https://example.test\n"
            "  auth: {type: bearer, token_env: TWO_TOKEN}\n"
            "resources:\n"
            "  - name: rows\n"
            "    primary_key: [tenant_id, row_id]\n",
            "two")

    def test_each_key_column_is_checked_for_nulls(self, tmp_path):
        spec = self.composite(tmp_path)
        texts = descriptions(raw.build(spec)[(spec.dataset, "rows")])
        assert "rows.tenant_id is never null" in texts
        assert "rows.row_id is never null" in texts

    def test_uniqueness_is_asserted_on_the_tuple_not_the_columns(self, tmp_path):
        """Either column alone repeats legitimately; only the pair is unique."""
        spec = self.composite(tmp_path)
        suite = raw.build(spec)[(spec.dataset, "rows")]
        texts = descriptions(suite)
        assert "rows (tenant_id, row_id) is unique" in texts
        assert "rows.tenant_id is unique" not in texts
        query = next(e.unexpected_rows_query for e in suite
                     if e.description == "rows (tenant_id, row_id) is unique")
        assert "GROUP BY tenant_id, row_id" in query


class TestAMinimalSpec:
    """A spec with no `quality` block at all still gets the identity checks."""

    @pytest.fixture
    def bare(self, tmp_path):
        return write(
            tmp_path,
            "name: bare\n"
            "status: reference\n"
            "api:\n"
            "  base_url: https://example.test\n"
            "  auth: {type: bearer, token_env: BARE_TOKEN}\n"
            "resources:\n"
            "  - name: things\n"
            "    primary_key: id\n",
            "bare")

    def test_it_builds(self, bare):
        assert set(raw.build(bare)) == {("raw_bare", "things")}

    def test_it_gets_identity_checks_and_nothing_it_did_not_declare(self, bare):
        texts = descriptions(raw.build(bare)[("raw_bare", "things")])
        assert "things.id is unique" in texts
        assert "things.id is never null" in texts
        assert not any("within the last" in text for text in texts)
        assert not any("tombstoned" in text for text in texts)

    def test_nothing_is_required_unless_declared(self, bare):
        assert raw.required_tables(bare) == ()


class TestExceptionSummary:
    """A raised expectation and a failed one both arrive as success=False. Only
    exception_info tells them apart, so it has to survive into ops.gx_results."""

    @staticmethod
    def result(exception_info):
        return types.SimpleNamespace(exception_info=exception_info)

    def test_a_clean_failure_reports_no_exception(self):
        assert results._exception_summary(self.result({"raised_exception": False})) is None
        assert results._exception_summary(self.result({})) is None
        assert results._exception_summary(self.result(None)) is None

    def test_a_top_level_exception_is_read(self):
        summary = results._exception_summary(self.result(
            {"raised_exception": True, "exception_message": 'syntax error at or near "child"'}))
        assert summary == 'syntax error at or near "child"'

    def test_an_exception_keyed_by_metric_is_read(self):
        # GX nests one level down when the failure came from computing a metric.
        summary = results._exception_summary(self.result(
            {"MetricConfigurationID(...)": {
                "raised_exception": True, "exception_message": "boom"}}))
        assert summary == "boom"

    def test_a_traceback_without_a_message_falls_back_to_its_last_line(self):
        summary = results._exception_summary(self.result(
            {"raised_exception": True,
             "exception_traceback": "Traceback:\n  File x\nclickhouse_connect.driver.exceptions.DatabaseError: nope\n"}))
        assert summary == "clickhouse_connect.driver.exceptions.DatabaseError: nope"

    def test_a_long_message_is_truncated(self):
        summary = results._exception_summary(self.result(
            {"raised_exception": True, "exception_message": "x" * 900}))
        assert len(summary) == 500 and summary.endswith("…")


class TestConfig:
    def test_connection_string_prefers_the_dlt_env_vars(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "warehouse-db")
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT", "8123")
        assert "@warehouse-db:8123/" in config.connection_string()
        assert config.connection_string().startswith("clickhouse+http://")

    def test_password_special_characters_survive_the_url(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD", "p@ss/word")
        assert "p%40ss%2Fword" in config.connection_string()

    def test_blank_env_var_falls_back_to_the_default(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "")
        assert "@localhost:" in config.connection_string()

    def test_the_connection_does_not_select_a_source_database(self, monkeypatch):
        """Every table name is qualified, so the connection must not depend on a
        particular source being connected to this stack."""
        monkeypatch.delenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", raising=False)
        assert config.connection_string().endswith("/default")

    def test_freshness_hours_defaults_to_what_the_caller_passes(self, monkeypatch):
        monkeypatch.delenv("GX_FRESHNESS_HOURS", raising=False)
        assert config.freshness_hours(default=48) == 48

    def test_freshness_hours_rejects_nonsense(self, monkeypatch):
        monkeypatch.setenv("GX_FRESHNESS_HOURS", "soon")
        with pytest.raises(config.ConfigError):
            config.freshness_hours()
