"""The suite builders must construct valid GX objects without touching a database."""

import types

import pytest
from quality_runtime import config, results
from quality_runtime.suites import raw_pylon


def expectation_types(expectations):
    return [expectation.expectation_type for expectation in expectations]


def descriptions(expectations):
    return [expectation.description or "" for expectation in expectations]


class TestRawSuites:
    def test_covers_every_ingested_table(self):
        suites = raw_pylon.build()
        assert {table for _, table in suites} == set(raw_pylon.ENTITY_TABLES)
        assert all(schema == config.RAW_SCHEMA for schema, _ in suites)

    def test_every_table_asserts_its_merge_key(self):
        for (_, table), suite in raw_pylon.build().items():
            texts = descriptions(suite)
            assert f"{table}.id is unique" in texts, table
            assert f"{table}.id is never null" in texts, table

    def test_only_soft_deleted_tables_get_the_tombstone_check(self):
        suites = raw_pylon.build()
        tombstone = "is tombstoned"
        for (_, table), suite in suites.items():
            has_check = any(tombstone in text for text in descriptions(suite))
            assert has_check == (table in raw_pylon.SOFT_DELETE_TABLES), table

    def test_issues_carries_the_freshness_signal(self, monkeypatch):
        monkeypatch.setenv("GX_FRESHNESS_HOURS", "6")
        suite = raw_pylon.build()[(config.RAW_SCHEMA, "issues")]
        freshness = [text for text in descriptions(suite) if "within the last" in text]
        assert len(freshness) == 1
        assert freshness[0].startswith("issues.updated_at is within the last 6h")

    def test_freshness_is_advisory_and_everything_else_is_not(self):
        """A quiet tenant must not redden the DAG, but nothing else may opt out.

        Freshness measures tenant activity, not pipeline health, so it warns.
        Making it advisory is only safe if advisory stays the exception.
        """
        advisory, gating = [], []
        for suite in raw_pylon.build().values():
            for expectation in suite:
                meta = expectation.meta or {}
                target = advisory if meta.get("severity") == "warn" else gating
                target.append(expectation.description or expectation.expectation_type)

        assert advisory, "freshness should be marked advisory"
        assert all("within the last" in text for text in advisory), advisory
        assert not any("within the last" in text for text in gating)

    def test_children_are_checked_against_their_parents(self):
        suites = raw_pylon.build()
        messages = descriptions(suites[(config.RAW_SCHEMA, "issue_messages")])
        assert any("belongs to a loaded issue" in text for text in messages)
        issues = descriptions(suites[(config.RAW_SCHEMA, "issues")])
        assert any("resolves to a loaded account" in text for text in issues)


class TestRawSuitesAgainstWhatLanded:
    """A resource that never yielded a row has no table: dlt creates it on first
    write. Validating the tables that exist beats failing all six."""

    ALL_BUT_TEAMS = set(raw_pylon.ENTITY_TABLES) - {"teams"}

    def test_a_table_that_never_landed_is_left_out(self):
        suites = raw_pylon.build(present=self.ALL_BUT_TEAMS)
        assert (config.RAW_SCHEMA, "teams") not in suites
        assert {table for _, table in suites} == self.ALL_BUT_TEAMS

    def test_the_tables_that_landed_keep_their_full_suite(self):
        everything = raw_pylon.build()
        landed = raw_pylon.build(present=self.ALL_BUT_TEAMS)
        for key, suite in landed.items():
            assert expectation_types(suite) == expectation_types(everything[key])

    def test_an_orphan_check_goes_when_its_parent_does(self):
        # The check reads raw_pylon.accounts by name, so keeping it would trade
        # one missing-table error for another at query time.
        suites = raw_pylon.build(present={"issues", "issue_messages"})
        issues = descriptions(suites[(config.RAW_SCHEMA, "issues")])
        assert not any("resolves to a loaded account" in text for text in issues)
        messages = descriptions(suites[(config.RAW_SCHEMA, "issue_messages")])
        assert any("belongs to a loaded issue" in text for text in messages)

    def test_messages_lose_their_parent_check_without_issues(self):
        suites = raw_pylon.build(present={"issue_messages"})
        messages = descriptions(suites[(config.RAW_SCHEMA, "issue_messages")])
        assert not any("belongs to a loaded issue" in text for text in messages)

    def test_an_empty_warehouse_builds_nothing_rather_than_erroring(self):
        assert raw_pylon.build(present=set()) == {}

    def test_issues_is_the_one_table_whose_absence_is_a_failure(self):
        # Everything else in raw_pylon is meaningless without it, so it must not
        # be skippable the way an empty directory resource is.
        assert raw_pylon.REQUIRED_TABLES == ("issues",)


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

    def test_freshness_hours_rejects_nonsense(self, monkeypatch):
        monkeypatch.setenv("GX_FRESHNESS_HOURS", "soon")
        with pytest.raises(config.ConfigError):
            config.freshness_hours()
