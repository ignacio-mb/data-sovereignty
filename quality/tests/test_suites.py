"""The suite builders must construct valid GX objects without touching a database."""

import pytest
import yaml
from pylon_quality import config
from pylon_quality.suites import marts, raw_pylon


def expectation_types(expectations):
    return [expectation.expectation_type for expectation in expectations]


def descriptions(expectations):
    return [expectation.description or "" for expectation in expectations]


def columns_required_present(expectations):
    return [
        expectation.column
        for expectation in expectations
        if expectation.expectation_type == "expect_column_values_to_not_be_null"
    ]


class TestRawSuites:
    def test_covers_every_ingested_table(self):
        suites = raw_pylon.build()
        assert {table for _, table in suites} == set(raw_pylon.ENTITY_TABLES)
        assert all(schema == config.RAW_SCHEMA for schema, _ in suites)

    def test_every_table_asserts_its_merge_key(self):
        for (_, table), suite in raw_pylon.build().items():
            types = expectation_types(suite)
            assert "expect_column_values_to_be_unique" in types, table
            assert "expect_column_values_to_not_be_null" in types, table

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
        assert freshness == ["issues.updated_at is within the last 6h"]

    def test_children_are_checked_against_their_parents(self):
        suites = raw_pylon.build()
        messages = descriptions(suites[(config.RAW_SCHEMA, "issue_messages")])
        assert any("belongs to a loaded issue" in text for text in messages)
        issues = descriptions(suites[(config.RAW_SCHEMA, "issues")])
        assert any("resolves to a loaded account" in text for text in issues)


MANIFEST = {
    "schema": "analytics",
    "transforms": [
        {"name": "fact_issue", "sql": "30_fact_issue.sql", "grain": ["issue_id"],
         "not_null": ["issue_id", "account_id"]},
        {"name": "metrics_support_daily", "sql": "40_metrics.sql",
         "grain": ["day", "team_id"],
         "reconciliation": [
             {"description": "splits sum to the total",
              "query": "SELECT 1 FROM {batch} WHERE opened <> new_issues + reopened_issues"},
         ]},
        {"name": "dim_date", "sql": "20_dim_date.sql"},
    ],
}


class TestMartSuites:
    def test_single_column_grain_uses_the_native_uniqueness_check(self):
        suite = marts.build(MANIFEST)[("analytics", "fact_issue")]
        assert "expect_column_values_to_be_unique" in expectation_types(suite)

    def test_compound_grain_is_checked_as_a_whole(self):
        suite = marts.build(MANIFEST)[("analytics", "metrics_support_daily")]
        # A per-column uniqueness check would be wrong here: day alone repeats.
        assert "expect_column_values_to_be_unique" not in expectation_types(suite)
        assert any("grain (day, team_id) is unique" in text for text in descriptions(suite))

    def test_grain_columns_are_also_required_to_be_present(self):
        suite = marts.build(MANIFEST)[("analytics", "metrics_support_daily")]
        not_null = columns_required_present(suite)
        assert set(not_null) == {"day", "team_id"}

    def test_not_null_does_not_duplicate_grain_columns(self):
        suite = marts.build(MANIFEST)[("analytics", "fact_issue")]
        not_null = columns_required_present(suite)
        assert sorted(not_null) == ["account_id", "issue_id"]

    def test_reconciliation_identities_become_expectations(self):
        suite = marts.build(MANIFEST)[("analytics", "metrics_support_daily")]
        assert any("splits sum to the total" in text for text in descriptions(suite))

    def test_a_transform_without_a_grain_still_gets_a_suite(self):
        suite = marts.build(MANIFEST)[("analytics", "dim_date")]
        assert "expect_table_row_count_to_be_between" in expectation_types(suite)

    def test_missing_manifest_is_not_an_error(self, tmp_path):
        manifest = marts.load_manifest(tmp_path / "absent.yml")
        assert manifest["transforms"] == []
        assert marts.build(manifest) == {}

    def test_manifest_round_trips_from_disk(self, tmp_path):
        path = tmp_path / "manifest.yml"
        path.write_text(yaml.safe_dump(MANIFEST))
        assert marts.build(marts.load_manifest(path)).keys() == marts.build(MANIFEST).keys()


class TestConfig:
    def test_connection_string_prefers_the_dlt_env_vars(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__HOST", "warehouse-db")
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__PORT", "5432")
        assert "@warehouse-db:5432/" in config.connection_string()

    def test_password_special_characters_survive_the_url(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD", "p@ss/word")
        assert "p%40ss%2Fword" in config.connection_string()

    def test_blank_env_var_falls_back_to_the_default(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__POSTGRES__CREDENTIALS__HOST", "")
        assert "@localhost:" in config.connection_string()

    def test_freshness_hours_rejects_nonsense(self, monkeypatch):
        monkeypatch.setenv("GX_FRESHNESS_HOURS", "soon")
        with pytest.raises(config.ConfigError):
            config.freshness_hours()
