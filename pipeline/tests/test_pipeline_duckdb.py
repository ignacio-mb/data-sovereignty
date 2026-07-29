"""End-to-end pipeline tests against duckdb, driven through the real CLI."""

import duckdb
import pendulum
import pytest
from click.testing import CliRunner

from ingest_runtime.cli import cli
from tests.conftest import DIRECTORY

WINDOW_ARGS = [
    "run", "--api-key", "test-key", "--destination", "duckdb",
    "--start", "2026-06-01", "--end", "2026-08-01",
]


def run_cli(args):
    result = CliRunner().invoke(cli, args, catch_exceptions=True)
    if result.exit_code != 0:
        raise AssertionError(f"CLI failed ({result.exit_code}):\n{result.output}") from result.exception
    return result


def db():
    # build_pipeline namespaces every non-production destination: pylon_duckdb.duckdb
    return duckdb.connect("pylon_duckdb.duckdb", read_only=True)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("ingest_runtime.ingest.pacing.time.sleep", lambda seconds: None)


def test_window_ingest_loads_all_tables(pylon_api, isolated_dlt):
    result = run_cli(WINDOW_ARGS + ["--sample", "1"])

    with db() as con:
        assert con.sql("select count(*) from raw_pylon.issues").fetchone()[0] == 3
        assert con.sql("select count(*) from raw_pylon.issue_messages").fetchone()[0] == 4
        assert con.sql("select count(*) from raw_pylon.accounts").fetchone()[0] == 2
        assert con.sql("select count(*) from raw_pylon.users").fetchone()[0] == 1
        # promoted scalars + flat JSON columns landed
        row = con.sql(
            "select account_id, assignee_email, custom_fields, _deleted "
            "from raw_pylon.issues where id = 'iss_1'"
        ).fetchone()
        assert row[0] == "acc_for_iss_1"
        assert row[1] == "assignee@example.com"
        assert '"plan"' in row[2]
        assert row[3] is False
        # message text stripped from HTML without escaping artifacts
        text = con.sql(
            "select message_text from raw_pylon.issue_messages where id = 'msg_1a'"
        ).fetchone()[0]
        assert text == 'Hello "there"'

    assert "sample [issues] #1" in result.output
    assert "RUN SUMMARY" in result.output


def test_rerun_merges_instead_of_duplicating(pylon_api, isolated_dlt):
    run_cli(WINDOW_ARGS)
    run_cli(WINDOW_ARGS)

    with db() as con:
        assert con.sql("select count(*) from raw_pylon.issues").fetchone()[0] == 3
        dupes = con.sql(
            "select id from raw_pylon.issues group by id having count(*) > 1"
        ).fetchall()
        assert dupes == []
        assert con.sql("select count(*) from raw_pylon.issue_messages").fetchone()[0] == 4


def test_incremental_uses_and_advances_cursor(pylon_api, isolated_dlt):
    run_cli(["run", "--api-key", "test-key", "--destination", "duckdb",
             "--resources", "issues"])

    search_bodies = [r.json() for r in pylon_api.request_history if r.path == "/issues/search"]
    # first run: windowed time_range search starting at BACKFILL_START minus the 3600s lookback
    assert search_bodies[0]["filter"]["operator"] == "time_range"
    assert search_bodies[0]["filter"]["field"] == "updated_at"
    assert search_bodies[0]["filter"]["values"][0] == "2018-12-31T23:00:00Z"
    # every window spans <= 30 days
    for body in search_bodies:
        lo, hi = (pendulum.parse(v) for v in body["filter"]["values"])
        assert (hi - lo).in_days() <= 30

    run_cli(["run", "--api-key", "test-key", "--destination", "duckdb",
             "--resources", "issues"])
    search_bodies = [r.json() for r in pylon_api.request_history if r.path == "/issues/search"]
    # second run: single window from max updated_at seen (2026-07-04T10:00:00Z) minus 1h lookback
    assert search_bodies[-1]["filter"]["values"][0] == "2026-07-04T09:00:00Z"

    with db() as con:
        assert con.sql("select count(*) from raw_pylon.issues").fetchone()[0] == 3


def test_messages_watermark_skips_up_to_date_issues(pylon_api, isolated_dlt):
    run_cli(WINDOW_ARGS)
    message_requests_first = len(
        [r for r in pylon_api.request_history if r.path.endswith("/messages")]
    )
    assert message_requests_first == 3  # one per issue

    run_cli(WINDOW_ARGS)
    message_requests_total = len(
        [r for r in pylon_api.request_history if r.path.endswith("/messages")]
    )
    # nothing changed upstream -> watermark says every issue is up to date
    assert message_requests_total == message_requests_first


def test_mark_deleted_flags_vanished_directory_rows(pylon_api, isolated_dlt, monkeypatch):
    run_cli(WINDOW_ARGS)

    # acc_2 disappears from the API; next run with --mark-deleted must flag it
    monkeypatch.setitem(DIRECTORY, "accounts", DIRECTORY["accounts"][:1])
    run_cli(WINDOW_ARGS + ["--mark-deleted"])

    with db() as con:
        deleted = dict(con.sql("select id, _deleted from raw_pylon.accounts").fetchall())
        assert deleted == {"acc_1": False, "acc_2": True}


def test_incremental_run_never_soft_deletes_issues(pylon_api, isolated_dlt):
    run_cli(WINDOW_ARGS)
    # incremental only re-fetches recently-updated issues; --mark-deleted must
    # not tombstone the quiet ones
    run_cli(["run", "--api-key", "test-key", "--destination", "duckdb", "--mark-deleted"])

    with db() as con:
        flagged = con.sql("select count(*) from raw_pylon.issues where _deleted").fetchone()[0]
        assert flagged == 0


def test_full_history_window_soft_deletes_vanished_issues(pylon_api, isolated_dlt, monkeypatch):
    import tests.conftest as fixtures

    # a full-history window (2019 -> now) is the reconcile that IS allowed to
    # tombstone issues absent from the fetch
    full = ["run", "--api-key", "test-key", "--destination", "duckdb", "--start", "2019-01-01"]
    run_cli(full)
    monkeypatch.setattr(fixtures, "ISSUES", fixtures.ISSUES[:2])  # iss_3 vanishes
    run_cli(full + ["--mark-deleted"])

    with db() as con:
        deleted = dict(con.sql("select id, _deleted from raw_pylon.issues").fetchall())
        assert deleted["iss_3"] is True
        assert deleted["iss_1"] is False


def test_end_before_start_is_rejected(pylon_api, isolated_dlt):
    result = CliRunner().invoke(cli, [
        "run", "--api-key", "test-key", "--destination", "duckdb",
        "--start", "2026-06-01", "--end", "2026-05-01",
    ])
    assert result.exit_code != 0
    assert "must be after" in result.output


def test_mark_deleted_survives_empty_directory_table(pylon_api, isolated_dlt, monkeypatch):
    import tests.conftest as fixtures

    monkeypatch.setitem(fixtures.DIRECTORY, "teams", [])  # teams table never gets created
    result = run_cli(WINDOW_ARGS + ["--mark-deleted"])
    assert "RUN SUMMARY" in result.output  # did not crash at the soft-delete step

    with db() as con:
        assert con.sql("select count(*) from raw_pylon.accounts").fetchone()[0] == 2


def test_pending_package_recovery_does_not_skip_extract_or_tombstone(pylon_api, isolated_dlt):
    # Simulate a previous crash after normalize but before load: a pending
    # package sits in the duckdb pipeline's working dir.
    from ingest_runtime.ingest.client import PylonClient
    from ingest_runtime.ingest.pacing import EndpointPacer
    from ingest_runtime.ingest.settings import RATE_LIMITS
    from ingest_runtime.ingest.source import pylon_source
    from ingest_runtime.warehouse import build_pipeline

    client = PylonClient("test-key", pacer=EndpointPacer(RATE_LIMITS, sleeper=lambda s: None))
    crashed = build_pipeline(destination="duckdb", dataset_name="raw_pylon")
    crashed.extract(pylon_source(client=client, selected=["accounts"]))
    crashed.normalize()  # normalized but NOT loaded -> pending package

    # A real run with --mark-deleted must recover the package, still fetch this
    # run's data, and not tombstone anything from the stale load id.
    run_cli(WINDOW_ARGS + ["--mark-deleted"])

    with db() as con:
        assert con.sql("select count(*) from raw_pylon.issues").fetchone()[0] == 3
        flagged = con.sql("select count(*) from raw_pylon.accounts where _deleted").fetchone()[0]
        assert flagged == 0
