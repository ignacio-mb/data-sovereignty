"""The incremental cursor must advance even when a scan returns nothing.

Parking the cursor at its old value after an empty scan means re-scanning the
same span every run — on a quiet or freshly provisioned tenant that is a
re-window from BACKFILL_START, hourly, forever.
"""

import pendulum
import pytest
from click.testing import CliRunner

from ingest_runtime.cli import cli

INCREMENTAL_ARGS = ["run", "--api-key", "test-key", "--destination", "duckdb",
                    "--resources", "issues"]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("ingest_runtime.ingest.pacing.time.sleep", lambda seconds: None)


def run_cli(args):
    result = CliRunner().invoke(cli, args, catch_exceptions=True)
    if result.exit_code != 0:
        raise AssertionError(f"CLI failed ({result.exit_code}):\n{result.output}") from result.exception
    return result


def search_windows(pylon_api):
    return [r.json()["filter"]["values"] for r in pylon_api.request_history
            if r.path == "/issues/search"]


def test_empty_scan_advances_cursor_to_the_horizon(requests_mock, isolated_dlt):
    """A tenant with no issues at all must not re-scan history every run."""
    requests_mock.post(
        "https://api.usepylon.com/issues/search",
        json={"data": [], "pagination": {"cursor": None, "has_next_page": False}},
    )

    run_cli(INCREMENTAL_ARGS)
    first_run = search_windows(requests_mock)
    # Cold start: windows the whole history from BACKFILL_START minus the lookback.
    assert first_run[0][0] == "2018-12-31T23:00:00Z"
    assert len(first_run) > 1, "expected the 2019->now history to be chunked into 30-day windows"

    run_cli(INCREMENTAL_ARGS)
    second_run = search_windows(requests_mock)[len(first_run):]

    # Second run starts from the horizon the first run cleared, not from 2019.
    assert len(second_run) == 1, f"expected a single window, got {len(second_run)}"
    assert pendulum.parse(second_run[0][0]) > pendulum.parse("2026-01-01T00:00:00Z")


def test_populated_scan_advances_to_max_updated_at_not_the_horizon(pylon_api, isolated_dlt):
    """When records do come back, the cursor tracks the data, not the clock."""
    run_cli(INCREMENTAL_ARGS)
    run_cli(INCREMENTAL_ARGS)

    windows = search_windows(pylon_api)
    # Fixture's newest updated_at is 2026-07-04T10:00:00Z; minus the 1h lookback.
    assert windows[-1][0] == "2026-07-04T09:00:00Z"
