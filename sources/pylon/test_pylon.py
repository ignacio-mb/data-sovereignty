"""The two behaviours that made Pylon need Python, and the one that made it lie.

Pylon is the reference connector because it is the awkward API: a contract that
only fitted tidy sources would break on the second one. Everything tidy about
it — four directory endpoints, the cursor envelope, the tombstone rules — is in
source.yml and is asserted generically by the contract suite. What is here is
the rest:

  search vs window   two endpoints for one table, chosen by whether there is a
                     cursor yet. Taking the wrong one does not fail; it
                     silently misses every issue opened before the window and
                     updated inside it.
  the glitch page    `has_next_page: true` with no data. Stopping on it
                     truncates the fetch; following it forever hangs the run.
  the worklist       messages have no cross-issue endpoint, so the work comes
                     from the warehouse — including on the run where the
                     messages table does not exist yet.
"""

from __future__ import annotations

from itertools import pairwise

import duckdb
import pytest
import requests_mock
import spec_mock
from harness import load_into_duckdb, reset_run_state
from ingest_runtime import extensions, spec

BASE = "https://api.usepylon.com"


@pytest.fixture
def pylon():
    """The shipped spec, not a fixture copy — so drift fails this test."""
    return spec.load("pylon")


@pytest.fixture
def extension(pylon):
    return extensions.load(pylon)


class FakeClock:
    """`time`, replaced in the extension's namespace only.

    Replacing the module reference rather than patching `time.sleep` itself:
    the backoff here is measured in seconds and dlt is using the real clock in
    the same process.
    """

    def __init__(self, step=0.0):
        self.now = 0.0
        self.step = step
        self.slept = []

    def monotonic(self):
        self.now += self.step
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def run(source_spec, resources):
    with spec_mock.SpecServer(source_spec) as server:
        load_into_duckdb(source_spec, resources=resources)
        return server


class TestTwoEndpointsForOneTable:
    """GET /issues filters on created_at; POST /issues/search on updated_at.

    They answer different questions. The first would miss an issue opened last
    year and updated this morning, which is exactly the row an incremental run
    exists to catch.
    """

    def test_a_first_run_walks_history_through_the_windowed_endpoint(self, pylon):
        server = run(pylon, ["issues"])
        assert server.calls("/issues", method="GET"), "no cursor yet means backfill"
        assert not server.calls("/issues/search", method="POST")

    def test_every_window_stays_inside_the_api_s_hard_cap(self, pylon):
        """30 days is Pylon's limit, not a preference: a wider slice is refused,
        and a backfill that assumed otherwise would fail at the first window."""
        import pendulum

        source_spec = pylon
        server = run(source_spec, ["issues"])
        cap = source_spec.resource("issues").incremental["window"]["max_window_days"]
        windows = []
        for call in server.calls("/issues", method="GET"):
            params = spec_mock.query(call)
            if "created_at[gte]" not in params:
                continue
            window = (pendulum.parse(params["created_at[gte]"][0]),
                      pendulum.parse(params["created_at[lte]"][0]))
            # A window with more than one page repeats its bounds per page.
            if not windows or windows[-1] != window:
                windows.append(window)
        assert windows, "the backfill must bound each request by created_at"
        assert all((end - start).days <= cap for start, end in windows)
        # Contiguous, or the gaps between windows are issues nobody fetches.
        assert all(end == next_start for (_, end), (next_start, _) in pairwise(windows))

    def test_a_resumed_run_searches_on_the_update_time_instead(self, pylon):
        source_spec = pylon
        run(source_spec, ["issues"])
        reset_run_state()
        server = run(source_spec, ["issues"])

        searches = server.calls("/issues/search", method="POST")
        assert searches, "with a cursor the run must use the endpoint that filters on it"
        assert not server.calls("/issues", method="GET")
        body = searches[0].json()
        assert body["filter"]["field"] == "updated_at"
        assert body["filter"]["operator"] == "time_is_after"

    def test_the_search_cursor_travels_in_the_body(self, pylon, extension):
        """`cursor_in: body` is declared because this endpoint alone works that
        way — every other one takes it in the query string.

        Driven from a bound old enough that the search really has to page: with
        a steady-state cursor it answers in one page, which would let a cursor
        sent in the wrong place pass unnoticed.
        """
        import pendulum

        resource = pylon.resource("issues")
        with spec_mock.SpecServer(pylon) as server:
            session = extension.session_for(pylon)
            records = list(extension._search(
                pylon, session, resource, resource.incremental["search"],
                "updated_at", pendulum.parse("2019-01-01T00:00:00Z"), lambda row: row))

        assert len(records) > 1, "the whole history should match a bound that old"
        paged = [call for call in server.calls("/issues/search", method="POST")
                 if call.json().get("cursor")]
        assert paged, "more than one page means the cursor was followed"
        assert all("cursor" not in spec_mock.query(call) for call in paged)


class TestThePageThatLies:
    """A response advertising another page while carrying no data."""

    @staticmethod
    def page(records, cursor):
        return {"data": records,
                "pagination": {"has_next_page": bool(cursor), "cursor": cursor}}

    def test_retrying_the_same_cursor_clears_it(self, pylon, extension, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(extension, "time", clock)
        with requests_mock.Mocker() as mock:
            mock.get(f"{BASE}/issues", [
                {"json": self.page([], "c1")},           # the glitch
                {"json": self.page([{"id": "iss-1"}], None)},
            ])
            session = extension.session_for(pylon)
            records = list(extension._paged(pylon, session, "/issues"))

        assert [record["id"] for record in records] == ["iss-1"]
        assert clock.slept == [2], "one retry, backed off once"

    def test_an_endless_glitch_raises_rather_than_reporting_a_short_read(
            self, pylon, extension, monkeypatch):
        """Returning what was collected so far would be a partial fetch
        reported as a complete one — and with --mark-deleted behind it, that is
        how a warehouse gets tombstoned."""
        clock = FakeClock()
        monkeypatch.setattr(extension, "time", clock)
        with requests_mock.Mocker() as mock:
            mock.get(f"{BASE}/issues", json=self.page([], "c1"))
            session = extension.session_for(pylon)
            with pytest.raises(RuntimeError, match="consecutive empty pages"):
                list(extension._paged(pylon, session, "/issues"))

        assert clock.slept == [2, 4, 6], "backoff grows, then it gives up"


class TestOneGoneTicketDoesNotFailTheRun:
    def test_a_skipped_status_costs_only_that_issue(self, pylon, extension, monkeypatch):
        """400/404/410 mean the issue is scrubbed or deleted. Failing the whole
        run over one is how a connector becomes something people stop trusting
        to be red."""
        monkeypatch.setattr(
            extension, "_pending_issue_ids",
            lambda parent, child: ["iss-001", "iss-gone", "iss-002"])

        with spec_mock.SpecServer(pylon):
            source = extension.build_issue_messages(pylon, pylon.resource("issue_messages"))
            records = list(source.resources["issue_messages"])

        assert {record["issue_id"] for record in records} == {"iss-001", "iss-002"}

    def test_a_status_the_spec_did_not_list_still_fails(self, pylon, extension):
        """Tolerating everything would turn an outage into an empty table."""
        resource = pylon.resource("issue_messages")
        with requests_mock.Mocker() as mock:
            mock.get(f"{BASE}/issues/iss-001/messages", status_code=500)
            session = extension.session_for(pylon)
            with pytest.raises(RuntimeError, match="HTTP 500"):
                list(extension._messages_for(
                    pylon, session, resource, resource.incremental["endpoint"],
                    "iss-001", set(resource.incremental["skip_statuses"])))


class TestTheBudgetStopsCleanly:
    def test_a_spent_budget_ends_the_run_rather_than_being_killed(
            self, pylon, extension, monkeypatch):
        """The watermark is derived from what actually landed, so the next run
        resumes exactly here. There is no state to keep and nothing to
        reconcile — which is what makes stopping safe."""
        monkeypatch.setattr(
            extension, "_pending_issue_ids",
            lambda parent, child: ["iss-001", "iss-002", "iss-004"])
        budget = pylon.resource("issue_messages").incremental["budget_minutes"] * 60
        # Each reading of the clock jumps most of the budget, so the deadline
        # passes after the first issue.
        monkeypatch.setattr(extension, "time", FakeClock(step=budget * 0.9))

        with spec_mock.SpecServer(pylon):
            source = extension.build_issue_messages(pylon, pylon.resource("issue_messages"))
            records = list(source.resources["issue_messages"])

        assert {record["issue_id"] for record in records} == {"iss-001"}


class TestTheWorklistIsAWarehouseQuery:
    """There is no cross-issue messages endpoint, so the work comes from SQL.

    Run against a real engine rather than string-matched: the query has been
    wrong in ways only a database notices — a correlated subquery ClickHouse
    rejects, an interval it will not parse.
    """

    @staticmethod
    def warehouse():
        connection = duckdb.connect()
        connection.execute(
            "CREATE TABLE issues (id VARCHAR, latest_message_time TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO issues VALUES "
            "('iss-001', '2026-05-06 14:03:00+00'), "   # caught up
            "('iss-002', '2026-06-12 11:30:00+00'), "   # has newer traffic
            "('iss-003', NULL)")                        # never had a message
        connection.execute(
            "CREATE TABLE issue_messages (id VARCHAR, issue_id VARCHAR, timestamp TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO issue_messages VALUES "
            "('msg-002', 'iss-001', '2026-05-06 14:03:00+00'), "
            "('msg-003', 'iss-002', '2026-06-11 08:00:00+00')")
        return connection

    @staticmethod
    def captured(extension, monkeypatch, rows):
        """The SQL the extension would have run, and what it got back."""
        seen = {}

        def fake_warehouse_rows(build_query):
            sql = build_query(lambda table: table)
            seen.setdefault("sql", []).append(sql)
            return rows(sql)

        monkeypatch.setattr(extension, "warehouse_rows", fake_warehouse_rows)
        return seen

    def test_only_issues_with_newer_traffic_are_pending(self, extension, monkeypatch):
        connection = self.warehouse()
        seen = self.captured(extension, monkeypatch,
                             lambda sql: connection.execute(sql).fetchall())

        pending = extension._pending_issue_ids("issues", "issue_messages")

        assert pending == ["iss-002"], (
            "iss-001 is caught up and iss-003 has never had a message; asking "
            "for either is a request per hour that returns nothing")
        assert any("LEFT JOIN" in sql for sql in seen["sql"])

    def test_the_first_run_asks_for_every_issue_with_traffic(self, extension, monkeypatch):
        """dlt creates a table when a resource yields its first row, so before
        that run the messages table does not exist — and a query naming it
        fails as a whole, which reads as "nothing is pending". A single query
        would therefore decide there was no work, yield nothing, and leave the
        table still absent. Forever."""
        connection = self.warehouse()
        connection.execute("DROP TABLE issue_messages")

        def rows(sql):
            if "issue_messages" in sql:
                # What warehouse_rows does with a missing relation.
                return []
            return connection.execute(sql).fetchall()

        self.captured(extension, monkeypatch, rows)

        assert extension._pending_issue_ids("issues", "issue_messages") == [
            "iss-001", "iss-002"]
