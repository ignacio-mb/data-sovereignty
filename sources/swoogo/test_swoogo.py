"""What is true of Swoogo and of nothing else.

Everything generic — the rows land, the key is unique, the timeouts are set,
the page size reaches the wire — is asserted for every connector in
pipeline/tests/test_connector_contract.py. What is left here is the fan-out,
which exists because twelve of Swoogo's fourteen endpoints are scoped to one
event and the declarative config has no way to say "call this once per row of
that other resource".

Each test below is a way this connector can under-fetch while still exiting
zero: visit one event instead of all of them, read one page instead of all of
them, invent a lower bound on a first load, or send the bound in a format the
API accepts and matches nothing against.
"""

from __future__ import annotations

import re

import spec_mock
from harness import connect, load_into_duckdb, reset_run_state
from ingest_runtime import extensions, spec

# Swoogo parses this and not ISO 8601. A `T` in the search filter is accepted
# and matches nothing, which is why the format is asserted rather than trusted.
SEARCH_GRAMMAR = re.compile(r"^updated_at>=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def swoogo():
    """The shipped spec, not a fixture copy — so drift fails this test."""
    return spec.load("swoogo")


def run(source_spec, resources):
    """One run against the connector's own fixtures. Returns the server."""
    with spec_mock.SpecServer(source_spec) as server:
        load_into_duckdb(source_spec, resources=resources)
        return server


def registrant_calls(server):
    return server.calls("/registrants", method="GET")


def test_the_extension_is_loaded_from_beside_the_spec():
    """`extensions: true` names no module, so the file's location IS the link."""
    source_spec = swoogo()
    extension = extensions.load(source_spec)
    assert extension is not None
    assert extension.__file__ == str(source_spec.extension_path)
    # One function serves all twelve per-event endpoints; a thirteenth needs no
    # Python at all.
    assert hasattr(extension, "build_resource")
    assert not [name for name in dir(extension) if name.startswith("build_registrants")]


def test_the_fan_out_visits_every_event_and_every_page(tmp_path):
    source_spec = swoogo()
    run(source_spec, ["events", "registrants"])

    connection = connect(source_spec, tmp_path)
    events = connection.execute("SELECT id FROM raw_swoogo.events ORDER BY id").fetchall()
    assert [row[0] for row in events] == [11, 22], "both pages of the parent must land"

    rows = connection.execute(
        "SELECT id, event_id FROM raw_swoogo.registrants ORDER BY id").fetchall()
    # Four rows means both events were visited AND both pages read within each.
    # A fan-out that stops after the first event gives two; one that ignores
    # paging also gives two — different bugs, same wrong count, so assert ids.
    assert [row[0] for row in rows] == [101, 102, 201, 202]
    assert {row[1] for row in rows} == {11, 22}


def test_the_worklist_is_read_once_and_asks_only_for_ids():
    """The fan-out's own read of /events is a worklist, not a load.

    Each of the twelve per-event resources needs the same list, and re-reading
    it for each one spent 120 credits of a 2000-credit budget restating a fact
    that cannot change mid-run.
    """
    source_spec = swoogo()
    server = run(source_spec, ["registrants"])
    worklist = [call for call in server.calls("/events", method="GET")
                if spec_mock.query(call).get("fields") == ["id"]]
    assert worklist, "the fan-out must read /events to know which events exist"
    # Two events at one a page: the worklist itself has to be paged through, or
    # every event after the first is invisible to the fan-out.
    assert len(worklist) == 2


def test_the_first_run_sends_no_search_filter():
    """With no cursor there is no lower bound, and inventing one would skip
    history on the very load that is supposed to establish it."""
    server = run(swoogo(), ["registrants"])
    filtered = [call for call in registrant_calls(server) if "search" in spec_mock.query(call)]
    assert not filtered, [call.url for call in filtered]


def test_the_second_run_bounds_every_event_in_swoogo_s_own_grammar():
    """The bound is what makes an hourly fan-out affordable.

    Applied server-side through `search=updated_at>=…`, so an unchanged event
    answers with one empty page instead of its whole registrant list — and in
    Swoogo's timestamp format, because an ISO string with a `T` is accepted and
    silently matches nothing.
    """
    source_spec = swoogo()
    run(source_spec, ["registrants"])
    # A run is a fresh process under Airflow; the cursor persists, the caches
    # do not.
    reset_run_state()
    server = run(source_spec, ["registrants"])

    calls = registrant_calls(server)
    searches = {spec_mock.query(call)["search"][0] for call in calls if "search" in
                spec_mock.query(call)}
    assert searches, "the second run must bound the fetch by the cursor it now has"
    assert all(SEARCH_GRAMMAR.match(value) for value in searches), searches
    # Every event is still visited, not just the ones whose own updated_at
    # moved: Swoogo does not touch an event when somebody registers for it, so
    # a "parents changed since last run" worklist would skip exactly the events
    # whose registrant data did move.
    scoped = {spec_mock.query(call)["event_id"][0] for call in calls}
    assert scoped == {"11", "22"}, scoped


def test_the_cursor_column_survives_the_sparse_projection(tmp_path):
    """`fields` must reach the wire, or incremental has nothing to compare.

    The mock returns `id, name` when `fields` is absent, exactly as Swoogo
    does. An updated_at that is present and non-null therefore proves the
    spec's field list was actually sent rather than merely written down.
    """
    source_spec = swoogo()
    run(source_spec, ["registrants"])

    connection = connect(source_spec, tmp_path)
    stamps = connection.execute(
        "SELECT updated_at FROM raw_swoogo.registrants ORDER BY id").fetchall()
    assert stamps and all(row[0] is not None for row in stamps), stamps
