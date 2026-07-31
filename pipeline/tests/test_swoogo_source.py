"""The Swoogo connector, driven end to end into duckdb against a mocked API.

Swoogo is the first source here whose resources are mostly NOT declarative, so
this covers the seams the Pylon-shaped test cannot reach: a token that has to be
minted before anything can be fetched, and a child endpoint that only exists
per-parent.

The mock implements Swoogo's actual behaviours rather than returning canned
bodies, because each of them is a way this connector can silently under-fetch:

  * two pages per collection, so the page-number paginator is exercised instead
    of the first response happening to be the whole dataset;
  * two events, so a fan-out that quietly stops after the first parent fails;
  * `_meta.pageCount`, which is what tells the walker to stop;
  * a sparse projection unless `fields` is sent — the API's real behaviour, and
    the one that would otherwise cost us the cursor column without an error.
"""

from unittest.mock import patch

import duckdb
import pytest
import requests
import requests_mock as rm_module

from ingest_runtime import runtime, spec
from ingest_runtime.sources import swoogo as swoogo_ext
from ingest_runtime.warehouse import build_pipeline

BASE = "https://api.swoogo.com/api/v1"
TOKEN_URL = f"{BASE}/oauth2/token"

EVENTS = [
    {"id": 11, "name": "Spring Summit", "status": "live",
     "start_date": "2026-04-01", "created_at": "2026-01-01 09:00:00",
     "updated_at": "2026-01-05 09:00:00"},
    {"id": 22, "name": "Autumn Forum", "status": "live",
     "start_date": "2026-10-01", "created_at": "2026-02-01 09:00:00",
     "updated_at": "2026-02-05 09:00:00"},
]

# Two per event, so each event needs two pages at page size 1.
REGISTRANTS = {
    11: [
        {"id": 101, "event_id": 11, "email": "a@test", "first_name": "Ada",
         "registration_status": "confirmed", "individual_gross": "100.00",
         "contact_id": 9001, "created_at": "2026-01-02 10:00:00",
         "updated_at": "2026-01-06 10:00:00"},
        {"id": 102, "event_id": 11, "email": "b@test", "first_name": "Bo",
         "registration_status": "cancelled", "individual_gross": "0.00",
         "contact_id": 9002, "created_at": "2026-01-03 10:00:00",
         "updated_at": "2026-01-07 10:00:00"},
    ],
    22: [
        {"id": 201, "event_id": 22, "email": "c@test", "first_name": "Cy",
         "registration_status": "confirmed", "individual_gross": "250.00",
         "contact_id": 9003, "created_at": "2026-02-02 10:00:00",
         "updated_at": "2026-02-06 10:00:00"},
        {"id": 202, "event_id": 22, "email": "d@test", "first_name": "Di",
         "registration_status": "confirmed", "individual_gross": "250.00",
         "contact_id": 9004, "created_at": "2026-02-03 10:00:00",
         "updated_at": "2026-02-07 10:00:00"},
    ],
}


def _page(rows, request, per_page):
    """Swoogo's envelope, paginated the way the real API paginates."""
    page = int(request.qs.get("page", ["1"])[0])
    start = (page - 1) * per_page
    window = rows[start:start + per_page]
    page_count = max(1, -(-len(rows) // per_page))
    return {"items": window,
            "_meta": {"totalCount": len(rows), "pageCount": page_count,
                      "currentPage": page, "perPage": per_page}}


def _project(rows, request):
    """Honour `fields`, and return the sparse default when it is missing.

    This is the behaviour that makes `fields` load-bearing: without it the real
    API hands back `id, name` and the cursor column simply is not there.
    """
    requested = request.qs.get("fields")
    if not requested:
        return [{"id": row["id"]} for row in rows]
    wanted = set(requested[0].split(","))
    return [{k: v for k, v in row.items() if k in wanted} for row in rows]


@pytest.fixture
def swoogo_spec():
    """The real shipped spec, not a fixture copy — so drift fails this test."""
    return spec.load("swoogo")


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("SWOOGO_ENCODED_CREDENTIALS", "ZW5jb2RlZDpjcmVkcw==")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    monkeypatch.chdir(tmp_path)
    # The token source is cached for the life of the process, which is one run
    # in Airflow but the whole session under pytest.
    runtime._TOKEN_SOURCES.clear()
    swoogo_ext._EVENT_IDS.clear()


def wire(mock):
    """Token endpoint, the events collection, and per-event registrants."""
    mock.post(TOKEN_URL, json={"access_token": "tok-abc",
                               "token_type": "Bearer", "expires_in": 1800})

    def events(request, context):
        # Page size 1 so two events need two pages.
        return _page(_project(EVENTS, request), request, per_page=1)

    def registrants(request, context):
        event_id = int(request.qs["event_id"][0])
        rows = REGISTRANTS[event_id]
        return _page(_project(rows, request), request, per_page=1)

    mock.get(f"{BASE}/events", json=events)
    mock.get(f"{BASE}/registrants", json=registrants)


def load(swoogo_spec, resources, paced=None):
    sources = runtime.build_source(swoogo_spec, selected=resources, paced=paced)
    pipeline = build_pipeline("swoogo", destination="duckdb")
    for source in sources:
        pipeline.run(source).raise_on_failed_jobs()


def test_every_built_object_is_a_source_not_a_bare_resource(swoogo_spec):
    """build_source's contract is sources, and the CLI relies on it.

    `attach_samplers` and the run summary both walk `.resources`, which a
    DltResource does not expose — so an extension returning one builds fine,
    runs fine under pipeline.run(), and dies on the first `--sample`. That is
    exactly how this was found, against the live API rather than here.
    """
    built = runtime.build_source(swoogo_spec, selected=["events", "registrants"])
    assert built, "nothing was built"
    for source in built:
        assert hasattr(source, "resources"), f"{source!r} is not a source"


def test_the_fan_out_visits_every_event_and_every_page(swoogo_spec, tmp_path):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["events", "registrants"])

    con = duckdb.connect(str(tmp_path / "swoogo_duckdb.duckdb"))

    events = con.execute("SELECT id FROM raw_swoogo.events ORDER BY id").fetchall()
    assert [r[0] for r in events] == [11, 22], "both pages of the parent must land"

    rows = con.execute(
        "SELECT id, event_id FROM raw_swoogo.registrants ORDER BY id"
    ).fetchall()
    # Four rows means: both events visited, and both pages read within each. A
    # fan-out that stops after the first event gives 2; one that ignores paging
    # also gives 2 — different bugs, same wrong count, so assert the ids.
    assert [r[0] for r in rows] == [101, 102, 201, 202]
    assert {r[1] for r in rows} == {11, 22}


def test_the_cursor_column_survives_the_sparse_projection(swoogo_spec, tmp_path):
    """`fields` must reach the wire, or incremental has nothing to compare.

    The mock returns `id` alone when `fields` is absent, exactly as Swoogo does.
    So an updated_at that is present and non-null here proves the spec's field
    list was actually sent rather than merely written down.
    """
    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["registrants"])

    con = duckdb.connect(str(tmp_path / "swoogo_duckdb.duckdb"))
    stamps = con.execute(
        "SELECT updated_at FROM raw_swoogo.registrants ORDER BY id"
    ).fetchall()
    assert all(row[0] is not None for row in stamps), stamps

    typed = con.execute(
        "SELECT typeof(updated_at) FROM raw_swoogo.registrants LIMIT 1"
    ).fetchone()[0]
    assert "TIMESTAMP" in typed.upper(), f"cursor must be a real timestamp, got {typed}"


def test_the_token_is_minted_with_basic_auth_and_reused(swoogo_spec, tmp_path):
    """The credential goes in an Authorization: Basic header, once, and the
    bearer it returns is what every data request carries."""
    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["events", "registrants"])
        history = mock.request_history

    token_calls = [r for r in history if r.url.startswith(TOKEN_URL)]
    assert len(token_calls) == 1, "one 30-minute token should serve the whole run"
    assert token_calls[0].headers["Authorization"] == "Basic ZW5jb2RlZDpjcmVkcw=="
    assert "grant_type=client_credentials" in token_calls[0].text

    # Swoogo's own endpoints only — dlt posts anonymous telemetry through the
    # same mocked transport, and it is not ours to sign.
    data_calls = [r for r in history
                  if r.url.startswith(BASE) and not r.url.startswith(TOKEN_URL)]
    assert data_calls, "the run must have fetched something"
    assert all(r.headers["Authorization"] == "Bearer tok-abc" for r in data_calls)


def test_page_size_uses_the_api_s_own_parameter_name(swoogo_spec, tmp_path):
    """Swoogo pages on `per-page` and ignores `limit`.

    Sending the wrong name is not an error — it silently yields 20 rows a
    request instead of 200, which against a 20-request/minute budget is the
    difference between a backfill of hours and one of days.
    """
    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["registrants"])
        child = [r for r in mock.request_history if "/registrants" in r.path]

    assert child, "no registrant requests were made"
    assert all(r.qs.get("per-page") == ["200"] for r in child), \
        [r.url for r in child]
    assert not any("limit" in r.qs for r in child)


def test_every_request_carries_a_timeout(swoogo_spec, tmp_path):
    """requests defaults to waiting forever, and a fan-out holds a pool of one.

    A hang here is not a slow run: the slot is never released, every later run
    queues behind it, and the only symptom is a task that never ends. This
    happened once for 858s before the scheduler SIGKILLed it, so the timeout is
    asserted rather than trusted to stay in the code.
    """
    seen = []
    with rm_module.Mocker() as mock:
        wire(mock)
        real_send = requests.Session.send

        def recording_send(self, request, **kwargs):
            seen.append(kwargs.get("timeout"))
            return real_send(self, request, **kwargs)

        with patch.object(requests.Session, "send", recording_send):
            load(swoogo_spec, ["registrants"])

    assert seen, "no requests were sent"
    assert all(t is not None for t in seen), \
        f"{seen.count(None)}/{len(seen)} requests had no timeout"


def test_the_first_run_sends_no_search_filter(swoogo_spec, tmp_path):
    """With no cursor yet there is no lower bound, and inventing one would skip
    history on the very load that is supposed to establish it."""
    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["registrants"])
        child = [r for r in mock.request_history if "/registrants" in r.path]

    assert not any("search" in r.qs for r in child), [r.url for r in child]


def test_every_request_is_paced_under_the_one_shared_family(swoogo_spec, tmp_path):
    """Swoogo's budget is per credential, not per endpoint.

    The pacer has to reach the extension too: these resources are delegated, and
    a delegated resource that paces nothing would burn the account-wide budget
    while the run still reported the limit as applied.
    """
    slept = []
    paced = runtime.EndpointPacer(swoogo_spec.rate_limits, sleeper=slept.append)

    with rm_module.Mocker() as mock:
        wire(mock)
        load(swoogo_spec, ["events", "registrants"], paced=paced)

    assert "unmatched" not in paced.requests_made, dict(paced.requests_made)
    assert set(paced.requests_made) == {"swoogo"}, dict(paced.requests_made)
    # 20/minute is one every 3s, and nothing here is the first request of its
    # family except the very first.
    assert slept and all(0 < s <= 3.0 for s in slept), slept
