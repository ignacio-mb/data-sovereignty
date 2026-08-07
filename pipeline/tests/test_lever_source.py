"""The Lever connector, driven end to end into duckdb against a mocked API.

Lever is the first source here that is BOTH camelCase JSON and millisecond
epochs, and the first whose incremental resource shares one endpoint for the
first (unbounded) read and every read after it — no window/search split, no
per-parent fan-out. So this covers different seams than Swoogo's test:

  * a cursor built from `updatedAt` (raw spelling) that has to survive into a
    warehouse column named `updated_at` and land typed as a real timestamp,
    not a null column with the value stranded in a `__v_bigint` variant — the
    exact failure `_parse_ts` was fixed for while building this connector;
  * a persisted cursor that actually advances between two separate runs, not
    just a first run that fetches everything;
  * basic auth with a blank password, which the runtime's own `_auth()` cannot
    produce for a bare session (same wall Swoogo's oauth2 case hits, different
    auth type);
  * one rate-limit family shared across BOTH the declarative resources and the
    delegated one — Lever has no per-endpoint limits, so a family that misses
    even one resource silently leaves it unpaced.

The mock implements Lever's actual envelope (`data`/`next`/`hasNext`, offset
pagination) rather than returning canned bodies, so the paginator is exercised
on every resource, not just handed a first response that happens to be
everything.
"""

import re
from unittest.mock import patch

import duckdb
import pytest
import requests
import requests_mock as rm_module

from ingest_runtime import runtime, spec
from ingest_runtime.sources import lever as lever_ext
from ingest_runtime.warehouse import build_pipeline

BASE = "https://api.lever.co/v1"

# Two opportunities, ms-epoch timestamps exactly as the live API sends them —
# these are the actual example values from Lever's own docs. Neither is
# archived, and — matching the live API rather than its own documented example
# payload — that means no `archived` key at all, not `"archivedAt": null`.
OPPORTUNITIES = [
    {"id": "opp-1", "name": "Shane Smith", "contact": "contact-1", "stage": "stage-1",
     "owner": "user-1", "confidentiality": "non-confidential",
     "tags": ["Engineering"], "sources": ["Referral"],
     "createdAt": 1407460071043, "updatedAt": 1407460080914,
     "lastInteractionAt": 1417588008760, "lastAdvancedAt": 1417587916150,
     "snoozedUntil": None,
     "stageChanges": [{"toStageId": "stage-1", "toStageIndex": 1,
                        "userId": "user-1", "updatedAt": 1407460071043}]},
    {"id": "opp-2", "name": "Grace Hopper", "contact": "contact-2", "stage": "stage-2",
     "owner": "user-2", "confidentiality": "non-confidential",
     "tags": [], "sources": [],
     "createdAt": 1407460072000, "updatedAt": 1407460081000,
     "lastInteractionAt": None, "lastAdvancedAt": None,
     "snoozedUntil": None, "stageChanges": []},
]

# Landed on a later incremental run — proves the persisted cursor, not just a
# first-run full read, and that only records at/after it come back.
OPPORTUNITY_LATE = {
    "id": "opp-3", "name": "Ada Lovelace", "contact": "contact-3", "stage": "stage-1",
    "owner": "user-1", "confidentiality": "non-confidential", "tags": [], "sources": [],
    "createdAt": 1407460090000, "updatedAt": 1407460090000,
    "lastInteractionAt": None, "lastAdvancedAt": None,
    "snoozedUntil": None, "stageChanges": [],
}

# Archived — the shape that is NOT what Lever's own docs show: `archivedAt` is
# nested inside `archived`, alongside the archive reason, not a flat top-level
# field. Confirmed by sampling the live API.
OPPORTUNITY_ARCHIVED = {
    "id": "opp-4", "name": "Katherine Johnson", "contact": "contact-4", "stage": "stage-2",
    "owner": "user-2", "confidentiality": "non-confidential", "tags": [], "sources": [],
    "createdAt": 1407460071043, "updatedAt": 1407460072043,
    "lastInteractionAt": None, "lastAdvancedAt": None, "snoozedUntil": None,
    "archived": {"reason": "ar-1", "archivedAt": 1407470000000},
    "stageChanges": [],
}

# `updatedAt: null` — also not in Lever's documented example, also real:
# confirmed against the live API, where some opportunities carry it. Lever's
# own note on the equivalent field elsewhere: it is only set once something
# changes AFTER creation, so a candidate nobody has touched since being added
# has none.
OPPORTUNITY_NEVER_TOUCHED = {
    "id": "opp-5", "name": "Radia Perlman", "contact": "contact-5", "stage": "stage-1",
    "owner": "user-1", "confidentiality": "non-confidential", "tags": [], "sources": [],
    "createdAt": 1407460071043, "updatedAt": None,
    "lastInteractionAt": None, "lastAdvancedAt": None, "snoozedUntil": None,
    "stageChanges": [],
}

POSTINGS = [
    {"id": "post-1", "text": "Infrastructure Engineer", "state": "published",
     "createdAt": 1407779365624, "updatedAt": 1407779365624},
]
USERS = [
    {"id": "user-1", "name": "Chandler Bing", "email": "chandler@example.com",
     "createdAt": 1407357447018, "deactivatedAt": None},
    {"id": "user-2", "name": "Rachel Green", "email": "rachel@example.com",
     "createdAt": 1478035107621, "deactivatedAt": 1409556487918},
]
STAGES = [{"id": "stage-1", "text": "New applicant"}, {"id": "stage-2", "text": "Phone screen"}]
ARCHIVE_REASONS = [{"id": "ar-1", "text": "Underqualified", "status": "active", "type": "non-hired"}]
SOURCES = [{"text": "Referral", "count": 90}, {"text": "Posting", "count": 51}]
TAGS = [{"text": "Engineering", "count": 23}]
REQUISITIONS = [
    {"id": "req-1", "requisitionCode": "ENG-1", "name": "Senior Engineer", "status": "open",
     "createdAt": 1450296000000, "updatedAt": 1590866770059, "closedAt": None,
     "hiringManager": "user-1", "owner": "user-2"},
]
REQUISITION_FIELDS = [{"id": "cost_center", "text": "Cost center", "type": "object"}]


def _envelope(rows, request, per_page):
    """Lever's own offset envelope, paginated the way the real API pages."""
    offset = request.qs.get("offset", ["0"])[0]
    start = int(offset) if offset.isdigit() else 0
    window = rows[start:start + per_page]
    next_offset = start + per_page
    has_next = next_offset < len(rows)
    body = {"data": window, "hasNext": has_next}
    if has_next:
        body["next"] = str(next_offset)
    return body


@pytest.fixture
def lever_spec():
    """The real shipped spec, not a fixture copy — so drift fails this test."""
    return spec.load("lever")


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("LEVER_API_KEY", "test-api-key")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    monkeypatch.chdir(tmp_path)
    # Process-scoped, so it survives across tests unless cleared — same
    # reason Swoogo's test fixture clears its own _EVENT_IDS.
    lever_ext._WORKLIST_CACHE.clear()


def wire(mock, opportunities=None, updated_at_start=None):
    """Every account-wide lookup, plus opportunities (page size 1, so two
    records need two pages)."""
    rows = OPPORTUNITIES if opportunities is None else opportunities

    def opportunities_handler(request, context):
        wanted = request.qs.get("updated_at_start", [None])[0]
        if updated_at_start is not None:
            assert wanted == str(updated_at_start), (wanted, updated_at_start)
        served = rows
        if wanted is not None:
            served = [r for r in rows if r["updatedAt"] >= int(wanted)]
        return _envelope(served, request, per_page=1)

    mock.get(f"{BASE}/opportunities", json=opportunities_handler)
    mock.get(f"{BASE}/postings", json=lambda req, ctx: _envelope(POSTINGS, req, per_page=100))
    mock.get(f"{BASE}/users", json=lambda req, ctx: _envelope(USERS, req, per_page=100))
    mock.get(f"{BASE}/stages", json=lambda req, ctx: _envelope(STAGES, req, per_page=100))
    mock.get(f"{BASE}/archive_reasons",
              json=lambda req, ctx: _envelope(ARCHIVE_REASONS, req, per_page=100))
    mock.get(f"{BASE}/sources", json=lambda req, ctx: _envelope(SOURCES, req, per_page=100))
    mock.get(f"{BASE}/tags", json=lambda req, ctx: _envelope(TAGS, req, per_page=100))
    mock.get(f"{BASE}/requisitions",
              json=lambda req, ctx: _envelope(REQUISITIONS, req, per_page=100))
    mock.get(f"{BASE}/requisition_fields",
              json=lambda req, ctx: _envelope(REQUISITION_FIELDS, req, per_page=100))


def load(lever_spec, resources, paced=None):
    sources = runtime.build_source(lever_spec, selected=resources, paced=paced)
    pipeline = build_pipeline("lever", destination="duckdb")
    for source in sources:
        pipeline.run(source).raise_on_failed_jobs()


ALL = ["opportunities", "postings", "users", "stages", "archive_reasons",
       "sources", "tags", "requisitions", "requisition_fields"]


def test_every_built_object_is_a_source_not_a_bare_resource(lever_spec):
    built = runtime.build_source(lever_spec, selected=ALL)
    assert built, "nothing was built"
    for source in built:
        assert hasattr(source, "resources"), f"{source!r} is not a source"


def test_opportunities_pagination_walks_every_page(lever_spec, tmp_path):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["opportunities"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.opportunities ORDER BY id").fetchall()
    assert [r[0] for r in ids] == ["opp-1", "opp-2"]


def test_the_first_run_sends_no_updated_at_start_filter(lever_spec):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["opportunities"])
        opp_calls = [r for r in mock.request_history if r.path == "/v1/opportunities"]

    assert opp_calls, "no opportunity requests were made"
    assert not any("updated_at_start" in r.qs for r in opp_calls), \
        [r.url for r in opp_calls]


def test_a_second_run_uses_the_persisted_cursor(lever_spec, tmp_path):
    """The cursor has to survive between runs, not just exist within one."""
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["opportunities"])

    # The lookback in the spec is 300s = 300_000ms; the newest updatedAt
    # loaded is opp-2's, 1407460081000.
    expected_since = 1407460081000 - 300_000

    with rm_module.Mocker() as mock:
        wire(mock, opportunities=OPPORTUNITIES + [OPPORTUNITY_LATE],
             updated_at_start=expected_since)
        load(lever_spec, ["opportunities"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.opportunities ORDER BY id").fetchall()
    # opp-1/opp-2 already landed; opp-3 is the only genuinely new row, and the
    # merge write disposition means re-serving opp-2 inside the lookback does
    # not duplicate it.
    assert [r[0] for r in ids] == ["opp-1", "opp-2", "opp-3"]


def test_initial_value_bounds_a_first_run_and_then_gets_out_of_the_way(lever_spec, tmp_path):
    """The mechanism a bounded production smoke test relies on: not a CLI
    flag (--start/--end never reach a delegated resource), but dlt's own
    `initial_value`, seeded directly by calling build_opportunities().

    Widely separated timestamps on purpose (~14 years apart), not the close
    together OPPORTUNITIES fixtures used elsewhere in this file: the spec's
    own 300s lookback is subtracted from whatever floor is chosen, and a gap
    of a few seconds — the scale everything else here uses — would vanish
    into that subtraction and prove nothing.
    """
    import pendulum

    old = {"id": "opp-old", "updatedAt": 1_000_000_000_000, "createdAt": 1_000_000_000_000,
           "lastInteractionAt": None, "lastAdvancedAt": None, "snoozedUntil": None, "stageChanges": []}
    new = {"id": "opp-new", "updatedAt": 1_700_000_000_000, "createdAt": 1_700_000_000_000,
           "lastInteractionAt": None, "lastAdvancedAt": None, "snoozedUntil": None, "stageChanges": []}

    floor = pendulum.from_timestamp(1_650_000_000, tz="UTC")  # between the two, in seconds
    floor_ms = int(floor.timestamp() * 1000) - 300_000  # minus the spec's own lookback

    with rm_module.Mocker() as mock:
        wire(mock, opportunities=[old, new], updated_at_start=floor_ms)
        pipeline = build_pipeline("lever", destination="duckdb")
        source = lever_ext.build_opportunities(lever_spec, lever_spec.resource("opportunities"),
                                                 initial_value=floor)
        pipeline.run(source).raise_on_failed_jobs()

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.opportunities ORDER BY id").fetchall()
    # opp-old predates the floor and must NOT be fetched; opp-new is after it.
    assert [r[0] for r in ids] == ["opp-new"]

    # A second run — the standard, unparameterized path — must carry on from
    # the cursor THIS run persisted, not fall back to the floor again.
    even_newer = {**new, "id": "opp-newer", "updatedAt": 1_700_000_010_000, "createdAt": 1_700_000_010_000}
    expected_since = new["updatedAt"] - 300_000
    with rm_module.Mocker() as mock:
        wire(mock, opportunities=[old, new, even_newer], updated_at_start=expected_since)
        load(lever_spec, ["opportunities"])

    ids = con.execute("SELECT id FROM raw_lever.opportunities ORDER BY id").fetchall()
    assert [r[0] for r in ids] == ["opp-new", "opp-newer"]


class TestTimestampsLandAsRealTimestamps:
    """The exact gap `_parse_ts` was fixed for: Lever's epochs are
    milliseconds, and the wrong fix here lands a null column with the value
    stranded in a `__v_bigint` variant instead of raising."""

    def test_the_incremental_resource(self, lever_spec, tmp_path):
        with rm_module.Mocker() as mock:
            wire(mock)
            load(lever_spec, ["opportunities"])

        con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
        kind, value = con.execute(
            "SELECT typeof(updated_at), updated_at FROM raw_lever.opportunities "
            "WHERE id = 'opp-1'").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"updated_at landed as {kind}, not a timestamp"
        assert value.year == 2014  # 1407460080914ms is 2014-08-08

    def test_a_declarative_resource(self, lever_spec, tmp_path):
        with rm_module.Mocker() as mock:
            wire(mock)
            load(lever_spec, ["requisitions"])

        con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
        kind, value = con.execute(
            "SELECT typeof(created_at), created_at FROM raw_lever.requisitions LIMIT 1"
        ).fetchone()
        assert "TIMESTAMP" in kind.upper(), f"created_at landed as {kind}, not a timestamp"
        assert value.year == 2015  # 1450296000000ms is 2015-12-16

    def test_a_promoted_nested_timestamp(self, lever_spec, tmp_path):
        """archived_at is dug out of a nested `archived` object by `promote`,
        never touched by flatten_record's timestamp_columns loop at all — a
        separate path from the two tests above, and one the `_parse_ts` fix
        alone does not cover without also routing `promote` through it."""
        with rm_module.Mocker() as mock:
            wire(mock, opportunities=[OPPORTUNITY_ARCHIVED])
            load(lever_spec, ["opportunities"])

        con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
        kind, value, reason = con.execute(
            "SELECT typeof(archived_at), archived_at, archive_reason_id "
            "FROM raw_lever.opportunities WHERE id = 'opp-4'").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"archived_at landed as {kind}, not a timestamp"
        assert value.year == 2014
        assert reason == "ar-1"


def test_a_null_updated_at_does_not_fail_the_run(lever_spec, tmp_path):
    """Confirmed against the live API: some real opportunities have no
    updatedAt at all. dlt's incremental raises on that by default
    (on_cursor_value_missing='raise') — the fix is 'include', not silently
    dropping the row, which 'exclude' would do forever since a None cursor
    value never compares greater than anything."""
    with rm_module.Mocker() as mock:
        wire(mock, opportunities=OPPORTUNITIES + [OPPORTUNITY_NEVER_TOUCHED])
        load(lever_spec, ["opportunities"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.opportunities ORDER BY id").fetchall()
    assert [r[0] for r in ids] == ["opp-1", "opp-2", "opp-5"]
    updated_at = con.execute(
        "SELECT updated_at FROM raw_lever.opportunities WHERE id = 'opp-5'").fetchone()[0]
    assert updated_at is None


def test_basic_auth_is_the_api_key_with_a_blank_password(lever_spec):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ALL)
        history = mock.request_history

    data_calls = [r for r in history if r.url.startswith(BASE)]
    assert data_calls, "the run must have fetched something"
    for request in data_calls:
        # requests_mock decodes Basic auth on request.headers itself; assert
        # the header's actual base64 content instead, since that is what
        # would be wrong if the password were not blank.
        import base64
        auth_header = request.headers["Authorization"]
        assert auth_header.startswith("Basic ")
        decoded = base64.b64decode(auth_header.removeprefix("Basic ")).decode()
        assert decoded == "test-api-key:", decoded


def test_postings_asks_for_both_distribution_channels(lever_spec):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["postings"])
        calls = [r for r in mock.request_history if r.path == "/v1/postings"]

    assert calls
    assert all(set(r.qs.get("distributionchannel", [])) == {"public", "internal"}
               for r in calls), [r.url for r in calls]


def test_users_includes_deactivated(lever_spec):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["users"])
        calls = [r for r in mock.request_history if r.path == "/v1/users"]

    assert calls
    assert all(r.qs.get("includedeactivated") == ["true"] for r in calls), \
        [r.url for r in calls]


def test_sources_and_tags_dedupe_on_text_not_id(lever_spec, tmp_path):
    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ["sources", "tags"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    texts = con.execute("SELECT text FROM raw_lever.sources ORDER BY text").fetchall()
    assert [r[0] for r in texts] == ["Posting", "Referral"]


def test_every_extension_request_carries_a_timeout(lever_spec):
    """requests waits forever by default, and the extension holds the
    source's pool of one while it does."""
    seen = []
    with rm_module.Mocker() as mock:
        wire(mock)
        real_send = requests.Session.send

        def recording_send(self, request, **kwargs):
            if request.url.startswith(f"{BASE}/opportunities"):
                seen.append(kwargs.get("timeout"))
            return real_send(self, request, **kwargs)

        with patch.object(requests.Session, "send", recording_send):
            load(lever_spec, ["opportunities"])

    assert seen, "no opportunity requests were sent"
    assert all(t is not None for t in seen), f"{seen.count(None)}/{len(seen)} had no timeout"


def test_every_request_is_paced_under_the_one_shared_family(lever_spec):
    """Lever has no per-endpoint limits, so declarative and delegated
    resources alike must share the single `lever` family — a resource left
    off it is silently unthrottled."""
    slept = []
    paced = runtime.EndpointPacer(lever_spec.rate_limits, sleeper=slept.append)

    with rm_module.Mocker() as mock:
        wire(mock)
        load(lever_spec, ALL, paced=paced)

    assert "unmatched" not in paced.requests_made, dict(paced.requests_made)
    assert set(paced.requests_made) == {"lever"}, dict(paced.requests_made)


# ═══ the nine per-opportunity children ═══════════════════════════════════════
#
# A separate, self-contained wiring from everything above: the worklist here
# only ever needs `id`+`updatedAt` (what `include=` narrows the real API to),
# and the child endpoints are keyed by opportunity id via a URL a real path
# template produces, not a canned list — so a fan-out that builds the wrong
# path 404s here exactly as it would live.

CHILD_RESOURCES = ["applications", "notes", "interviews", "feedback", "offers",
                    "panels", "referrals", "resumes", "forms"]

# Same 300s the shipped spec declares for every one of the nine.
LOOKBACK_MS = 300_000


def wire_fanout(mock, opportunities, children, expected_since=None):
    """opportunities: [{"id":..., "updatedAt":...}, ...]. children:
    {(opportunity_id, resource_name): [records]}.
    """
    def opportunities_handler(request, context):
        wanted = request.qs.get("updated_at_start", [None])[0]
        if expected_since is not None:
            assert wanted == str(expected_since), (wanted, expected_since)
        served = opportunities
        if wanted is not None:
            served = [o for o in opportunities if (o.get("updatedAt") or 0) >= int(wanted)]
        return _envelope(served, request, per_page=100)

    mock.get(f"{BASE}/opportunities", json=opportunities_handler)

    def make_child_handler(name):
        def handler(request, context):
            opportunity_id = request.path.rstrip("/").split("/")[-2]
            rows = children.get((opportunity_id, name), [])
            return _envelope(rows, request, per_page=100)
        return handler

    for name in CHILD_RESOURCES:
        mock.get(re.compile(rf"{re.escape(BASE)}/opportunities/[^/]+/{name}$"),
                  json=make_child_handler(name))


def test_children_fan_out_and_inject_opportunity_id(lever_spec, tmp_path):
    """Notes carries no opportunityId of its own on the live API — the
    connector has to set it from the id it used to fetch the page."""
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}, {"id": "opp-2", "updatedAt": 2_000_000}]
    children = {
        ("opp-1", "notes"): [
            {"id": "note-1", "text": "hi", "createdAt": 1_000_000, "completedAt": 1_000_000, "deletedAt": None},
            {"id": "note-2", "text": "yo", "createdAt": 1_100_000, "completedAt": None, "deletedAt": None},
        ],
        ("opp-2", "notes"): [],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["notes"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    rows = con.execute(
        "SELECT id, opportunity_id FROM raw_lever.notes ORDER BY id").fetchall()
    assert rows == [("note-1", "opp-1"), ("note-2", "opp-1")]


def test_applications_promotes_the_nested_archived_object(lever_spec, tmp_path):
    """Same nested `archived: {reason, archivedAt}` shape as opportunities —
    confirmed live, not assumed — reached through build_resource's
    parent_watermark path rather than build_opportunities' search_window
    one, so this is a distinct code path even though both ultimately call
    the same shared make_transformer()."""
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}]
    children = {
        ("opp-1", "applications"): [
            {"id": "app-1", "createdAt": 1_000_000,
             "archived": {"reason": "ar-1", "archivedAt": 1_000_500}},
        ],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["applications"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    kind, value, reason = con.execute(
        "SELECT typeof(archived_at), archived_at, archive_reason_id "
        "FROM raw_lever.applications WHERE id = 'app-1'").fetchone()
    assert "TIMESTAMP" in kind.upper(), f"archived_at landed as {kind}, not a timestamp"
    assert value is not None
    assert reason == "ar-1"


def test_notes_and_offers_never_land_the_fields_column(lever_spec, tmp_path):
    """notes.fields/offers.fields carry freeform assessment text and
    compensation/PII on the live account — sources/lever.yml excludes both via
    `exclude_columns`, not something specific to this test's fixture data."""
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}]
    children = {
        ("opp-1", "notes"): [
            {"id": "note-1", "text": "hi", "createdAt": 1_000_000,
             "fields": [{"zythr": {"summary": "sensitive assessment text"}}]},
        ],
        ("opp-1", "offers"): [
            {"id": "offer-1", "createdAt": 1_000_000, "status": "SENT",
             "fields": [{"identifier": "salary_amount", "value": 180000}]},
        ],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["notes", "offers"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    for table in ("notes", "offers"):
        columns = {row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = 'raw_lever' AND table_name = '{table}'"
        ).fetchall()}
        assert "fields" not in columns, f"raw_lever.{table} must never land a fields column"


def test_a_zero_yield_opportunity_still_advances_the_watermark(lever_spec, tmp_path):
    """The exact bug a naive dlt.sources.incremental() cursor would have: an
    opportunity with no notes at all must not permanently drag the watermark
    back to the last opportunity that DID have one — or every later run keeps
    re-including it, forever, since nothing ever tells the cursor it was
    already checked.
    """
    opp_a = {"id": "opp-a", "updatedAt": 1_000_000}   # has a note
    opp_b = {"id": "opp-b", "updatedAt": 2_000_000}   # zero notes — the trap
    children_run1 = {
        ("opp-a", "notes"): [
            {"id": "note-1", "createdAt": 1_000_000, "completedAt": None, "deletedAt": None},
        ],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, [opp_a, opp_b], children_run1)
        load(lever_spec, ["notes"])

    # Run 2 must query bounded by opp-b's updatedAt (the highest the worklist
    # actually reached), minus the lookback — NOT opp-a's, which is what a
    # per-record incremental cursor would have been stuck on.
    expected_since = opp_b["updatedAt"] - LOOKBACK_MS
    opp_c = {"id": "opp-c", "updatedAt": 5_000_000}
    children_run2 = dict(children_run1)
    children_run2[("opp-c", "notes")] = [
        {"id": "note-2", "createdAt": 5_000_000, "completedAt": None, "deletedAt": None},
    ]

    with rm_module.Mocker() as mock:
        wire_fanout(mock, [opp_a, opp_b, opp_c], children_run2, expected_since=expected_since)
        load(lever_spec, ["notes"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.notes ORDER BY id").fetchall()
    assert [r[0] for r in ids] == ["note-1", "note-2"]


def test_first_run_worklist_has_no_time_filter(lever_spec):
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}]
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, {})
        load(lever_spec, ["notes"])
        calls = [r for r in mock.request_history if r.path == "/v1/opportunities"]

    assert calls
    assert not any("updated_at_start" in r.qs for r in calls), [r.url for r in calls]


def test_initial_watermark_bounds_a_first_run(lever_spec, tmp_path):
    """The fan-out's equivalent of build_opportunities' initial_value: a raw
    epoch-ms int (this resource's watermark is never a dlt column, so nothing
    here ever routes it through _parse_ts, unlike the opportunities case)."""
    old = {"id": "opp-old", "updatedAt": 1_000_000_000_000}
    new = {"id": "opp-new", "updatedAt": 1_700_000_000_000}
    children = {
        ("opp-old", "notes"): [{"id": "note-old", "createdAt": 1_000_000_000_000,
                                  "completedAt": None, "deletedAt": None}],
        ("opp-new", "notes"): [{"id": "note-new", "createdAt": 1_700_000_000_000,
                                  "completedAt": None, "deletedAt": None}],
    }
    floor = 1_650_000_000_000  # between the two
    expected_since = floor - LOOKBACK_MS

    with rm_module.Mocker() as mock:
        wire_fanout(mock, [old, new], children, expected_since=expected_since)
        pipeline = build_pipeline("lever", destination="duckdb")
        resource = lever_spec.resource("notes")
        source = lever_ext.build_resource(lever_spec, resource, initial_watermark=floor)
        pipeline.run(source).raise_on_failed_jobs()

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    ids = con.execute("SELECT id FROM raw_lever.notes ORDER BY id").fetchall()
    # opp-old predates the floor and must not be fanned out at all.
    assert [r[0] for r in ids] == ["note-new"]


def test_worklist_is_shared_across_resources_in_one_run(lever_spec):
    """Every one of the nine needs the identical worklist on a given run —
    without the shared cache, N resources would independently re-ask the
    identical question N times."""
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}]
    children = {
        ("opp-1", "notes"): [{"id": "note-1", "createdAt": 1_000_000, "completedAt": None, "deletedAt": None}],
        ("opp-1", "interviews"): [],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["notes", "interviews"])
        worklist_calls = [r for r in mock.request_history if r.path == "/v1/opportunities"]

    assert len(worklist_calls) == 1, [c.url for c in worklist_calls]


def test_fan_out_timestamps_land_as_real_timestamps(lever_spec, tmp_path):
    """The same ms-epoch gap `_parse_ts` was fixed for, on a resource reached
    by a completely different code path (parent_watermark, not
    search_window) — proves the fix is in the shared transform, not
    something local to `opportunities`."""
    opportunities = [{"id": "opp-1", "updatedAt": 1_407_460_080_914}]
    children = {
        ("opp-1", "notes"): [
            {"id": "note-1", "createdAt": 1_407_460_071_043, "completedAt": None, "deletedAt": None},
        ],
    }
    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["notes"])

    con = duckdb.connect(str(tmp_path / "lever_duckdb.duckdb"))
    kind, value = con.execute(
        "SELECT typeof(created_at), created_at FROM raw_lever.notes WHERE id = 'note-1'").fetchone()
    assert "TIMESTAMP" in kind.upper(), f"created_at landed as {kind}, not a timestamp"
    assert value.year == 2014


def test_every_fan_out_request_is_paced_under_the_shared_family(lever_spec):
    opportunities = [{"id": "opp-1", "updatedAt": 1_000_000}]
    children = {("opp-1", "notes"): [{"id": "note-1", "createdAt": 1_000_000,
                                        "completedAt": None, "deletedAt": None}]}
    slept = []
    paced = runtime.EndpointPacer(lever_spec.rate_limits, sleeper=slept.append)

    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, children)
        load(lever_spec, ["notes"], paced=paced)

    assert "unmatched" not in paced.requests_made, dict(paced.requests_made)
    assert set(paced.requests_made) == {"lever"}, dict(paced.requests_made)


def test_the_fan_out_actually_overlaps_requests(lever_spec):
    """Not just "still works" — proves the fetch loop actually runs
    concurrently, so a future regression back to one-at-a-time (e.g. someone
    "simplifying" the loop) fails a test instead of just quietly being slow
    again.

    Observed at the `_pages` call, not the mocked HTTP layer: requests_mock's
    own Mocker serializes access internally, so two real threads calling a
    mocked `session.get()` never actually overlap regardless of what the
    code under test does — confirmed by running this at the HTTP layer
    first and seeing peak concurrency pinned at 1 even though the live API
    measurements (in the spec's own comments) show real overlap. `_pages` is
    a plain function call, made synchronously by whichever worker thread
    calls it, with no such shared test-double state to serialize on.
    """
    import threading
    import time as time_module
    from unittest.mock import patch

    opportunities = [{"id": f"opp-{i}", "updatedAt": 1_000_000 + i} for i in range(20)]

    lock = threading.Lock()
    concurrency = {"current": 0, "peak": 0}
    real_pages = lever_ext._pages

    def tracking_pages(spec_, session, endpoint, since):
        if endpoint.get("path") == "/opportunities":
            return real_pages(spec_, session, endpoint, since)  # the worklist call, untouched
        with lock:
            concurrency["current"] += 1
            concurrency["peak"] = max(concurrency["peak"], concurrency["current"])
        time_module.sleep(0.05)
        with lock:
            concurrency["current"] -= 1
        return real_pages(spec_, session, endpoint, since)

    with rm_module.Mocker() as mock:
        wire_fanout(mock, opportunities, {})
        with patch.object(lever_ext, "_pages", side_effect=tracking_pages):
            load(lever_spec, ["notes"])

    assert concurrency["peak"] > 1, (
        f"requests never overlapped (peak={concurrency['peak']}) — the fan-out is running sequentially"
    )


def test_every_built_object_including_fan_out_is_a_source(lever_spec):
    built = runtime.build_source(lever_spec, selected=ALL + CHILD_RESOURCES)
    assert built, "nothing was built"
    for source in built:
        assert hasattr(source, "resources"), f"{source!r} is not a source"
