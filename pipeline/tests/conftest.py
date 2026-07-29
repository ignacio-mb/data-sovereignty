"""Shared fixtures: canned Pylon API responses served via requests-mock.

The mock API holds three issues (two in June 2026, one in July 2026), a couple
of directory records per entity, and per-issue messages. GET /issues respects
start_time/end_time filtering on created_at; POST /issues/search filters on
updated_at; both paginate with the Pylon cursor envelope when the page size
forces it.
"""

import json

import pytest

from ingest_runtime.ingest.client import PylonClient
from ingest_runtime.ingest.pacing import EndpointPacer
from ingest_runtime.ingest.settings import RATE_LIMITS

API = "https://api.usepylon.com"


def issue(issue_id, created_at, updated_at, latest_message_time, title="An issue", **extra):
    record = {
        "id": issue_id,
        "title": title,
        "state": "closed",
        "created_at": created_at,
        "updated_at": updated_at,
        "latest_message_time": latest_message_time,
        "account": {"id": f"acc_for_{issue_id}"},
        "assignee": {"id": "user_1", "email": "assignee@example.com"},
        "requester": {"id": "contact_1", "email": "requester@example.com"},
        "team": {"id": "team_1"},
        "custom_fields": {"plan": {"slug": "plan", "value": "pro"}},
        "tags": ["bug", "billing"],
        "csat_responses": [{"score": 5}],
    }
    record.update(extra)
    return record


ISSUES = [
    issue("iss_1", "2026-06-05T10:00:00Z", "2026-06-06T09:00:00Z", "2026-06-06T08:59:00Z"),
    issue("iss_2", "2026-06-20T11:00:00Z", "2026-07-01T12:00:00Z", "2026-07-01T11:59:00Z"),
    issue("iss_3", "2026-07-03T09:30:00Z", "2026-07-04T10:00:00Z", "2026-07-04T09:59:00Z"),
]

MESSAGES = {
    "iss_1": [
        {"id": "msg_1a", "timestamp": "2026-06-05T10:01:00Z",
         "message_html": "<p>Hello \"there\"</p>", "author": {"id": "contact_1"}},
        {"id": "msg_1b", "timestamp": "2026-06-06T08:59:00Z",
         "message_html": "<p>Line one<br>Line two</p>", "author": {"id": "user_1"}},
    ],
    "iss_2": [
        {"id": "msg_2a", "timestamp": "2026-07-01T11:59:00Z",
         "message_html": "<p>Only message</p>", "author": {"id": "contact_1"}},
    ],
    "iss_3": [
        {"id": "msg_3a", "timestamp": "2026-07-04T09:59:00Z",
         "message_html": "<p>Newest</p>", "author": {"id": "contact_1"}},
    ],
}

DIRECTORY = {
    "accounts": [
        {"id": "acc_1", "name": "Acme", "created_at": "2025-01-01T00:00:00Z",
         "custom_fields": {"tier": {"slug": "tier", "value": "enterprise"}}},
        {"id": "acc_2", "name": "Globex", "created_at": "2025-02-01T00:00:00Z"},
    ],
    "users": [{"id": "user_1", "email": "assignee@example.com", "name": "Agent One"}],
    "teams": [{"id": "team_1", "name": "Support"}],
    "contacts": [{"id": "contact_1", "email": "requester@example.com"}],
}


def _paged(records, cursor, page_size):
    """Slice records into Pylon's cursor envelope."""
    offset = int(cursor) if cursor else 0
    page = records[offset:offset + page_size]
    next_offset = offset + page_size
    return {
        "data": page,
        "pagination": {
            "cursor": str(next_offset),
            "has_next_page": next_offset < len(records),
        },
    }


@pytest.fixture
def pylon_api(requests_mock):
    """Mock the Pylon API. Returns the requests_mock adapter for assertions."""

    def issues_endpoint(request, context):
        params = request.qs
        start = params["start_time"][0].upper()
        end = params["end_time"][0].upper()
        matching = [i for i in ISSUES if start <= i["created_at"] < end]
        cursor = params.get("cursor", [None])[0]
        return _paged(matching, cursor, page_size=2)

    def search_endpoint(request, context):
        body = json.loads(request.text)
        # incremental mode sends a bounded updated_at time_range: values=[start, end]
        start, end = (v.upper() for v in body["filter"]["values"])
        matching = [i for i in ISSUES if start <= i["updated_at"] <= end]
        return _paged(matching, body.get("cursor"), page_size=2)

    def directory_endpoint(name):
        def endpoint(request, context):
            cursor = request.qs.get("cursor", [None])[0]
            return _paged(DIRECTORY[name], cursor, page_size=999)
        return endpoint

    requests_mock.get(f"{API}/issues", json=issues_endpoint)
    requests_mock.post(f"{API}/issues/search", json=search_endpoint)
    for name in DIRECTORY:
        requests_mock.get(f"{API}/{name}", json=directory_endpoint(name))
    for issue_id, messages in MESSAGES.items():
        # deliberately no pagination key: tests PylonPaginator's tolerance
        requests_mock.get(f"{API}/issues/{issue_id}/messages", json={"data": messages})
    requests_mock.get(f"{API}/issues/iss_gone/messages", status_code=404, json={"error": "not found"})

    return requests_mock


@pytest.fixture
def fast_pacer():
    """Pacer that never sleeps but still counts requests."""
    return EndpointPacer(RATE_LIMITS, sleeper=lambda seconds: None)


@pytest.fixture
def client(pylon_api, fast_pacer):
    return PylonClient("test-key", pacer=fast_pacer)


@pytest.fixture
def isolated_dlt(tmp_path, monkeypatch):
    """Run dlt fully inside tmp_path: working dir, duckdb file, schema export."""
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    (tmp_path / "schemas" / "export").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path
