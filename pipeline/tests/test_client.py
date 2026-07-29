import pytest

from ingest_runtime.ingest.client import PylonClient, PylonPaginator
from ingest_runtime.ingest.pacing import EndpointPacer


@pytest.fixture(autouse=True)
def _no_glitch_sleep(monkeypatch):
    # the paginator's glitch-retry backoff uses client.time.sleep; keep tests fast
    monkeypatch.setattr("ingest_runtime.ingest.client.time.sleep", lambda seconds: None)


def _collect(client, path):
    return [record
            for page in client.paginate_get(path, {"limit": 999}, family="directory")
            for record in page]


def test_paginate_get_follows_cursor_until_has_next_page_false(client):
    pages = list(client.paginate_get(
        "issues",
        {"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-07-01T00:00:00Z", "limit": 20000},
        family="issues_list",
    ))
    # 2 matching issues at page_size=2 -> one page; envelope reports has_next_page False
    records = [record for page in pages for record in page]
    assert [r["id"] for r in records] == ["iss_1", "iss_2"]


def test_paginate_get_pages_through_multiple_pages(client):
    pages = list(client.paginate_get(
        "issues",
        {"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-08-01T00:00:00Z", "limit": 20000},
        family="issues_list",
    ))
    records = [record for page in pages for record in page]
    # 3 matches at page_size=2 -> two pages, second requested with cursor=2
    assert [r["id"] for r in records] == ["iss_1", "iss_2", "iss_3"]


def test_paginate_search_sends_cursor_in_body(client, pylon_api):
    body = {"filter": {"field": "updated_at", "operator": "time_range",
                       "values": ["2026-01-01T00:00:00Z", "2026-12-31T00:00:00Z"]}, "limit": 500}
    pages = list(client.paginate_search(body))
    records = [record for page in pages for record in page]
    assert [r["id"] for r in records] == ["iss_1", "iss_2", "iss_3"]
    search_requests = [r for r in pylon_api.request_history if r.path == "/issues/search"]
    assert len(search_requests) == 2
    assert search_requests[1].json()["cursor"] == "2"


def test_messages_without_pagination_key_returns_single_batch(client):
    messages = client.get_issue_messages("iss_1")
    assert [m["id"] for m in messages] == ["msg_1a", "msg_1b"]


def test_messages_404_is_skipped_not_raised(client):
    assert client.get_issue_messages("iss_gone") is None


def test_pacer_counts_and_spaces_requests(client, fast_pacer):
    list(client.paginate_get(
        "issues",
        {"start_time": "2026-06-01T00:00:00Z", "end_time": "2026-08-01T00:00:00Z", "limit": 20000},
        family="issues_list",
    ))
    assert fast_pacer.requests_made["issues_list"] == 2  # two pages -> two paced requests


def test_glitched_empty_data_page_retries_same_cursor(requests_mock, fast_pacer):
    # First call to page 1 glitches (has_next_page true, no data list); retry succeeds.
    responses = [
        {"json": {"pagination": {"cursor": "c2", "has_next_page": True}}},   # glitch, no "data"
        {"json": {"data": [{"id": "a"}, {"id": "b"}], "pagination": {"has_next_page": False}}},
    ]
    requests_mock.get("https://api.usepylon.com/accounts", responses)
    client = PylonClient("k", pacer=fast_pacer)
    records = _collect(client, "accounts")
    assert [r["id"] for r in records] == ["a", "b"]
    # both requests hit the same (initial, cursorless) page
    reqs = [r for r in requests_mock.request_history if r.path == "/accounts"]
    assert len(reqs) == 2
    assert all("cursor" not in r.qs for r in reqs)


def test_glitched_pages_eventually_give_up_loudly(requests_mock, fast_pacer):
    requests_mock.get("https://api.usepylon.com/accounts",
                      json={"pagination": {"cursor": "c", "has_next_page": True}})  # always glitched
    client = PylonClient("k", pacer=fast_pacer)
    with pytest.raises(RuntimeError, match="glitched"):
        _collect(client, "accounts")


def test_paginator_glitch_retry_does_not_sleep_in_tests():
    # constructing with an injected no-op sleeper keeps the retry test fast
    paginator = PylonPaginator(cursor_path="pagination.cursor", cursor_param="cursor",
                               has_more_path="pagination.has_next_page", sleeper=lambda s: None)
    assert paginator._glitch_retries == 0


def test_pacer_sleeps_only_the_remaining_interval():
    sleeps = []
    fake_now = {"t": 0.0}

    def clock():
        return fake_now["t"]

    def sleeper(seconds):
        sleeps.append(seconds)
        fake_now["t"] += seconds

    pacer = EndpointPacer({"issues_list": 10}, sleeper=sleeper, clock=clock)  # 6s interval
    pacer.wait("issues_list")          # first request: no sleep
    fake_now["t"] += 2.0               # 2s of work
    pacer.wait("issues_list")          # should sleep the remaining 4s
    assert sleeps == [4.0]
