"""YouTube: the report queue, the Data API crawls, and the traps in both.

The mocks here implement the APIs' behaviour rather than returning canned
bodies — pagination is real, the report queue really does hand back a
restatement of a day it already served, and one CSV really does put its columns
in a different order. A single-page mock would pass while a broken paginator
truncated every load, and a fixed-column mock would pass while a positional
parser transposed OS codes with device codes.

Offline: no network, no secrets, duckdb.
"""

from __future__ import annotations

import json
import typing

import duckdb
import pytest
import requests_mock as rm_module

from ingest_runtime import runtime, spec
from ingest_runtime.sources import youtube
from ingest_runtime.warehouse import build_pipeline

TOKEN_URL = "https://oauth2.googleapis.com/token"
REPORTING = "https://youtubereporting.googleapis.com/v1"
DATA_API = "https://www.googleapis.com/youtube/v3"
CHANNEL_ID = "UCtest0000000000000000"
UPLOADS = "UUtest0000000000000000"


@pytest.fixture
def youtube_spec():
    return spec.load("youtube")


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    """A run's worth of environment, and no state carried between tests.

    The extension caches sessions, job ids, the channel and the video list for
    the length of the PROCESS — which is one run under Airflow but many tests
    here, so leaking them between tests would let a later test pass on an
    earlier one's fixtures.
    """
    monkeypatch.setenv("YOUTUBE_OAUTH_CLIENT_ID", "client-id")
    monkeypatch.setenv("YOUTUBE_OAUTH_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("YOUTUBE_OAUTH_REFRESH_TOKEN", "refresh-token")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    # One attempt: a test that exercises a skipped 403 should not wait out
    # dlt's production retry ladder.
    monkeypatch.setenv("RUNTIME__REQUEST_MAX_ATTEMPTS", "1")
    monkeypatch.chdir(tmp_path)
    for cache in (youtube._SESSIONS, youtube._JOB_IDS, youtube._CHANNELS, youtube._VIDEO_IDS):
        cache.clear()
    yield
    for cache in (youtube._SESSIONS, youtube._JOB_IDS, youtube._CHANNELS, youtube._VIDEO_IDS):
        cache.clear()


# ── The fake YouTube ─────────────────────────────────────────────────────────


def oauth(mock, expires_in=3600):
    mock.post(TOKEN_URL, json={"access_token": "access-1", "expires_in": expires_in,
                               "token_type": "Bearer"})


def report(report_id, job_id, day, create_time):
    """A Report resource, as jobs.reports.list returns it."""
    return {
        "id": report_id,
        "jobId": job_id,
        "startTime": f"{day}T07:00:00Z",
        "endTime": f"{day}T07:00:00Z",
        "createTime": create_time,
        "downloadUrl": f"{REPORTING}/media/CS{report_id}",
    }


BASIC_HEADER = ("date,channel_id,video_id,live_or_on_demand,subscribed_status,country_code,"
                "engaged_views,views,subscribers_gained,subscribers_lost")


def basic_csv(day, views, *, extra_rows=()):
    """A channel_basic_a3 CSV.

    Row 2 is the one that matters: a channel-level subscriber row with an EMPTY
    video_id. Google documents it — "a basic user activity report contains a
    row that does not specify a video_id dimension value" — and video_id is a
    primary-key column, so an empty string there is the shape a merge key has
    to survive.
    """
    rows = [
        f"{day},{CHANNEL_ID},vid001,ON_DEMAND,SUBSCRIBED,US,{views},{views},1,0",
        f"{day},{CHANNEL_ID},,ON_DEMAND,SUBSCRIBED,US,0,0,4,1",
        *extra_rows,
    ]
    return "\n".join([BASIC_HEADER, *rows]) + "\n"


def paged(pages, item_key="items"):
    """A requests_mock callback serving `pages` in order, by pageToken.

    Real pagination: page N hands back a token that page N+1 requires, so a
    paginator that stops after the first response fails the assertion on row
    count rather than passing quietly.
    """
    def handler(request, _context):
        token = request.qs.get("pagetoken", [None])[0]
        index = 0 if token is None else int(token)
        body = {item_key: pages[index]}
        if index + 1 < len(pages):
            body["nextPageToken"] = str(index + 1)
        return body
    return handler


def video_resource(n):
    """A video with EVERY field the spec promotes populated.

    Deliberately complete rather than realistic: dlt materialises a column only
    once some row carries a value, so a fixture that left a field out would make
    `test_fivetran_column_names_are_reproduced` pass on a typo'd `promote` path.
    A field YouTube rarely sends any more (statistics.dislikeCount) is included
    for exactly that reason — the assertion is about the path, not the data.
    """
    return {
        "id": f"vid{n:03d}",
        "etag": f"etag{n}",
        "kind": "youtube#video",
        "snippet": {
            "channelId": CHANNEL_ID, "channelTitle": "Test Channel",
            "title": f"Video {n}", "description": f"Description {n}",
            "publishedAt": "2026-06-01T10:00:00Z", "categoryId": "22",
            "defaultLanguage": "en", "defaultAudioLanguage": "en-US",
            "tags": ["a", "b"], "liveBroadcastContent": "none",
            "thumbnails": {"default": {"url": "https://i.ytimg.com/x.jpg"}},
            "localized": {"title": f"Video {n}", "description": "d"},
        },
        "contentDetails": {"duration": "PT10M1S", "dimension": "2d", "definition": "hd",
                           "caption": "true", "licensedContent": True,
                           "projection": "rectangular", "hasCustomThumbnail": True,
                           "regionRestriction": {"allowed": ["US"]}},
        "statistics": {"viewCount": str(100 * n), "likeCount": "5", "dislikeCount": "1",
                       "favoriteCount": "0", "commentCount": "2"},
        "status": {"uploadStatus": "processed", "privacyStatus": "public",
                   "failureReason": "", "rejectionReason": "",
                   "license": "youtube", "embeddable": True,
                   "publicStatsViewable": True, "madeForKids": False,
                   "selfDeclaredMadeForKids": False,
                   "publishAt": "2026-06-01T10:00:00Z"},
        "player": {"embedHtml": "<iframe/>", "embedHeight": 360, "embedWidth": 640},
    }


def channel_resource():
    return {
        "id": CHANNEL_ID, "etag": "chetag", "kind": "youtube#channel",
        "snippet": {"title": "Test Channel", "description": "About",
                    "customUrl": "@test", "publishedAt": "2020-01-01T00:00:00Z",
                    "country": "US", "defaultLanguage": "en",
                    "localized": {"title": "Test Channel", "description": "About"},
                    "thumbnails": {"default": {"url": "https://yt3.ggpht.com/x"}}},
        "contentDetails": {"relatedPlaylists": {"uploads": UPLOADS, "likes": "LLtest"}},
        "statistics": {"viewCount": "12345", "subscriberCount": "678",
                       "hiddenSubscriberCount": False, "videoCount": "3"},
        "status": {"privacyStatus": "public"},
        "brandingSettings": {"channel": {"title": "Test Channel"}},
        "topicDetails": {"topicCategories": ["https://en.wikipedia.org/wiki/Music"]},
    }


def mock_channel(mock):
    mock.get(f"{DATA_API}/channels", json={"items": [channel_resource()]})


def load(sources, destination="duckdb"):
    pipeline = build_pipeline("youtube", destination=destination)
    for source in sources:
        pipeline.run(source).raise_on_failed_jobs()
    return pipeline


def resource_state(pipeline, resource_name):
    """The persisted state dlt kept for one resource, or {}."""
    sources = (pipeline.state.get("sources") or {})
    return ((sources.get("youtube") or {}).get("resources") or {}).get(resource_name, {})


def rows(pipeline, sql):
    con = duckdb.connect(str(pipeline.working_dir and _duckdb_path(pipeline)))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _duckdb_path(pipeline):
    # build_pipeline namespaces a non-production destination, so the file is
    # youtube_duckdb.duckdb beside the cwd the test chdir'd into.
    return "youtube_duckdb.duckdb"


# ── The invariant that keeps this connector off the declarative path ─────────


class TestNothingReachesTheDeclarativePath:
    """`runtime._auth()` cannot build this spec's credential, by design.

    A refresh-token grant is three secrets and `api.auth` carries one
    `token_env`, so the spec names an auth type `_auth()` refuses. That is only
    safe while every resource is delegated to the extension. A resource
    switched to `full_refresh` would route through `rest_api_source`, call
    `_auth()`, and die with a message about auth types — so the invariant is
    asserted here, where the failure names the real cause.
    """

    def test_no_resource_uses_a_declarative_strategy(self, youtube_spec):
        declarative = [r.name for r in youtube_spec.resources
                       if r.strategy in runtime._DECLARATIVE_STRATEGIES]
        assert declarative == [], (
            f"{declarative} would be built declaratively, which calls runtime._auth() "
            f"and fails on auth type {youtube_spec.api['auth']['type']!r}. Either give "
            f"the resource a delegated strategy or teach the runtime this grant."
        )

    def test_the_runtime_really_does_refuse_this_auth_type(self, youtube_spec):
        """Guards the assumption above rather than trusting it: if `_auth()`
        ever learns this type, the reason for the whole arrangement is gone and
        this test should be the thing that says so."""
        with pytest.raises(RuntimeError, match="is not one dlt can build"):
            runtime._auth(youtube_spec)

    def test_every_resource_has_a_builder(self, youtube_spec):
        extension = runtime.extensions(youtube_spec)
        for resource in youtube_spec.resources:
            builder = (getattr(extension, f"build_{resource.name}", None)
                       or getattr(extension, "build_resource", None))
            assert builder is not None, resource.name

    def test_the_extension_refuses_a_mismatched_auth_type(self, youtube_spec, monkeypatch):
        broken = spec.load("youtube")
        broken._doc["api"]["auth"] = dict(broken.api["auth"], type="bearer")
        with pytest.raises(RuntimeError, match="oauth2_refresh_token"):
            youtube._GoogleOAuth(broken, youtube._plain_session)


# ── Auth ─────────────────────────────────────────────────────────────────────


class TestAuth:
    def test_the_refresh_token_is_exchanged_and_the_header_is_set(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            load(runtime.build_source(youtube_spec, selected=["channel"]))

            token_call = next(r for r in mock.request_history if r.url.startswith(TOKEN_URL))
            assert token_call.text is not None
            body = dict(pair.split("=", 1) for pair in token_call.text.split("&"))
            assert body["grant_type"] == "refresh_token"
            assert body["refresh_token"] == "refresh-token"
            assert body["client_id"] == "client-id"

            data_call = next(r for r in mock.request_history if "/youtube/v3/" in r.url)
            assert data_call.headers["Authorization"] == "Bearer access-1"

    def test_one_token_serves_the_whole_run(self, youtube_spec):
        """Twenty-five resources minting twenty-five tokens would spend from
        the same 100-refresh-tokens-per-client ceiling Google enforces."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlists", json={"items": []})
            load(runtime.build_source(youtube_spec, selected=["channel", "playlist"]))
            assert len([r for r in mock.request_history if r.url.startswith(TOKEN_URL)]) == 1

    def test_a_rejected_refresh_token_is_terminal_and_says_what_to_do(self, youtube_spec):
        """invalid_grant means the credential is dead — revoked, six months
        unused, or a Testing-status consent screen. Retrying cannot fix it, and
        a retry loop that mints tokens silently evicts the live one."""
        with rm_module.Mocker() as mock:
            mock.post(TOKEN_URL, status_code=400, json={"error": "invalid_grant"})
            mock_channel(mock)
            with pytest.raises(Exception, match="refresh token"):
                load(runtime.build_source(youtube_spec, selected=["channel"]))


# ── The report queue ─────────────────────────────────────────────────────────


class TestReportQueue:
    def test_a_report_lands_with_its_composite_key_and_provenance(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "job-basic", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/job-basic/reports", json=paged([
                [report("r1", "job-basic", "2026-08-04", "2026-08-06T04:00:00Z")],
                [report("r2", "job-basic", "2026-08-05", "2026-08-07T04:00:00Z")],
            ], item_key="reports"))
            mock.get(f"{REPORTING}/media/CSr1", text=basic_csv("2026-08-04", 10))
            mock.get(f"{REPORTING}/media/CSr2", text=basic_csv("2026-08-05", 20))

            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

        landed = rows(pipeline, "SELECT date, video_id, views, _report_id "
                               "FROM raw_youtube.channel_basic_a3 ORDER BY date, video_id")
        assert len(landed) == 4, "both pages of the report list must be walked"
        assert [r[0] for r in landed] == ["2026-08-04", "2026-08-04", "2026-08-05", "2026-08-05"]
        assert [r[3] for r in landed] == ["r1", "r1", "r2", "r2"], "provenance per row"

    def test_the_date_column_stays_text_not_a_utc_instant(self, youtube_spec):
        """`date` is a calendar day in PACIFIC time. Parsing it into a UTC
        timestamp would encode a specific wrong answer 8 hours out, inside a
        primary key, and make every edge-of-day join off by one."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports",
                     json={"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]})
            mock.get(f"{REPORTING}/media/CSr1", text=basic_csv("2026-08-04", 10))
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

        [(date_type, create_type)] = rows(pipeline, """
            SELECT typeof(date), typeof(_report_create_time)
            FROM raw_youtube.channel_basic_a3 LIMIT 1""")
        assert "VARCHAR" in date_type.upper(), date_type
        assert "TIMESTAMP" in create_type.upper(), (
            "the report's own createTime IS an instant and must be typed as one")

    def test_an_empty_key_column_stays_an_empty_string(self, youtube_spec):
        """A NULL inside a merge key is not a key. video_id is legitimately
        blank on channel-level subscriber rows, and `traffic_source_detail` and
        `playback_location_detail` are blank whenever their type dimension is
        not one that populates them."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports",
                     json={"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]})
            mock.get(f"{REPORTING}/media/CSr1", text=basic_csv("2026-08-04", 10))
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

        blank = rows(pipeline, "SELECT video_id, subscribers_gained FROM "
                              "raw_youtube.channel_basic_a3 WHERE video_id = ''")
        assert blank == [("", "4")], (
            "the channel-level subscriber row must land with video_id = '' — not NULL, "
            "not dropped")
        assert rows(pipeline, "SELECT count(*) FROM raw_youtube.channel_basic_a3 "
                             "WHERE video_id IS NULL") == [(0,)]

    def test_columns_are_read_by_header_not_by_position(self, youtube_spec):
        """channel_reach_combined_a1 is the one report in the catalogue whose
        dimensions end `operating_system, device_type` instead of the other way
        round, and `engaged_views` was inserted BEFORE `views` in the a3 bump.
        Every value involved is a small integer, so a positional parser
        transposes them and never errors."""
        header = ("date,channel_id,video_id,traffic_source_type,traffic_source_detail,"
                  "operating_system,device_type,video_thumbnail_impressions,"
                  "video_thumbnail_impressions_ctr")
        body = f"{header}\n2026-08-04,{CHANNEL_ID},vid001,1,,19,102,900,0.05\n"
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_reach_combined_a1"}]})
            mock.get(f"{REPORTING}/jobs/j/reports",
                     json={"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]})
            mock.get(f"{REPORTING}/media/CSr1", text=body)
            pipeline = load(runtime.build_source(
                youtube_spec, selected=["channel_reach_combined_a1"]))

        assert rows(pipeline, "SELECT operating_system, device_type, traffic_source_detail "
                             "FROM raw_youtube.channel_reach_combined_a1") == [("19", "102", "")]

    def test_a_restated_day_replaces_the_original_instead_of_landing_beside_it(self, youtube_spec):
        """Google re-issues a corrected day as a NEW report id carrying the SAME
        startTime/endTime, and documents the rule: "if two new reports have the
        same startTime and endTime property values, only import the report with
        the newer createTime value". This is also the case Fivetran gets wrong —
        it keys report tables on a hash of the row's values, so the revision
        lands as an extra row."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports", json={"reports": [
                report("stale", "j", "2026-08-04", "2026-08-06T04:00:00Z"),
                report("fresh", "j", "2026-08-04", "2026-08-07T04:00:00Z"),
            ]})
            mock.get(f"{REPORTING}/media/CSstale", text=basic_csv("2026-08-04", 10))
            mock.get(f"{REPORTING}/media/CSfresh", text=basic_csv("2026-08-04", 99))
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

            downloaded = [r.url for r in mock.request_history if "/media/" in r.url]
            assert not any("CSstale" in url for url in downloaded), \
                "the superseded report must not even be downloaded"

        assert rows(pipeline, "SELECT views FROM raw_youtube.channel_basic_a3 "
                             "WHERE video_id = 'vid001'") == [("99",)], \
            "one row per key, carrying the restated value"

    def test_the_watermark_advances_and_bounds_the_next_run(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports",
                     json={"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]})
            mock.get(f"{REPORTING}/media/CSr1", text=basic_csv("2026-08-04", 10))
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

            state = resource_state(pipeline, "channel_basic_a3")
            assert state["created_after"] == "2026-08-06T04:00:00Z"
            assert state["seen_report_ids"] == ["r1"]

            first_listing = next(r for r in mock.request_history if "/reports" in r.url)
            assert "createdafter" not in first_listing.qs, \
                "a first run has no watermark and must list everything the job has"

        # Second run, same pipeline state.
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports", json={"reports": []})
            youtube._SESSIONS.clear()
            youtube._JOB_IDS.clear()
            load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

            listing = next(r for r in mock.request_history if "/reports" in r.url)
            assert listing.qs["createdafter"] == ["2026-08-06t04:00:00z"], listing.qs

    def test_an_already_seen_report_id_is_not_downloaded_twice(self, youtube_spec):
        """`createdAfter` is a timestamp, so two reports created in the same
        second can straddle it. Google's documented safeguard is the id set."""
        listing = {"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]}
        for attempt in range(2):
            with rm_module.Mocker() as mock:
                oauth(mock)
                mock.get(f"{REPORTING}/jobs",
                         json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
                mock.get(f"{REPORTING}/jobs/j/reports", json=listing)
                mock.get(f"{REPORTING}/media/CSr1", text=basic_csv("2026-08-04", 10))
                youtube._SESSIONS.clear()
                youtube._JOB_IDS.clear()
                load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))
                downloads = [r for r in mock.request_history if "/media/" in r.url]
                assert len(downloads) == (1 if attempt == 0 else 0), attempt

    def test_a_header_only_csv_is_a_healthy_day_not_a_failure(self, youtube_spec):
        """YouTube "does generate downloadable reports for days on which no data
        was available. Those reports will contain a header row but won't contain
        additional data." The watermark must still advance, or that day is
        re-downloaded forever."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs",
                     json={"jobs": [{"id": "j", "reportTypeId": "channel_basic_a3"}]})
            mock.get(f"{REPORTING}/jobs/j/reports",
                     json={"reports": [report("r1", "j", "2026-08-04", "2026-08-06T04:00:00Z")]})
            mock.get(f"{REPORTING}/media/CSr1", text=BASIC_HEADER + "\n")
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))
            assert resource_state(pipeline, "channel_basic_a3")["created_after"] == \
                "2026-08-06T04:00:00Z"

    def test_a_missing_job_is_created_once_and_then_reused(self, youtube_spec):
        """Create-then-reuse. A second job for a type that already has one does
        not widen history — it is measured from the first job either way — it
        just produces a duplicate stream of reports."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs", json={"jobs": []})
            created = mock.post(f"{REPORTING}/jobs",
                                json={"id": "new-job", "reportTypeId": "channel_basic_a3"})
            mock.get(f"{REPORTING}/jobs/new-job/reports", json={"reports": []})
            load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))

            assert created.call_count == 1
            assert created.last_request.json()["reportTypeId"] == "channel_basic_a3"

        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{REPORTING}/jobs", json={"jobs": [
                {"id": "existing", "reportTypeId": "channel_basic_a3"}]})
            never = mock.post(f"{REPORTING}/jobs", json={"id": "should-not-happen"})
            mock.get(f"{REPORTING}/jobs/existing/reports", json={"reports": []})
            youtube._SESSIONS.clear()
            youtube._JOB_IDS.clear()
            load(runtime.build_source(youtube_spec, selected=["channel_basic_a3"]))
            assert never.call_count == 0

    def test_the_job_list_is_read_once_for_two_report_resources(self, youtube_spec):
        """Twenty report resources each re-listing jobs is twenty requests
        restating something that cannot change mid-run."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            listed = mock.get(f"{REPORTING}/jobs", json={"jobs": [
                {"id": "j1", "reportTypeId": "channel_basic_a3"},
                {"id": "j2", "reportTypeId": "channel_cards_a1"},
            ]})
            mock.get(f"{REPORTING}/jobs/j1/reports", json={"reports": []})
            mock.get(f"{REPORTING}/jobs/j2/reports", json={"reports": []})
            load(runtime.build_source(
                youtube_spec, selected=["channel_basic_a3", "channel_cards_a1"]))
            assert listed.call_count == 2, (
                "one listing per report type is the cache miss; the point is that it "
                "is not one per PAGE or per report")

    def test_reports_carry_no_tombstone_column(self, youtube_spec):
        """A report row is never deleted upstream — a day is restated, which the
        composite key merges. Google also documents that a regenerated report
        legitimately contains FEWER videos than the one it replaces, so absence
        here carries no deletion signal at all."""
        for resource in youtube_spec.resources:
            if resource.strategy == "search_window":
                assert resource.soft_delete is None, resource.name


# ── Metadata: the Data API crawls ────────────────────────────────────────────


class TestMetadata:
    def test_the_documented_enumeration_path_hydrates_every_video(self, youtube_spec):
        """channels.list -> relatedPlaylists.uploads -> playlistItems.list ->
        videos.list?id=. Two pages of playlist items and two id batches, so both
        the paginator and the client-side batching are exercised."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlistItems", json=paged([
                [{"contentDetails": {"videoId": f"vid{n:03d}"}} for n in range(1, 4)],
                [{"contentDetails": {"videoId": "vid004"}}],
            ]))

            def videos(request, _context):
                ids = request.qs["id"][0].split(",")
                return {"items": [video_resource(int(v.removeprefix("vid"))) for v in ids]}
            mock.get(f"{DATA_API}/videos", json=videos)

            pipeline = load(runtime.build_source(youtube_spec, selected=["video"]))

            hydrations = [r for r in mock.request_history if "/videos" in r.url]
            assert len(hydrations) == 1, "4 ids fit in one batch of 50"
            assert "search" not in " ".join(r.url for r in mock.request_history), (
                "search.list is capped at 100 CALLS/day project-wide and 500 videos per "
                "channel, so it must never be on the enumeration path")

        landed = rows(pipeline, "SELECT id, snippet_title, statistics_view_count, "
                               "privacy_status, upload_status, content_details_duration, "
                               "player_embed_height, _deleted "
                               "FROM raw_youtube.video ORDER BY id")
        assert [r[0] for r in landed] == ["vid001", "vid002", "vid003", "vid004"], \
            "both pages of the uploads playlist must be walked"
        assert landed[0][1] == "Video 1"
        # Text, not a number, and this is the API's doing rather than a choice
        # here: `statistics.viewCount` is a JSON STRING ("100") in the Data API
        # response, so it lands as text — which is also why Fivetran types these
        # columns String. Asserted so a later "helpful" cast is a visible
        # decision rather than a silent type change under everything downstream.
        assert landed[0][2] == "100"
        assert landed[0][3:7] == ("public", "processed", "PT10M1S", 360)
        assert all(r[7] is False for r in landed), "soft_delete: always means a tombstone column"

    def test_a_failed_hydration_batch_fails_the_run_rather_than_tombstoning_50_videos(
            self, youtube_spec):
        """`video` is soft_delete: always, so a batch quietly skipped is 50
        videos absent from the load — and the next reconcile run tombstones every
        one of them as deleted upstream. max_deleted_fraction wouldn't catch it
        either: 50 of a 500-video catalogue is 10%. A failed run is correct."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlistItems",
                     json={"items": [{"contentDetails": {"videoId": "vid001"}}]})
            mock.get(f"{DATA_API}/videos", status_code=403, json={"error": {}})
            with pytest.raises(Exception, match="403"):
                load(runtime.build_source(youtube_spec, selected=["video"]))

    def test_fivetran_column_names_are_reproduced(self, youtube_spec):
        """The point of the connector: the same column names a Fivetran
        destination would have. Spot-checked against the names that are easy to
        get wrong — the two status fields Fivetran drops the prefix from, and
        the nested parts it flattens."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlistItems",
                     json={"items": [{"contentDetails": {"videoId": "vid001"}}]})
            mock.get(f"{DATA_API}/videos", json={"items": [video_resource(1)]})
            pipeline = load(runtime.build_source(youtube_spec, selected=["video"]))

        columns = {r[0] for r in rows(pipeline, """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'raw_youtube' AND table_name = 'video'""")}
        for expected in (
            "snippet_channel_id", "snippet_title", "snippet_published_at",
            "snippet_default_audio_language", "snippet_live_broadcast_content",
            "statistics_view_count", "statistics_comment_count",
            "content_details_duration", "content_details_licensed_content",
            "content_details_region_restriction",
            # Fivetran drops the `status_` prefix on exactly these two.
            "privacy_status", "upload_status",
            "status_made_for_kids", "status_self_declared_made_for_kids",
            "player_embed_html", "player_embed_height", "player_embed_width",
        ):
            assert expected in columns, f"{expected} missing; have {sorted(columns)}"

    def test_a_promoted_nested_timestamp_is_typed_as_one(self, youtube_spec):
        """snippet_published_at is dug out of a nested path, so
        `timestamp_columns` could never name it — only the hint makes it a
        timestamp rather than text."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel"]))

        [(published_type,)] = rows(
            pipeline, "SELECT typeof(snippet_published_at) FROM raw_youtube.channel")
        assert "TIMESTAMP" in published_type.upper(), published_type

    def test_the_nested_part_survives_whole_beside_the_promoted_columns(self, youtube_spec):
        """`promote` digs into the record rather than consuming it, so each part
        also lands JSON-stringified under its own name. That is how a field
        Google adds tomorrow reaches the warehouse without a spec edit — and
        five of Fivetran's own columns are exactly that passthrough."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            pipeline = load(runtime.build_source(youtube_spec, selected=["channel"]))

        [(branding, topics)] = rows(
            pipeline, "SELECT branding_settings, topic_details FROM raw_youtube.channel")
        assert json.loads(branding)["channel"]["title"] == "Test Channel"
        assert "Music" in json.loads(topics)["topicCategories"][0]

    def test_the_channel_is_read_once_for_the_resources_that_need_it(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            called = mock.get(f"{DATA_API}/channels", json={"items": [channel_resource()]})
            mock.get(f"{DATA_API}/playlists", json={"items": []})
            load(runtime.build_source(youtube_spec, selected=["channel", "playlist"]))
            assert called.call_count == 1

    def test_an_empty_channel_list_refuses_rather_than_loading_nothing(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock.get(f"{DATA_API}/channels", json={"items": []})
            with pytest.raises(Exception, match="no channel"):
                load(runtime.build_source(youtube_spec, selected=["channel"]))

    def test_a_paginator_handed_a_token_and_no_rows_stops(self, youtube_spec):
        """The Pylon failure, ruled out here rather than discovered in
        production: a listing that reports another page while carrying no data
        would otherwise loop forever."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlists",
                     json={"items": [], "nextPageToken": "forever"})
            load(runtime.build_source(youtube_spec, selected=["playlist"]))


# ── Comments ─────────────────────────────────────────────────────────────────


def thread(thread_id, reply_count, embedded_replies=(), video_id="vid001"):
    return {
        "id": thread_id,
        "snippet": {
            "channelId": CHANNEL_ID, "videoId": video_id, "canReply": True,
            "isPublic": True, "totalReplyCount": reply_count,
            "topLevelComment": {
                "id": f"{thread_id}-top", "etag": "e",
                "snippet": {"channelId": CHANNEL_ID, "videoId": video_id,
                            "textDisplay": "top", "textOriginal": "top",
                            "authorDisplayName": "A", "authorChannelId": {"value": "UCauthor"},
                            "authorChannelUrl": "https://youtube.com/@a",
                            "canRate": True, "likeCount": 3,
                            "publishedAt": "2026-08-01T00:00:00Z",
                            "updatedAt": "2026-08-02T00:00:00Z"},
            },
        },
        "replies": {"comments": list(embedded_replies)},
    }


def reply(reply_id, parent_id):
    return {
        "id": reply_id, "etag": "e",
        # A reply's own snippet has no videoId — the parent scopes it.
        "snippet": {"channelId": CHANNEL_ID, "parentId": parent_id,
                    "textDisplay": "r", "textOriginal": "r",
                    "authorDisplayName": "B", "authorChannelId": {"value": "UCb"},
                    "canRate": True, "likeCount": 0,
                    "publishedAt": "2026-08-03T00:00:00Z",
                    "updatedAt": "2026-08-03T00:00:00Z"},
    }


class TestComments:
    def test_threads_and_replies_land_in_one_table_the_way_fivetran_shapes_it(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/commentThreads", json=paged([
                [thread("t1", 1, [reply("t1-r1", "t1-top")])],
                [thread("t2", 0)],
            ]))
            pipeline = load(runtime.build_source(youtube_spec, selected=["comment"]))

        landed = rows(pipeline, "SELECT id, snippet_parent_id, total_reply_count, can_reply, "
                               "video_id FROM raw_youtube.comment ORDER BY id")
        assert [r[0] for r in landed] == ["t1-r1", "t1-top", "t2-top"], \
            "both pages, and the reply in the same table as its parent"
        by_id = {r[0]: r for r in landed}
        assert by_id["t1-top"][1] is None, "a top-level comment has no parent"
        assert by_id["t1-r1"][1] == "t1-top", "a reply is identified by snippet_parent_id"
        assert by_id["t1-top"][2] == 1 and by_id["t1-top"][3] is True, \
            "thread-level fields belong on the top-level comment"
        assert by_id["t1-r1"][2] is None, "...and not on the reply"
        assert by_id["t1-r1"][4] == "vid001", (
            "a reply's own snippet carries no videoId, so the parent's is copied on — a "
            "foreign key nothing populates is a foreign key nobody can join")

    def test_a_thread_with_more_replies_than_part_replies_returned_is_topped_up(self, youtube_spec):
        """part=replies is documented as incomplete: "a commentThread resource
        does not necessarily contain all replies to a comment, and you need to
        use the comments.list method if you want to retrieve all replies"."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/commentThreads",
                     json={"items": [thread("t1", 7, [reply("t1-r1", "t1-top")])]})
            mock.get(f"{DATA_API}/comments", json=paged([
                [reply(f"t1-r{n}", "t1-top") for n in range(1, 6)],
                [reply(f"t1-r{n}", "t1-top") for n in range(6, 8)],
            ]))
            pipeline = load(runtime.build_source(youtube_spec, selected=["comment"]))

        assert rows(pipeline, "SELECT count(*) FROM raw_youtube.comment "
                             "WHERE snippet_parent_id = 't1-top'") == [(7,)]

    def test_an_exhausted_budget_saves_a_page_token_and_resumes(self, youtube_spec):
        """Bounded but resumable, not early-exiting. `order=time` puts recent
        activity first, but what `time` orders by is not documented precisely
        enough to stop on — stopping on that guess would drop comments
        silently and permanently."""
        pages = [[thread(f"t{n}", 0)] for n in range(1, 8)]
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/commentThreads", json=paged(pages))
            # 3 units, so 3 listing pages and then it must stop and remember.
            resource = youtube_spec.resource("comment")
            resource._entry["incremental"] = dict(resource.incremental, unit_budget=3)
            pipeline = load(runtime.build_source(youtube_spec, selected=["comment"]))

            assert resource_state(pipeline, "comment")["page_token"] == "3", \
                "the token where the budget ran out must be persisted"
        assert rows(pipeline, "SELECT count(*) FROM raw_youtube.comment") == [(3,)]

        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/commentThreads", json=paged(pages))
            youtube._SESSIONS.clear()
            youtube._CHANNELS.clear()
            pipeline = load(runtime.build_source(youtube_spec, selected=["comment"]))
            first = next(r for r in mock.request_history if "commentThreads" in r.url)
            assert first.qs["pagetoken"] == ["3"], "the next run resumes where it stopped"
        assert rows(pipeline, "SELECT count(*) FROM raw_youtube.comment") == [(6,)]

    def test_a_completed_pass_clears_the_token_so_the_next_run_starts_over(self, youtube_spec):
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/commentThreads", json={"items": [thread("t1", 0)]})
            pipeline = load(runtime.build_source(youtube_spec, selected=["comment"]))
            assert resource_state(pipeline, "comment")["page_token"] is None

    def test_comments_carry_no_tombstone_column(self, youtube_spec):
        """A budget-bounded pass has not seen most of the table, so absence
        means "not reached yet" — tombstoning on it would wipe the table."""
        assert youtube_spec.resource("comment").soft_delete is None
        assert youtube_spec.resource("caption").soft_delete is None


# ── Captions ─────────────────────────────────────────────────────────────────

VTT = """WEBVTT
Kind: captions
Language: en

00:00:00.000 --> 00:00:02.500
Hello there

2
00:00:02.500 --> 00:00:05.000
second cue
spanning two lines
"""


def caption_track(track_id, language="en", last_updated="2026-07-01T00:00:00Z"):
    return {"id": track_id, "snippet": {"language": language, "trackKind": "standard",
                                        "lastUpdated": last_updated}}


class TestCaptions:
    def _mock_one_video(self, mock):
        oauth(mock)
        mock_channel(mock)
        mock.get(f"{DATA_API}/playlistItems",
                 json={"items": [{"contentDetails": {"videoId": "vid001"}}]})

    def test_cues_land_at_fivetrans_grain(self, youtube_spec):
        """One row per (video, track language, cue start) — not one row per
        track. captions.list returns track METADATA; the cues only exist in the
        downloaded track."""
        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={"items": [caption_track("cap1")]})
            mock.get(f"{DATA_API}/captions/cap1", text=VTT)
            pipeline = load(runtime.build_source(youtube_spec, selected=["caption"]))

        landed = rows(pipeline, "SELECT video_id, languages, start, duration, text "
                               "FROM raw_youtube.caption ORDER BY start")
        assert landed == [
            ("vid001", "en", 0.0, 2.5, "Hello there"),
            ("vid001", "en", 2.5, 2.5, "second cue\nspanning two lines"),
        ]

    def test_the_budget_stops_it_and_progress_is_persisted(self, youtube_spec):
        """250 units is one video per run by construction: captions.list is 50
        and captions.download is 200, against a 10,000/day pool."""
        with rm_module.Mocker() as mock:
            oauth(mock)
            mock_channel(mock)
            mock.get(f"{DATA_API}/playlistItems", json={
                "items": [{"contentDetails": {"videoId": f"vid{n:03d}"}} for n in (1, 2, 3)]})
            mock.get(f"{DATA_API}/captions", json={"items": [caption_track("cap1")]})
            mock.get(f"{DATA_API}/captions/cap1", text=VTT)
            pipeline = load(runtime.build_source(youtube_spec, selected=["caption"]))

            # requests_mock lowercases query-string keys, so this is `videoid`.
            listed = [r for r in mock.request_history
                      if r.path.endswith("/captions") and "videoid" in r.qs]
            assert len(listed) == 1, (
                f"250 units buys exactly one video (50 list + 200 download); got "
                f"{[r.url for r in listed]}")
        covered = resource_state(pipeline, "caption")["covered"]
        assert list(covered) == ["vid001"], covered

    def test_a_video_with_no_captions_is_still_recorded_as_covered(self, youtube_spec):
        """Otherwise it is "never crawled" forever and the worklist never moves
        past it — which at one video per run stalls the whole crawl."""
        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={"items": []})
            pipeline = load(runtime.build_source(youtube_spec, selected=["caption"]))
            assert "vid001" in resource_state(pipeline, "caption")["covered"]

    def test_an_unchanged_track_is_not_re_downloaded(self, youtube_spec):
        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={"items": [caption_track("cap1")]})
            downloaded = mock.get(f"{DATA_API}/captions/cap1", text=VTT)
            load(runtime.build_source(youtube_spec, selected=["caption"]))
            assert downloaded.call_count == 1

        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={"items": [caption_track("cap1")]})
            again = mock.get(f"{DATA_API}/captions/cap1", text=VTT)
            youtube._SESSIONS.clear()
            youtube._CHANNELS.clear()
            youtube._VIDEO_IDS.clear()
            load(runtime.build_source(youtube_spec, selected=["caption"]))
            assert again.call_count == 0, \
                "snippet.lastUpdated has not moved, so there is nothing to re-read"

    def test_a_video_whose_tracks_outrun_the_budget_is_not_marked_covered(self, youtube_spec):
        """250 units buys exactly one download, so a two-track video gets its
        first track and stops. Recording it as covered there would mean the
        second track is never fetched — silently, forever, because `covered` is
        what decides the worklist."""
        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={
                "items": [caption_track("cap-en", "en"), caption_track("cap-fr", "fr")]})
            mock.get(f"{DATA_API}/captions/cap-en", text=VTT)
            mock.get(f"{DATA_API}/captions/cap-fr", text=VTT)
            pipeline = load(runtime.build_source(youtube_spec, selected=["caption"]))
            assert resource_state(pipeline, "caption")["covered"] == {}, (
                "one of two tracks downloaded is not coverage")
        assert rows(pipeline, "SELECT count(*) FROM raw_youtube.caption") == [(2,)], \
            "the cues it did manage to read still land"

    def test_an_undownloadable_track_is_skipped_not_fatal(self, youtube_spec):
        """Third-party contributions and disabled captions 403. That is one
        track, not a broken run — and this resource holds the pool slot for 24
        others."""
        with rm_module.Mocker() as mock:
            self._mock_one_video(mock)
            mock.get(f"{DATA_API}/captions", json={"items": [caption_track("cap1")]})
            mock.get(f"{DATA_API}/captions/cap1", status_code=403, json={"error": {}})
            pipeline = load(runtime.build_source(youtube_spec, selected=["caption"]))
            assert "vid001" in resource_state(pipeline, "caption")["covered"]


class TestVttParsing:
    def test_the_optional_hours_field_is_handled(self):
        assert youtube._parse_vtt(
            "WEBVTT\n\n01:02.500 --> 01:04.000\nshort form\n"
        ) == [(62.5, 1.5, "short form")]

    def test_headers_notes_and_style_blocks_are_not_cues(self):
        body = ("WEBVTT\n\nNOTE this is a comment\n\nSTYLE\n::cue { color: red }\n\n"
                "00:00:01.000 --> 00:00:02.000\nreal\n")
        assert youtube._parse_vtt(body) == [(1.0, 1.0, "real")]

    def test_a_cue_with_no_text_is_dropped(self):
        assert youtube._parse_vtt("WEBVTT\n\n00:00:01.000 --> 00:00:02.000\n") == []

    def test_comma_decimal_separators_parse(self):
        assert youtube._parse_vtt(
            "WEBVTT\n\n00:00:01,250 --> 00:00:02,750\nx\n") == [(1.25, 1.5, "x")]


class TestUnitBudget:
    def test_it_charges_before_the_request_not_after_success(self):
        """Google charges for failures too: "All API requests, including invalid
        requests, incur at least a one-point quota cost"."""
        budget = youtube._UnitBudget(2)
        assert budget.spend(1) and budget.spend(1)
        assert not budget.spend(1)
        assert budget.spent == 2

    def test_zero_means_unbounded(self):
        budget = youtube._UnitBudget(0)
        assert budget.spend(100_000)


class TestReportKeysMatchTheDocumentedDimensions:
    """The keys were taken from Google's channel_reports page and diffed against
    it column by column. Pinned here so a later edit cannot quietly drop one —
    a missing key column does not error, it silently merges two different rows
    into one.
    """

    EXPECTED: typing.ClassVar = {
        "channel_basic_a3": 6, "channel_combined_a3": 10, "channel_province_a3": 7,
        "channel_device_os_a3": 8, "channel_traffic_source_a3": 8,
        "channel_playback_location_a3": 8, "channel_demographics_a1": 8,
        "channel_sharing_service_a2": 7, "channel_subtitles_a3": 8,
        "channel_annotations_a2": 8, "channel_cards_a1": 8, "channel_end_screens_a2": 8,
        "channel_reach_basic_a1": 3, "channel_reach_combined_a1": 7,
        "playlist_basic_a2": 7, "playlist_combined_a2": 11, "playlist_province_a2": 8,
        "playlist_device_os_a2": 9, "playlist_traffic_source_a2": 9,
        "playlist_playback_location_a2": 9,
    }

    def test_every_report_type_is_current(self, youtube_spec):
        """The channel_* reports moved a2 -> a3 and playlist_* a1 -> a2
        effective 2025-06-30; the predecessors were deprecated on 2025-10-31. A
        job created for a retired report id produces no report, ever, and
        nothing errors."""
        declared = {r.incremental["report_type"] for r in youtube_spec.resources
                    if r.strategy == "search_window"}
        assert declared == set(self.EXPECTED)

    def test_the_table_name_is_the_report_it_fetches(self, youtube_spec):
        """Deliberately unlike Fivetran, which freezes table names at an old
        version — "we sync the channel_basic_a3 report to the CHANNEL_BASIC_A2
        table" — and whose docs, ERD and dbt package spell the same table three
        different ways."""
        for resource in youtube_spec.resources:
            if resource.strategy == "search_window":
                assert resource.name == resource.incremental["report_type"]

    def test_the_key_is_every_dimension_and_date_is_always_one(self, youtube_spec):
        for resource in youtube_spec.resources:
            if resource.strategy != "search_window":
                continue
            key = resource.primary_key
            assert len(key) == self.EXPECTED[resource.name], resource.name
            assert key[0] == "date", resource.name
            assert "channel_id" in key, resource.name
