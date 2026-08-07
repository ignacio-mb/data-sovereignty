"""YouTube: the three things its spec cannot say.

Everything here exists because of a property of the API, not a quirk of one
endpoint — and every resource in `sources/youtube.yml` is delegated to this
module, which is unusual for this repo and deliberate. The reason is the first
item below: the declarative path calls `runtime._auth()`, and no auth kind
`_auth()` builds can express this credential, so nothing here may take that
path. `test_youtube_source.py` asserts that, because a resource quietly
switched to `full_refresh` would fail at `_auth()` with a message about auth
types rather than about the strategy.

  the credential   Every Reporting API method needs OAuth 2.0 user-delegated
                   consent. Google closes both doors that would make it a
                   static token: a service account cannot be used at all
                   ("there is no way to link a Service Account to a YouTube
                   account" -> NoLinkedYouTubeAccount) and the device flow is
                   unsupported. So the credential is a refresh token, minted
                   once interactively by a channel owner, exchanged for a
                   one-hour access token here. That is three secrets against
                   `api.auth`'s single `token_env`, which is why the spec's
                   auth type is one `_auth()` refuses.

  reports are a queue, not an endpoint
                   You create a reporting JOB per report type, then poll a list
                   of generated CSVs ordered by creation time, dedupe
                   restatements, download each survivor, and parse it by header.
                   The cursor is `createdAfter` over report creation time — not
                   over the data's own dates — which is the whole reason an
                   hourly schedule is worth anything against daily data: a
                   restated day arrives as a NEW report id carrying an OLD
                   start/end time, and only a creation-time cursor sees it.

  quota is the bound
                   `comment` and `caption` are limited by Data API UNITS, not
                   by a cursor. 10,000 units/day per Cloud project, and
                   captions alone cost 250 per video — so these two resources
                   spend a declared per-run budget, persist where they stopped,
                   and converge over days. A cursor cannot express that, and
                   ignoring it means a connector that exhausts the project's
                   quota in one run and then fails every other resource too.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import time

import dlt

from ..runtime import column_hints, make_transformer, paced_session

log = logging.getLogger(__name__)

# (connect, read) seconds. Explicit for the same reason Swoogo's and Lever's
# are: `requests` defaults to no timeout at all, and a resource holding this
# source's pool slot of one while it waits forever is a stopped pipeline, not a
# slow one.
_TIMEOUT = (10, 60)
# Report CSVs are whole days of per-video rows and can be tens of MB, so the
# read half is generous. Still finite.
_DOWNLOAD_TIMEOUT = (10, 300)

# Access tokens live ~1 hour ("User access tokens ... automatically expire after
# one hour"), but the response carries `expires_in` and Google's own sample
# returns 3920 rather than 3600 — so the real value is read, never assumed, and
# this is the margin at which it is renewed rather than risked mid-request.
_TOKEN_REFRESH_MARGIN_SECONDS = 300

# Documented Data API v3 quota costs, in units. A `list` is 1 unit for every
# endpoint this connector touches EXCEPT captions, and note that Google charges
# for failures too: "All API requests, including invalid requests, incur at
# least a one-point quota cost." So the budget is charged before the request,
# not after a successful one.
_UNITS_LIST = 1
_UNITS_CAPTIONS_LIST = 50
_UNITS_CAPTIONS_DOWNLOAD = 200

# Google's documented dedup safeguard for the report queue, alongside the
# creation-time cursor: "keep a set of report ids you have already processed".
# Bounded, because resource state is persisted with the pipeline and an
# unbounded set would grow forever — 2000 covers far more than the 60 days of
# reports any single job can have outstanding.
_SEEN_REPORT_IDS_KEPT = 2000

# Statuses that mean "this particular thing is not readable", not "the run is
# broken". A caption track can be undownloadable (third-party or disabled) and
# a video can vanish between enumeration and hydration; failing the whole run
# over either would stop 24 other resources for one missing row.
_SKIP_STATUSES = (403, 404, 410)


# ── Auth ─────────────────────────────────────────────────────────────────────


class _GoogleOAuth:
    """A `requests` auth callable that keeps a Google access token fresh.

    An auth callable rather than a header set once on the session, because a
    run can outlive a token: the 30-day first load downloads ~600 report CSVs,
    and a token minted at the start would expire partway through. This way
    every request carries a token checked for expiry immediately before it is
    sent, and the refresh is one POST rather than a failed request and a retry.

    Not built through `runtime._auth()`: that returns a config dict shaped for
    dlt's declarative REST client (or, for oauth2_client_credentials, a dlt
    auth object for the *client-credentials* grant, which is the flow these
    APIs explicitly reject). Both Swoogo's and Lever's extensions read their
    own credential for the same reason.
    """

    def __init__(self, spec, session_factory):
        auth = spec.api["auth"]
        if auth.get("type") != "oauth2_refresh_token":
            raise RuntimeError(
                f"{spec.name}: this extension implements the oauth2_refresh_token "
                f"grant, but the spec declares auth type {auth.get('type')!r}. "
                f"Either fix the spec or use a source whose auth `_auth()` builds."
            )
        self._token_url = auth.get("token_url") or "https://oauth2.googleapis.com/token"
        self._client_id = _required_env(spec, auth, "client_id_env")
        self._client_secret = _required_env(spec, auth, "client_secret_env")
        self._refresh_token = _required_env(spec, auth, "token_env")
        # A plain session, deliberately unpaced and unauthed: minting a token is
        # not a data request, and routing it through the paced session would
        # make the auth callable re-enter itself.
        self._session_factory = session_factory
        self._access_token = None
        self._expires_at = 0.0

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self._token()}"
        return request

    def _token(self):
        if self._access_token and time.monotonic() < self._expires_at:
            return self._access_token
        response = self._session_factory().post(
            self._token_url,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=_TIMEOUT,
        )
        if response.status_code == 400:
            # invalid_grant is terminal, not transient: the refresh token has
            # been revoked, has gone six months unused, or the OAuth consent
            # screen is still in "Testing" (which expires refresh tokens after
            # 7 days). Retrying cannot fix any of those, and a retry loop that
            # mints tokens can silently evict the live one — Google keeps at
            # most 100 refresh tokens per account per client and drops the
            # oldest without warning.
            raise RuntimeError(
                "youtube: Google refused the refresh token (400). This is not "
                "retryable — re-run scripts/youtube_oauth_setup.py to mint a new "
                "one, and check the OAuth consent screen is published ('In "
                "production'), not 'Testing', which expires tokens after 7 days."
            )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        # Read, never assumed — see _TOKEN_REFRESH_MARGIN_SECONDS.
        lifetime = int(payload.get("expires_in") or 3600)
        self._expires_at = time.monotonic() + max(lifetime - _TOKEN_REFRESH_MARGIN_SECONDS, 30)
        log.info("youtube: minted an access token, valid %ss", lifetime)
        return self._access_token


def _required_env(spec, auth, key):
    """The value of the env var `auth[key]` names, or a message saying which."""
    name = auth.get(key)
    if not name:
        raise RuntimeError(f"{spec.name}: api.auth is missing `{key}`.")
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{spec.name}: {name} is not set. Add it to .env (never to the spec).")
    return value


def _plain_session():
    from dlt.sources.helpers.requests.retry import Client

    return Client(raise_for_status=False).session


def _authed_session(spec, paced):
    """dlt's retrying client, paced if the run supplied a pacer, Google-authed.

    Cached per source for the length of the process — an Airflow task is a
    fresh process per run, so this is run-scoped. Twenty-five resources each
    building their own session would each mint their own token, and each of
    those spends from the same 100-refresh-tokens-per-client ceiling.
    """
    cached = _SESSIONS.get(spec.name)
    if cached is not None:
        return cached
    session = paced_session(spec, paced) if paced is not None else _plain_session()
    session.auth = _GoogleOAuth(spec, _plain_session)
    _SESSIONS[spec.name] = session
    return session


_SESSIONS = {}


def _get(session, url, params=None, timeout=_TIMEOUT, skip_statuses=()):
    """One GET, raising on anything but the skippable statuses.

    Returns None for a skipped status so the caller can carry on; every other
    error raises, because a run that silently drops a page looks exactly like a
    channel with less data.
    """
    response = session.get(url, params=params or {}, timeout=timeout)
    if response.status_code in skip_statuses:
        log.warning("youtube: %s -> %s, skipping", url, response.status_code)
        return None
    response.raise_for_status()
    return response


# ── Report tables (the 20 search_window resources) ───────────────────────────


def build_resource(spec, resource, paced=None):
    """One bulk-report table.

    Generic across all twenty: the only thing that differs between them is
    `incremental.report_type` and the primary key, both of which are in the
    spec — the same way Swoogo's one function serves its twelve per-event
    endpoints. A twenty-first report type needs no Python.

    Deliberately NOT a `build_<name>` per report: twenty near-identical
    functions is twenty places for one fix to be applied nineteen times.
    """
    if resource.strategy != "search_window":
        raise RuntimeError(
            f"{spec.name}.{resource.name}: build_resource here serves the report "
            f"queue (strategy search_window), got {resource.strategy!r}. The five "
            f"metadata resources have their own build_<name> functions."
        )
    report_type = resource.incremental.get("report_type")
    if not report_type:
        raise RuntimeError(
            f"{spec.name}.{resource.name}: needs `incremental.report_type` — the "
            f"YouTube report type id whose job this resource reads."
        )
    transform = make_transformer(resource)

    decorate = dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        # Report rows are already flat, so this changes nothing for them — kept
        # for the same reason every other resource here has it: nothing about a
        # new column upstream should be able to mint a child table.
        max_table_nesting=0,
    )

    @decorate
    def report_rows():
        # A hand-rolled watermark rather than dlt.sources.incremental(), for
        # Lever's reason and one more. Lever's: the value that must advance is
        # "how far this run's WORKLIST reached", and a report that is generated
        # for a day with no data is a header-only CSV — YouTube "does generate
        # downloadable reports for days on which no data was available" — so a
        # cursor bound to yielded ROWS would never advance past a quiet day and
        # would re-download it forever. The extra reason: dlt's incremental
        # would also FILTER rows on the cursor, and every row of a restated
        # report legitimately carries a create_time newer than the rows it
        # replaces while the report's own start_time is old.
        state = dlt.current.resource_state()
        created_after = state.get("created_after")
        seen = set(state.get("seen_report_ids") or ())

        session = _authed_session(spec, paced)
        job_id = _ensure_job(spec, session, report_type)
        reports = _reports_to_load(spec, session, job_id, created_after, seen)
        log.info(
            "%s: %d report(s) to load%s", resource.name, len(reports),
            f" created after {created_after}" if created_after
            else " (no watermark yet — everything the job has, ~30 days)",
        )

        newest = created_after
        loaded_ids = []
        for report in reports:
            for record in _download_rows(spec, session, report):
                yield transform(record)
            newest = _max_str(newest, report.get("createTime"))
            loaded_ids.append(report["id"])

        # Committed only after the whole worklist was walked, and that is not a
        # choice this function gets to make anyway: dlt discards a failed
        # extract along with any state it mutated, so a crash on report N
        # rewinds to the watermark this run started from and the next run
        # re-reads the queue from there. Which is safe rather than merely
        # tolerable — every report merges on its composite key, so
        # re-downloading a day already loaded rewrites identical rows.

        state["created_after"] = newest
        # Newest last, then trimmed from the front: the ids worth remembering
        # are the recent ones, since `createdAfter` already excludes anything
        # older than the watermark.
        state["seen_report_ids"] = (list(state.get("seen_report_ids") or ()) + loaded_ids)[
            -_SEEN_REPORT_IDS_KEPT:
        ]

    return _as_source(spec, report_rows)


def _ensure_job(spec, session, report_type):
    """This report type's job id, creating the job if it has none.

    Create-then-reuse, which is the pattern Fivetran documents ("If a reporting
    job already exists for that report, Fivetran reuses it instead of creating
    a new one") and the only safe one: a second job for a type that already has
    one does not widen history by a day — history is 30 days before the FIRST
    job's creation either way — it just produces a second stream of duplicate
    reports to download.

    Cached per (source, report type) for the process, which is the run.
    """
    cached = _JOB_IDS.get((spec.name, report_type))
    if cached is not None:
        return cached

    base = f"{spec.base_url}/v1/jobs"
    for job in _google_pages(session, base, {}, item_key="jobs"):
        if job.get("reportTypeId") != report_type:
            continue
        # An expired job produces no error and no new reports — it just stops.
        # Logged rather than raised: the reports already generated are still
        # worth loading, and a hard failure here would take down 19 healthy
        # report tables over one job that needs recreating.
        if job.get("expireTime"):
            log.warning("%s: reporting job %s expires at %s — no reports are "
                        "generated after that", report_type, job["id"], job["expireTime"])
        _JOB_IDS[(spec.name, report_type)] = job["id"]
        return job["id"]

    response = session.post(
        base, json={"reportTypeId": report_type, "name": f"data-sovereignty {report_type}"},
        timeout=_TIMEOUT,
    )
    response.raise_for_status()
    job_id = response.json()["id"]
    # Worth a warning rather than an info: this is the moment the 30-day
    # history floor is set for this report type, permanently, and the first
    # report will not exist for ~48 hours.
    log.warning(
        "%s: created reporting job %s. History for this report now begins 30 days "
        "before today and can never reach further back; the first report becomes "
        "available within 48 hours.", report_type, job_id,
    )
    _JOB_IDS[(spec.name, report_type)] = job_id
    return job_id


_JOB_IDS = {}


def _reports_to_load(spec, session, job_id, created_after, seen):
    """The reports this run should download, oldest createTime first.

    Two filters, both from Google's documented handling:

      restatements  "If YouTube has backfill data, it generates a new report
                    with a new report ID [and] the report's startTime and
                    endTime property values will match ... a report that was
                    previously available", and the rule is "if two new reports
                    have the same startTime and endTime property values, only
                    import the report with the newer createTime value". So a
                    single listing can contain both the original and its
                    replacement, and downloading both would merge the stale one
                    on top of the fresh one depending on order.
      already seen  the id set safeguard. `createdAfter` is a timestamp, so two
                    reports created in the same second can straddle it.
    """
    params = {}
    if created_after:
        params["createdAfter"] = created_after
    listed = list(_google_pages(
        session, f"{spec.base_url}/v1/jobs/{job_id}/reports", params, item_key="reports"))

    newest_per_window = {}
    already_seen = 0
    for report in listed:
        if report.get("id") in seen:
            already_seen += 1
            continue
        window = (report.get("startTime"), report.get("endTime"))
        incumbent = newest_per_window.get(window)
        if incumbent is None or _max_str(
                incumbent.get("createTime"), report.get("createTime")) == report.get("createTime"):
            newest_per_window[window] = report

    # The two are counted separately on purpose. Rolled into one number they
    # read as the same thing, and they mean opposite things when a table looks
    # short: "superseded" is YouTube restating days (healthy), while a large
    # "already seen" means the watermark is not advancing (not healthy).
    superseded = len(listed) - already_seen - len(newest_per_window)
    if superseded or already_seen:
        log.info("job %s: %d listed report(s) superseded by a restatement of the same "
                 "day, %d already processed", job_id, superseded, already_seen)
    # Ascending createTime, so the watermark can advance per report and a crash
    # never skips one. Reports with no createTime sort first and are harmless.
    return sorted(newest_per_window.values(), key=lambda r: r.get("createTime") or "")


def _download_rows(spec, session, report):
    """One report CSV, as dicts, with the report's own provenance attached.

    Parsed BY HEADER, never by position, because Google says so twice: "To
    determine the ordering of the report's columns, use the report's header row"
    and "your application should expect the addition [of] new metrics to any
    report". Two live traps this sidesteps — `engaged_views` was inserted BEFORE
    `views` in the a3 bump, and channel_reach_combined_a1 is the one report in
    the catalogue whose dimensions end `operating_system, device_type` instead
    of `device_type, operating_system`. Both would transpose silently, since
    every value involved is a small integer.

    An empty CSV field stays the EMPTY STRING. This is load-bearing rather than
    incidental: `video_id` is legitimately blank on channel_basic_a3's
    channel-level subscriber rows, and `traffic_source_detail` and
    `playback_location_detail` are blank whenever their type dimension is not
    one that populates them — all three are primary-key columns, and a NULL
    inside a merge key is not a key. `csv.DictReader` already yields '' for a
    blank field; the point is that nothing here "helpfully" converts it.
    """
    url = report.get("downloadUrl")
    if not url:
        log.warning("report %s has no downloadUrl, skipping", report.get("id"))
        return
    # The download needs the same bearer token as the JSON calls — `downloadUrl`
    # is not pre-signed, and `media.download` requires a scope like every other
    # method. Going through `session` is what supplies it.
    response = _get(session, url, {"alt": "media"},
                    timeout=_DOWNLOAD_TIMEOUT, skip_statuses=_SKIP_STATUSES)
    if response is None:
        return

    provenance = {
        "_report_id": report.get("id"),
        "_report_job_id": report.get("jobId"),
        "_report_create_time": report.get("createTime"),
        "_report_start_time": report.get("startTime"),
    }
    rows = 0
    for row in csv.DictReader(io.StringIO(response.text)):
        # A header-only CSV is a normal, healthy day: YouTube "does generate
        # downloadable reports for days on which no data was available".
        # DictReader yields nothing for it, so this loop simply does not run.
        rows += 1
        yield {**row, **provenance}
    log.info("report %s (%s): %d row(s)", report.get("id"), report.get("startTime"), rows)


# ── Metadata tables (the 5 parent_fanout resources) ──────────────────────────
#
# Each has its own build_<name> because each is a genuinely different fetch —
# unlike the twenty reports, which are one fetch parameterised by the spec.


def build_channel(spec, resource, paced=None):
    """The channel itself: one row, one request, 1 unit."""
    transform = make_transformer(resource)

    @_resource_for(resource)
    def channel():
        session = _authed_session(spec, paced)
        yield transform(_channel_record(spec, session))

    return _as_source(spec, channel)


def build_playlist(spec, resource, paced=None):
    """Every playlist on the channel.

    No playlist-to-video bridge table, matching Fivetran, which does not sync
    playlistItems at all — `content_details_item_count` is the only membership
    signal in that schema. Deciding what membership MEANS is modelling, which
    this repo does not do.
    """
    transform = make_transformer(resource)
    endpoint = _endpoint(spec, resource)

    @_resource_for(resource)
    def playlist():
        session = _authed_session(spec, paced)
        channel_id = _channel_record(spec, session)["id"]
        params = dict(endpoint["params"])
        params["channelId"] = channel_id
        params["maxResults"] = endpoint.get("page_size", 50)
        for record in _google_pages(session, endpoint["url"], params):
            yield transform(record)

    return _as_source(spec, playlist)


def build_video(spec, resource, paced=None):
    """Every video on the channel, enumerated the documented way.

    channels.list -> contentDetails.relatedPlaylists.uploads ->
    playlistItems.list (50/page, 1 unit each) -> videos.list?id=<=50 ids to
    hydrate (1 unit each). Google documents exactly this two-step as the way to
    list a channel's uploads.

    Deliberately NOT search.list. It is capped at 100 CALLS per day
    project-wide (a quota that is now a call count, not a unit cost), returns at
    most 500 videos when `channelId` is set, and its `totalResults` is
    documented as an approximation — so it cannot enumerate a real catalogue,
    and burning that budget here would take it away from everything else.

    The full walk every run is a deliberate difference from Fivetran, which
    only refreshes "metadata for the videos that you've uploaded starting one
    month before the last sync date" — a window on UPLOAD date, so an edit to
    an older video's title never reaches its warehouse. At 2 units per 50
    videos the whole catalogue costs ~20 units for 500 videos, which is
    cheaper than the bookkeeping to avoid it, and it is what makes
    `soft_delete: always` honest here.
    """
    transform = make_transformer(resource)
    endpoint = _endpoint(spec, resource)
    batch_size = endpoint.get("page_size", 50)

    @_resource_for(resource)
    def video():
        session = _authed_session(spec, paced)
        ids = _video_ids(spec, session)
        log.info("video: hydrating %d video(s) in batches of %d", len(ids), batch_size)
        for start in range(0, len(ids), batch_size):
            batch = ids[start:start + batch_size]
            # videos.list?id= is NOT paginated — maxResults "is not supported
            # for use in conjunction with the id parameter" — so the id list IS
            # the page and there is no token to follow.
            #
            # NO skip_statuses here, deliberately, unlike the caption and comment
            # crawls. This resource is `soft_delete: always`, so a batch quietly
            # skipped is 50 videos absent from the load — and the next reconcile
            # run tombstones every one of them as deleted upstream.
            # `max_deleted_fraction: 0.5` would not catch it either: 50 of a
            # 500-video catalogue is 10%. A failed run is the correct outcome.
            # (A batch containing ids that no longer exist does not 404; the API
            # returns 200 with fewer items, which the enumeration handles.)
            response = _get(session, endpoint["url"],
                            {**endpoint["params"], "id": ",".join(batch)})
            for record in response.json().get("items") or []:
                yield transform(record)

    return _as_source(spec, video)


def build_comment(spec, resource, paced=None):
    """Every comment on the channel and its videos, flattened like Fivetran's.

    One table for threads and replies both, which is Fivetran's shape: a reply
    is a row whose `snippet_parent_id` is set, and the three thread-level
    columns (`can_reply`, `is_public`, `total_reply_count`) are populated only
    on the top-level comment.

    `allThreadsRelatedToChannelId` gets every thread on the channel AND its
    videos from one paged endpoint at 1 unit per 100 threads. The per-video
    alternative spends 1 unit per video before it has read a single comment.

    Replies are topped up via comments.list because part=replies is documented
    as incomplete: "a commentThread resource does not necessarily contain all
    replies to a comment, and you need to use the comments.list method if you
    want to retrieve all replies".

    Bounded by `incremental.unit_budget` and RESUMABLE rather than
    early-exiting. `order=time` puts recent activity first so a bounded pass
    sees fresh threads first, but what `time` orders by is not documented
    precisely enough to stop on — stopping early on that guess would drop
    comments silently and permanently. Instead the page token where the budget
    ran out is persisted, the next run resumes there, and a completed pass
    starts over.
    """
    transform = make_transformer(resource)
    endpoint = _endpoint(spec, resource)
    budget_units = int(resource.incremental.get("unit_budget") or 0)
    comments_url = endpoint["url"].replace("/commentThreads", "/comments")

    @_resource_for(resource)
    def comment():
        state = dlt.current.resource_state()
        budget = _UnitBudget(budget_units)
        session = _authed_session(spec, paced)
        channel_id = _channel_record(spec, session)["id"]

        params = dict(endpoint["params"])
        params["allThreadsRelatedToChannelId"] = channel_id
        params["maxResults"] = endpoint.get("page_size", 100)
        page_token = state.get("page_token")
        log.info("comment: %s, budget %d units", "resuming at a saved page token"
                 if page_token else "starting a fresh pass", budget_units)

        while True:
            if not budget.spend(_UNITS_LIST):
                # Out of budget mid-pass: remember where to resume. The token
                # is what makes this resumable rather than restarting, which at
                # this budget would mean never reaching the tail.
                state["page_token"] = page_token
                log.info("comment: budget exhausted, will resume next run")
                return
            query = dict(params)
            if page_token:
                query["pageToken"] = page_token
            response = _get(session, endpoint["url"], query, skip_statuses=_SKIP_STATUSES)
            if response is None:
                state["page_token"] = None
                return
            body = response.json()
            for thread in body.get("items") or []:
                yield from (transform(record) for record
                            in _thread_records(session, comments_url, thread, budget))
            page_token = body.get("nextPageToken")
            if not page_token:
                # A completed pass. Cleared rather than left, so the next run
                # starts from page one and picks up new threads.
                state["page_token"] = None
                log.info("comment: completed a full pass, %d units spent", budget.spent)
                return

    return _as_source(spec, comment)


def _thread_records(session, comments_url, thread, budget):
    """One commentThread, as its top-level comment plus every reply."""
    snippet = thread.get("snippet") or {}
    top = snippet.get("topLevelComment")
    if not top:
        return
    reply_count = int(snippet.get("totalReplyCount") or 0)
    # Thread-level fields, set on the top-level comment only — the shape
    # Fivetran lands. Top level, not nested, so dlt snake_cases them straight
    # into can_reply / is_public / total_reply_count.
    yield {
        **top,
        "canReply": snippet.get("canReply"),
        "isPublic": snippet.get("isPublic"),
        "totalReplyCount": reply_count,
    }

    embedded = (thread.get("replies") or {}).get("comments") or []
    video_id = (top.get("snippet") or {}).get("videoId")
    if reply_count > len(embedded):
        replies = _all_replies(session, comments_url, top["id"], budget)
        # Falling back to the embedded replies rather than yielding nothing:
        # running out of budget partway must not make a thread look childless.
        replies = replies if replies is not None else embedded
    else:
        replies = embedded

    for reply in replies:
        # A reply's own snippet has no videoId — the parent scopes it — so the
        # parent's is copied on rather than left null. Same reasoning as Lever
        # stamping opportunityId onto children the API does not echo it to: a
        # foreign key nothing populates is a foreign key nobody can join.
        reply = dict(reply)
        reply_snippet = dict(reply.get("snippet") or {})
        reply_snippet.setdefault("videoId", video_id)
        reply["snippet"] = reply_snippet
        yield reply


def _all_replies(session, comments_url, parent_id, budget):
    """Every reply to one comment, or None if the budget ran out first."""
    collected = []
    page_token = None
    while True:
        if not budget.spend(_UNITS_LIST):
            return None
        params = {"part": "snippet", "parentId": parent_id, "maxResults": 100}
        if page_token:
            params["pageToken"] = page_token
        response = _get(session, comments_url, params, skip_statuses=_SKIP_STATUSES)
        if response is None:
            return collected
        body = response.json()
        collected.extend(body.get("items") or [])
        page_token = body.get("nextPageToken")
        if not page_token:
            return collected


def build_caption(spec, resource, paced=None):
    """Caption CUES — one row per (video, track language, cue start).

    That is Fivetran's grain, keyed on (video_id, languages, start), and it is
    not what captions.list returns: the list gives track METADATA, and the cue
    text and timings only come from downloading the track. Hence two calls per
    video and the cost below.

    THE EXPENSIVE RESOURCE, by two orders of magnitude. captions.list is 50
    units (every other list here is 1) and captions.download is 200, against a
    10,000-unit daily pool — so ~250 units for one video's captions, and a
    catalogue of any size is unreachable in a single run at any schedule.

    So this converges instead of completing: `unit_budget` units per run,
    uncovered videos first, then re-checking covered ones round-robin against
    the track's own `snippet.lastUpdated`. What has been covered is persisted,
    so progress survives a crash and no video is crawled twice for nothing. At
    the spec's 250 units that is one video per run, ~24 a day.

    Old cue rows for a track that was edited SHORTER are not removed — there is
    no soft_delete here, because a budget-bounded pass has not seen most of the
    table and absence means "not reached yet". Fivetran does not capture
    deletes on any YouTube table either, so this matches rather than regresses.
    """
    transform = make_transformer(resource)
    endpoint = _endpoint(spec, resource)
    budget_units = int(resource.incremental.get("unit_budget") or 0)
    download_url_base = endpoint["url"]

    @_resource_for(resource)
    def caption():
        state = dlt.current.resource_state()
        covered = dict(state.get("covered") or {})
        budget = _UnitBudget(budget_units)
        session = _authed_session(spec, paced)
        ids = _video_ids(spec, session)

        pending = [v for v in ids if v not in covered]
        if pending:
            worklist = pending
            log.info("caption: %d video(s) never crawled, budget %d units",
                     len(pending), budget_units)
        else:
            # Everything covered once — now re-check in a rotating order so no
            # video is starved. The cursor is an index into the video list,
            # which is stable enough for this purpose and self-correcting if
            # the catalogue changes.
            start = int(state.get("recheck_cursor") or 0) % max(len(ids), 1)
            worklist = ids[start:] + ids[:start]
            log.info("caption: all %d video(s) covered, re-checking from %d",
                     len(ids), start)

        checked = 0
        for video_id in worklist:
            if not budget.can_afford(_UNITS_CAPTIONS_LIST + _UNITS_CAPTIONS_DOWNLOAD):
                break
            budget.spend(_UNITS_CAPTIONS_LIST)
            checked += 1
            response = _get(session, download_url_base,
                            {**endpoint["params"], "videoId": video_id},
                            skip_statuses=_SKIP_STATUSES)
            tracks = (response.json().get("items") or []) if response is not None else []
            # Recorded even when there are no tracks, and even when the list
            # call was skipped: without that, a video with no captions is
            # "never crawled" forever and the worklist never moves past it.
            #
            # A list of STRINGS rather than of tuples, because this is persisted
            # in dlt's pipeline state and comes back through JSON — where a
            # tuple returns as a list and never compares equal to the freshly
            # built one. The symptom was silent and expensive: every track
            # re-downloaded every run at 200 units each, so the crawl would
            # never have advanced past the first video.
            signature = sorted(
                f"{t['id']}@{(t.get('snippet') or {}).get('lastUpdated', '')}" for t in tracks)
            if covered.get(video_id) == signature:
                continue
            # `complete` is what stops a video with more tracks than the
            # remaining budget from being recorded as covered: at 250 units a
            # run buys one download, so a two-track video would otherwise be
            # marked done after its first track and its second would never be
            # fetched again. A track SKIPPED by the API (a 403 on a
            # third-party or disabled caption) does not clear this — that video
            # really has been looked at, and retrying it every run forever
            # would spend the whole crawl on it.
            complete = True
            for track in tracks:
                if not budget.can_afford(_UNITS_CAPTIONS_DOWNLOAD):
                    complete = False
                    break
                budget.spend(_UNITS_CAPTIONS_DOWNLOAD)
                for cue in _caption_cues(session, download_url_base, track, video_id):
                    yield transform(cue)
            if complete:
                covered[video_id] = signature

        state["covered"] = covered
        if not pending:
            state["recheck_cursor"] = int(state.get("recheck_cursor") or 0) + checked
        log.info("caption: %d video(s) checked, %d units spent", checked, budget.spent)

    return _as_source(spec, caption)


def _caption_cues(session, base_url, track, video_id):
    """One caption track, downloaded as WebVTT and split into cue rows."""
    snippet = track.get("snippet") or {}
    # A track can be undownloadable — third-party contributions, or captions
    # disabled — and that is a 403 about one track, not a broken run.
    response = _get(session, f"{base_url}/{track['id']}", {"tfmt": "vtt"},
                    skip_statuses=_SKIP_STATUSES)
    if response is None:
        return
    # Fivetran's column is `languages`, plural, and is part of the key; the API
    # field is `language`, singular. Renamed here to match the schema being
    # reproduced rather than the API being read.
    language = snippet.get("language") or ""
    for start, duration, text in _parse_vtt(response.text):
        yield {
            "video_id": video_id,
            "languages": language,
            "start": start,
            "duration": duration,
            "text": text,
            "track_id": track["id"],
            "track_kind": snippet.get("trackKind"),
            "track_last_updated": snippet.get("lastUpdated"),
        }


# `00:01:02.500` or `01:02.500` — WebVTT makes the hours field optional.
_VTT_TIME = re.compile(r"(?:(\d+):)?(\d{1,2}):(\d{2})(?:[.,](\d{1,3}))?")


def _parse_vtt(body):
    """[(start_seconds, duration_seconds, text)] from a WebVTT document.

    Hand-parsed rather than pulled in as a dependency: the subset YouTube emits
    is a cue-timing line plus text lines, and a parser for that is smaller than
    the argument about which library to add to a shared environment.
    """
    cues = []
    for block in re.split(r"\n\s*\n", body.replace("\r\n", "\n")):
        lines = [line for line in block.strip().split("\n") if line.strip()]
        timing = next((i for i, line in enumerate(lines) if "-->" in line), None)
        if timing is None:
            continue  # the WEBVTT header, a NOTE, or a STYLE block
        left, _, right = lines[timing].partition("-->")
        start, end = _vtt_seconds(left), _vtt_seconds(right)
        if start is None or end is None:
            continue
        text = "\n".join(lines[timing + 1:]).strip()
        if text:
            cues.append((start, round(end - start, 3), text))
    return cues


def _vtt_seconds(value):
    match = _VTT_TIME.search(value)
    if not match:
        return None
    hours, minutes, seconds, millis = match.groups()
    return (
        int(hours or 0) * 3600 + int(minutes) * 60 + int(seconds)
        + int((millis or "0").ljust(3, "0")) / 1000
    )


# ── Shared plumbing ──────────────────────────────────────────────────────────


class _UnitBudget:
    """Data API quota units this resource may spend in one run.

    Charged BEFORE the request, because Google charges for failures too: "All
    API requests, including invalid requests, incur at least a one-point quota
    cost." A budget that only counted successes would let a retry storm spend
    the project's whole day.
    """

    def __init__(self, limit):
        self.limit = limit
        self.spent = 0

    def can_afford(self, units):
        return self.limit <= 0 or self.spent + units <= self.limit

    def spend(self, units):
        if not self.can_afford(units):
            return False
        self.spent += units
        return True


def _resource_for(resource):
    """The dlt.resource decorator this spec's resource asks for."""
    return dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        # Nested parts are JSON-stringified by the transformer, same as the
        # declarative path, so a field Google adds to `snippet` tomorrow cannot
        # mint a table here.
        max_table_nesting=0,
    )


def _as_source(spec, resource_fn):
    """A dlt SOURCE wrapping one resource.

    A source, not the bare resource: the CLI's samplers and the run summary's
    row counts both walk `.resources`, which a DltResource does not have — so
    returning one fails at the first `--sample` rather than at build time.
    """
    @dlt.source(name=spec.name)
    def one():
        return resource_fn

    return one()


def _endpoint(spec, resource):
    """{url, params, page_size} for a metadata resource.

    The Data API is on a different host from the Reporting API, so these
    resources carry `endpoint.base_url` and this is where it is honoured. A
    spec key rather than a constant here because the host is a fact about the
    API, and the spec is where facts about the API live.
    """
    endpoint = resource.incremental.get("endpoint") or resource.endpoint
    path = endpoint.get("path")
    if not path:
        raise RuntimeError(f"{spec.name}.{resource.name}: needs `incremental.endpoint.path`.")
    base = endpoint.get("base_url") or spec.base_url
    return {
        "url": f"{base}{path}",
        "params": dict(endpoint.get("params") or {}),
        "page_size": endpoint.get("page_size"),
    }


def _channel_record(spec, session):
    """The channel resource, fetched once per run.

    Cached because four of the five metadata resources need the channel's id or
    its uploads-playlist id, and re-reading it per resource would spend four
    units restating something that cannot change mid-run. Process-scoped, which
    is run-scoped under Airflow — the same pattern as Swoogo's `_EVENT_IDS`.
    """
    cached = _CHANNELS.get(spec.name)
    if cached is not None:
        return cached

    endpoint = _endpoint(spec, spec.resource("channel"))
    channel = spec.api.get("channel") or "MINE"
    params = dict(endpoint["params"])
    # `mine=true` reads whichever channel consented, which is what a
    # single-channel connector wants; an explicit id would let the credential
    # read a channel it merely has access to.
    params.update({"mine": "true"} if channel == "MINE" else {"id": channel})
    response = _get(session, endpoint["url"], params)
    items = response.json().get("items") or []
    if not items:
        raise RuntimeError(
            f"{spec.name}: channels.list returned no channel for {channel!r}, so every "
            f"metadata resource would load zero rows. Refusing to report that as "
            f"success — check the refresh token belongs to a Google account with a "
            f"YouTube channel."
        )
    _CHANNELS[spec.name] = items[0]
    return items[0]


_CHANNELS = {}


def _video_ids(spec, session):
    """Every video id on the channel, fetched once per run.

    Shared by `video` (which hydrates them) and `caption` (which crawls them),
    so it is cached: walking the uploads playlist twice per run would double
    the cost of the cheapest resource for no new information.
    """
    cached = _VIDEO_IDS.get(spec.name)
    if cached is not None:
        return cached

    channel = _channel_record(spec, session)
    uploads = ((channel.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads")
    if not uploads:
        raise RuntimeError(
            f"{spec.name}: the channel has no contentDetails.relatedPlaylists.uploads, "
            f"which is the only documented way to enumerate its videos. Check "
            f"`part=contentDetails` is still on the channel resource's endpoint params."
        )
    items_url = _endpoint(spec, spec.resource("video"))["url"].replace("/videos", "/playlistItems")
    ids = []
    for item in _google_pages(session, items_url,
                              {"part": "contentDetails", "playlistId": uploads,
                               "maxResults": 50}):
        video_id = (item.get("contentDetails") or {}).get("videoId")
        if video_id:
            ids.append(video_id)
    log.info("youtube: %d video(s) in the uploads playlist", len(ids))
    _VIDEO_IDS[spec.name] = ids
    return ids


_VIDEO_IDS = {}


def _google_pages(session, url, params, item_key="items"):
    """Walk Google's pageToken envelope.

    One shape for both APIs: `pageToken` in, `nextPageToken` and a list beside
    it in the body. `item_key` differs because the Reporting API names its list
    `jobs`/`reports` while the Data API always calls it `items`.

    Stops on a missing nextPageToken, and also on an empty page — a listing
    that hands back a token and no rows would otherwise loop forever, which is
    the failure Pylon's glitch paginator exists for and is cheap to rule out
    here rather than discover in production.
    """
    page_token = None
    while True:
        query = dict(params)
        if page_token:
            query["pageToken"] = page_token
        response = _get(session, url, query)
        body = response.json()
        items = body.get(item_key) or []
        yield from items
        page_token = body.get("nextPageToken")
        if not page_token or not items:
            return


def _max_str(left, right):
    """The later of two RFC3339 strings, either of which may be None.

    String comparison is correct for RFC3339 in a fixed offset, which is what
    Google returns ("2026-08-07T12:34:56.789Z"), and it avoids parsing a value
    that is only ever passed straight back as a query parameter.
    """
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)
