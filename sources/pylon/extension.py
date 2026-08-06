"""Pylon: the two things its spec cannot say.

This is the worked example of the escape hatch, and it is deliberately the
awkward API rather than a tidy one — a contract that only fits easy sources
would break on the second connector.

    issues          `search_window`. Pylon filters GET /issues on created_at and
                    POST /issues/search on updated_at. Those answer different
                    questions: the first would silently miss an issue opened
                    last year and updated this morning, which is precisely the
                    row an incremental run exists to catch. So steady state uses
                    the search endpoint, and only backfill walks the windowed
                    one — in 30-day slices, because that is a hard API cap.

    issue_messages  `parent_watermark`. There is no cross-issue messages
                    endpoint at all; /issues/{id}/messages is the only way in.
                    The worklist therefore comes from the warehouse: issues
                    whose latest_message_time is newer than the newest message
                    already loaded for them.

Both are behaviours, not shapes, which is why no configuration vocabulary
expresses them. Everything else about Pylon — four directory endpoints, the
cursor envelope, the rate budgets, the tombstone rules — is in source.yml and
needs no Python.

The contract this file implements is `sources/CONTRACT.md`.
"""

from __future__ import annotations

import logging
import time

import dlt
import pendulum
from ingest_runtime.extension_api import (
    column_hints,
    endpoint_params,
    make_transformer,
    session_for,
    warehouse_rows,
)

log = logging.getLogger(__name__)

# (connect, read) seconds. Explicit because `requests` defaults to no timeout at
# all: a call that never answers blocks the generator forever while holding the
# source's pool slot of one. That is not a slow run, it is a stopped pipeline.
_TIMEOUT = (10, 60)

# Pylon serves pages that advertise another cursor and carry no data. Retrying
# the same cursor clears it; looping on it forever does not.
_GLITCH_RETRIES = 3
_GLITCH_BACKOFF_SECONDS = 2

# Clock slack when comparing a parent's change time against the newest child
# already loaded, so a message written in the same second as its issue's
# latest_message_time is not permanently just-missed.
_WATERMARK_FUDGE_SECONDS = 3


def build_issues(spec, resource, paced=None):
    """Issues, incrementally by updated_at — or by created_at window on a backfill."""
    incremental = resource.incremental
    search = incremental.get("search") or {}
    window = incremental.get("window") or {}
    cursor_field = incremental.get("cursor_field", "updated_at")
    lookback = int(incremental.get("lookback_seconds") or 0)
    transform = make_transformer(resource)

    @dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        max_table_nesting=0,
    )
    def issues(cursor=dlt.sources.incremental(cursor_field)):  # noqa: B008
        session = session_for(spec, paced)
        since = _since(cursor, lookback)
        if since is None:
            # No cursor yet: walk history through the windowed endpoint, whose
            # filter is created_at. Every issue has one, so this is the only
            # traversal that provably reaches all of them.
            yield from _backfill(spec, session, resource, window, transform)
        else:
            log.info("issues: searching for updates since %s", since)
            yield from _search(spec, session, resource, search, cursor_field,
                               since, transform)

    return _as_source(spec, issues)


def build_issue_messages(spec, resource, paced=None):
    """Messages for issues the warehouse says have newer traffic than we hold."""
    incremental = resource.incremental
    endpoint = incremental.get("endpoint") or {}
    parent = incremental.get("parent", "issues")
    skip_statuses = set(incremental.get("skip_statuses") or ())
    budget_seconds = int(incremental.get("budget_minutes") or 0) * 60
    transform = make_transformer(resource)

    @dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        max_table_nesting=0,
    )
    def issue_messages():
        session = session_for(spec, paced)
        pending = _pending_issue_ids(parent, resource.name)
        if not pending:
            log.info("issue_messages: no issue has traffic newer than what is loaded")
            return
        log.info("issue_messages: %d issue(s) to catch up", len(pending))

        deadline = time.monotonic() + budget_seconds if budget_seconds else None
        for index, issue_id in enumerate(pending):
            if deadline is not None and time.monotonic() > deadline:
                # Stop cleanly rather than be killed mid-load. The watermark is
                # derived from what actually landed, so the next run resumes
                # exactly here — no state to keep, nothing to reconcile.
                log.warning("issue_messages: budget spent after %d of %d issues; "
                            "the next run resumes from the same watermark",
                            index, len(pending))
                return
            for record in _messages_for(spec, session, resource, endpoint,
                                        issue_id, skip_statuses):
                yield transform(record)

    return _as_source(spec, issue_messages)


def _as_source(spec, resource_fn):
    """A dlt source, never a bare resource.

    Everything downstream — the CLI's samplers, the run summary's row counts —
    walks `.resources`, which a DltResource does not have. Returning one fails
    at the first `--sample` rather than at build time, so the contract suite
    asserts the shape for every connector.
    """
    @dlt.source(name=spec.name)
    def source():
        return resource_fn

    return source()


def _since(cursor, lookback_seconds):
    """The lower bound for this run, or None on a first load.

    The lookback re-reads a little of what was already loaded. That overlap is
    free — every resource merges on its primary key — and it covers records
    written while the previous run was mid-fetch.
    """
    last = getattr(cursor, "last_value", None)
    if not last:
        return None
    moment = pendulum.parse(last) if isinstance(last, str) else pendulum.instance(last)
    return moment.subtract(seconds=lookback_seconds)


def _search(spec, session, resource, search, cursor_field, since, transform):
    """Steady state: POST /issues/search, filtering on the update time."""
    endpoint = search.get("endpoint") or {}
    path = endpoint.get("path", "/issues/search")
    body = {
        "filter": {
            "field": cursor_field,
            "operator": "time_is_after",
            "value": since.to_iso8601_string(),
        },
        **endpoint_params(resource, endpoint),
    }
    # Pylon takes the cursor in the body on this endpoint and in the query string
    # on every other one — the reason `cursor_in: body` is declared in the spec
    # rather than assumed.
    for record in _paged(spec, session, path, method="POST", body=body):
        yield transform(record)


def _backfill(spec, session, resource, window, transform):
    """First run: walk created_at in slices the API will actually answer."""
    endpoint = window.get("endpoint") or {}
    path = endpoint.get("path", "/issues")
    filters_on = window.get("filters_on", "created_at")
    max_days = int(window.get("max_window_days") or 30)
    floor = pendulum.parse(spec.backfill_start or "2019-01-01")
    now = pendulum.now("UTC")

    start = floor
    while start < now:
        end = min(start.add(days=max_days), now)
        params = {
            **endpoint_params(resource, endpoint),
            f"{filters_on}[gte]": start.to_iso8601_string(),
            f"{filters_on}[lte]": end.to_iso8601_string(),
        }
        log.info("issues: backfill window %s .. %s", start.to_date_string(), end.to_date_string())
        for record in _paged(spec, session, path, params=params):
            yield transform(record)
        start = end


def _pending_issue_ids(parent_table, child_table):
    """Issues whose latest_message_time is newer than the newest message loaded.

    One query, not one per issue: the alternative is a request per open ticket
    every hour, most of which return nothing.

    Two queries on the first run only, and the reason is worth stating. dlt
    creates a table when a resource yields its first row, so before that run
    the messages table does not exist — and a query naming it fails as a whole,
    which `warehouse_rows` turns into an empty result. That is indistinguishable
    from "nothing is pending", so a single query would decide there was no work,
    yield nothing, and leave the table still not existing. Forever. The
    watermark can only bootstrap if the absence of the child is asked about
    separately.
    """
    loaded = warehouse_rows(lambda qualified: f"SELECT count(*) FROM {qualified(child_table)}")
    have_messages = bool(loaded) and bool(loaded[0][0])

    def query(qualified):
        issues = qualified(parent_table)
        if not have_messages:
            # Nothing loaded yet, so every issue that has ever had traffic is
            # pending. No join: the table it would name is not there.
            return (
                f"SELECT i.id FROM {issues} AS i "
                f"WHERE i.latest_message_time IS NOT NULL "
                f"ORDER BY i.latest_message_time"
            )
        messages = qualified(child_table)
        return (
            f"SELECT i.id FROM {issues} AS i "
            f"LEFT JOIN (SELECT issue_id, max(timestamp) AS newest "
            f"           FROM {messages} GROUP BY issue_id) AS m "
            f"  ON m.issue_id = i.id "
            f"WHERE i.latest_message_time IS NOT NULL "
            f"  AND (m.newest IS NULL OR i.latest_message_time > m.newest + "
            f"       INTERVAL {_WATERMARK_FUDGE_SECONDS} SECOND) "
            f"ORDER BY i.latest_message_time"
        )

    return [row[0] for row in warehouse_rows(query)]


def _messages_for(spec, session, resource, endpoint, issue_id, skip_statuses):
    """One issue's messages, tolerating the statuses that mean 'it is gone'."""
    path = endpoint.get("path", "/issues/{issue_id}/messages").format(issue_id=issue_id)
    try:
        yield from _paged(spec, session, path, params=endpoint_params(resource, endpoint))
    except _HttpStatus as error:
        if error.status in skip_statuses:
            # A scrubbed or deleted ticket. Failing the whole run over one is how
            # a connector becomes something people stop trusting to be red.
            log.info("issue_messages: skipping issue %s (HTTP %s)", issue_id, error.status)
            return
        raise


class _HttpStatus(RuntimeError):
    def __init__(self, status, url):
        super().__init__(f"HTTP {status} from {url}")
        self.status = status


def _paged(spec, session, path, params=None, method="GET", body=None):
    """Walk Pylon's cursor envelope, surviving pages that lie about having more.

    The glitch is real and load-bearing: a page arrives with
    `pagination.has_next_page: true`, a cursor, and an empty `data`. Treating
    the empty page as the end truncates the fetch silently; following the cursor
    forever hangs the run. Retrying the same cursor a few times clears it, and
    exhausting the retries raises rather than returning what was collected so
    far — a short read that reports success is the failure this whole stack
    exists to prevent.
    """
    url = f"{spec.base_url}{path}"
    cursor = None
    empty_streak = 0

    while True:
        payload = dict(body or {})
        query = dict(params or {})
        if cursor:
            if method == "POST":
                payload["cursor"] = cursor
            else:
                query["cursor"] = cursor

        response = session.request(
            method, url,
            params=query or None,
            json=payload if method == "POST" else None,
            timeout=_TIMEOUT,
        )
        if response.status_code >= 400:
            raise _HttpStatus(response.status_code, url)
        envelope = response.json() or {}
        records = envelope.get("data") or []
        pagination = envelope.get("pagination") or {}
        next_cursor = pagination.get("cursor")
        has_more = bool(pagination.get("has_next_page")) and bool(next_cursor)

        if records:
            empty_streak = 0
            yield from records
        elif has_more:
            empty_streak += 1
            if empty_streak > _GLITCH_RETRIES:
                raise RuntimeError(
                    f"{spec.name}: {url} returned {empty_streak} consecutive empty "
                    f"pages while still advertising more. Refusing to report a "
                    f"short read as a complete fetch."
                )
            log.warning("empty page with has_next_page at %s — retry %d/%d",
                        url, empty_streak, _GLITCH_RETRIES)
            time.sleep(_GLITCH_BACKOFF_SECONDS * empty_streak)
            continue

        if not has_more:
            return
        cursor = next_cursor
