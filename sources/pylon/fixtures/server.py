"""Pylon's two awkward endpoints, plus the page that lies.

Everything the spec can describe — the cursor envelope, the four directory
endpoints — is served generically from the fixtures. What is here is the part
that made this connector need Python at all, so the offline suite exercises the
same behaviours the extension exists to survive:

  two endpoints, one table  GET /issues filters on created_at and is walked in
                            30-day slices; POST /issues/search filters on
                            updated_at and takes its cursor in the body. Both
                            are honoured, so a run with no cursor and a run
                            with one visibly take different paths.
  the glitch page           a response advertising `has_next_page: true` with
                            an empty `data`. Served ONCE per run, cleared on
                            retry — which is exactly the real behaviour, and
                            the reason the extension retries a cursor instead
                            of treating an empty page as the end.
  messages are per issue    /issues/{id}/messages is the only way in, and an
                            id the tenant no longer has answers 404. That is
                            what `skip_statuses` is for: one scrubbed ticket
                            must not fail a run.
"""

from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse

# One record a page, so a window holding two issues is walked rather than
# swallowed whole.
PAGE_SIZE = 1


def _query(request):
    return parse_qs(urlparse(request.url).query)


def _moment(value):
    """RFC3339 in either spelling — the fixtures use `Z`, the client `+00:00`."""
    return datetime.fromisoformat(value)


def _within(value, gte, lte):
    if not value:
        return False
    moment = _moment(value)
    if gte and moment < _moment(gte):
        return False
    return not (lte and moment > _moment(lte))


def _page(rows, cursor, state):
    """Pylon's cursor envelope, glitching once per run.

    The glitch is served instead of the first page that has records, and only
    once: retrying the same cursor gets the real page. A mock that glitched
    forever would test a hang, and one that never glitched would let the retry
    path rot.
    """
    offset = int(cursor or 0)
    window = rows[offset:offset + PAGE_SIZE]
    if window and not state["glitched"]:
        state["glitched"] = True
        return {"data": [], "pagination": {"has_next_page": True, "cursor": str(offset)}}
    more = offset + PAGE_SIZE < len(rows)
    return {
        "data": window,
        "pagination": {"has_next_page": more,
                       "cursor": str(offset + PAGE_SIZE) if more else None},
    }


def register(mock, spec, fixtures):
    base = spec.base_url.rstrip("/")
    issues = fixtures.get("issues") or []
    messages = fixtures.get("issue_messages") or []
    known = {issue["id"] for issue in issues}
    state = {"glitched": False}

    def serve_window(request, context):
        """Backfill: the slice of history this window asked for."""
        params = _query(request)
        gte = params.get("created_at[gte]", [None])[0]
        lte = params.get("created_at[lte]", [None])[0]
        rows = [issue for issue in issues if _within(issue.get("created_at"), gte, lte)]
        return _page(rows, params.get("cursor", [None])[0], state)

    def serve_search(request, context):
        """Steady state: everything updated after the cursor's lower bound."""
        body = request.json() or {}
        since = (body.get("filter") or {}).get("value")
        rows = [issue for issue in issues
                if issue.get("updated_at") and (
                    since is None or _moment(issue["updated_at"]) > _moment(since))]
        return _page(rows, body.get("cursor"), state)

    def serve_messages(request, context):
        issue_id = urlparse(request.url).path.split("/")[-2]
        if issue_id not in known:
            # Deleted or scrubbed. The run must survive it.
            context.status_code = 404
            return {"error": "issue not found"}
        rows = [m for m in messages if m["issue_id"] == issue_id]
        return _page(rows, _query(request).get("cursor", [None])[0], state)

    mock.get(f"{base}/issues", json=serve_window)
    mock.post(f"{base}/issues/search", json=serve_search)
    mock.get(re.compile(re.escape(base) + r"/issues/[^/]+/messages"), json=serve_messages)
