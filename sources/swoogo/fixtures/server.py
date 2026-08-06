"""Swoogo's API, to the extent that a spec cannot describe it.

Four behaviours here, and each one is a way this connector can silently
under-fetch rather than fail:

  a token has to be minted   nothing can be fetched until the encoded
                             credential has been exchanged for a bearer, so a
                             connector whose auth type is wrong gets 401s
                             rather than an empty result.
  the projection is sparse   every endpoint returns `id, name` unless `fields`
                             is sent. That is what makes the field list in the
                             spec load-bearing: drop it and the cursor column
                             stops existing, and the failure reads as "the
                             source never changes".
  children are event-scoped  `GET /registrants` without an `event_id` is an
                             error, not a full collection. Serving all of them
                             regardless would let a fan-out that visits one
                             event look identical to one that visits all.
  `search=updated_at>=…`     Swoogo's own filter grammar, in Swoogo's own
                             timestamp format. An ISO string with a `T` is
                             accepted and matches nothing, so honouring it here
                             is what makes the second run's bound observable.

Pages are one record wide throughout, so a fan-out that reads only the first
page of an event fails here.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

PER_PAGE = 1

# What Swoogo returns when `fields` is absent, verified against the live API.
SPARSE_FIELDS = ("id", "name")


def _query(request):
    """The query string with its values' case intact — `request.qs` lowercases."""
    return parse_qs(urlparse(request.url).query)


def _project(rows, request):
    fields = _query(request).get("fields")
    wanted = set(fields[0].split(",")) if fields else set(SPARSE_FIELDS)
    return [{key: value for key, value in row.items() if key in wanted} for row in rows]


def _page(rows, request):
    """Swoogo's envelope: items[] plus the _meta the walker stops on."""
    page = int(_query(request).get("page", ["1"])[0])
    start = (page - 1) * PER_PAGE
    return {
        "items": rows[start:start + PER_PAGE],
        "_meta": {"totalCount": len(rows),
                  "pageCount": max(1, -(-len(rows) // PER_PAGE)),
                  "currentPage": page,
                  "perPage": PER_PAGE},
    }


def _since(request):
    """The lower bound out of `search=updated_at>=2026-01-06 10:00:00`, or None."""
    search = _query(request).get("search")
    if not search or ">=" not in search[0]:
        return None
    return search[0].split(">=", 1)[1].strip()


def register(mock, spec, fixtures):
    base = spec.base_url.rstrip("/")
    auth = spec.api["auth"]
    events = fixtures.get("events") or []
    registrants = fixtures.get("registrants") or []

    mock.post(auth["token_url"],
              json={"access_token": "tok-abc", "token_type": "Bearer", "expires_in": 1800})

    def serve_events(request, context):
        return _page(_project(events, request), request)

    def serve_registrants(request, context):
        scope = _query(request).get("event_id")
        if not scope:
            # The real API's answer, and the reason the fan-out exists at all.
            context.status_code = 400
            return {"message": "event_id is required"}
        rows = [r for r in registrants if str(r["event_id"]) == scope[0]]
        since = _since(request)
        if since:
            rows = [r for r in rows if r.get("updated_at") and r["updated_at"] >= since]
        return _page(_project(rows, request), request)

    mock.get(f"{base}/events", json=serve_events)
    mock.get(f"{base}/registrants", json=serve_registrants)
