"""Swoogo: the one thing its spec cannot say.

Twelve of Swoogo's fourteen useful endpoints are scoped to a single event —
`GET /registrants` without an `event_id` is an error, not a full collection. So
every one of them has to be called once per event, and the declarative config
has no vocabulary for "call this endpoint once per row of that other resource".
That is the whole reason this file exists.

The worklist is deliberately EVERY event, not the events that changed. Swoogo
does not touch an event's `updated_at` when somebody registers for it, so
"parents modified since last run" would skip exactly the events whose
registrant data moved — silently, and worse the longer an event has been
running. The bound that keeps this affordable is the child's own cursor,
applied server-side through Swoogo's `search=updated_at>=…` filter, so a quiet
event costs one empty page rather than a full re-read.
"""

from __future__ import annotations

import logging

import dlt

from ..runtime import _auth, column_hints, make_transformer, paced_session

log = logging.getLogger(__name__)

_EVENTS_PATH = "/events"
_EVENT_PAGE_SIZE = 200

# (connect, read) seconds. Explicit because `requests` defaults to no timeout at
# all: a call that never answers blocks the generator forever, and a fan-out
# resource holds the source's pool slot of one while it does. That is not a slow
# run, it is a stopped pipeline — every later run queues behind it, and the only
# symptom is a task that never finishes. Observed exactly once, for 858 seconds,
# before the scheduler had to SIGKILL it.
_TIMEOUT = (10, 60)

# Swoogo renders and parses timestamps in this shape, not ISO 8601. Sending an
# ISO string with a `T` to the search filter is accepted and matches nothing.
_TIMESTAMP_FORMAT = "YYYY-MM-DD HH:mm:ss"


def build_resource(spec, resource, paced=None):
    """One dlt resource, fanned out over every event.

    Generic across all twelve per-event endpoints: everything that differs
    between them — path, page size, field list, cursor — is in the spec.
    """
    incremental = resource.incremental
    endpoint = incremental.get("endpoint") or {}
    if not endpoint.get("path"):
        raise RuntimeError(
            f"{spec.name}.{resource.name}: parent_fanout needs "
            f"`incremental.endpoint.path`."
        )
    # Optional, and its absence is a real configuration rather than an omission:
    # an endpoint whose records carry no usable timestamp cannot be bounded, so
    # it is re-read in full for every event, every run. Swoogo's line items are
    # the case — no id, no created_at, and updated_at null on 528 of 530 rows.
    # Filtering on a cursor that is usually null would permanently miss every
    # record that is never edited after it is written.
    cursor_field = incremental.get("cursor_field")
    lookback = int(incremental.get("lookback_seconds") or 0)
    transform = make_transformer(resource)

    def _fetch(since):
        session = _authed_session(spec, paced)
        events = _event_ids(spec, session)
        log.info("%s: fanning out over %d event(s)%s", resource.name, len(events),
                 f" since {since}" if since
                 else " (full read — no cursor)" if not cursor_field
                 else " (no cursor yet — full read)")
        for event_id in events:
            for record in _pages(spec, session, endpoint, event_id, cursor_field, since):
                yield transform(record)

    decorate = dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        # Nested objects are stringified by the transformer, same as the
        # declarative path, so a new Swoogo custom field cannot mint a table.
        max_table_nesting=0,
    )

    if not cursor_field:
        fan_out = decorate(lambda: _fetch(None))
    else:
        # The incremental has to be the default argument: that is the hook dlt
        # uses to bind persisted state to the call, and building it in the body
        # would give a cursor that forgets its last value between runs.
        @decorate
        def fan_out(cursor=dlt.sources.incremental(cursor_field)):  # noqa: B008
            yield from _fetch(_since(cursor, lookback))

    # A source, not the bare resource. Everything downstream — the CLI's
    # samplers, the run summary's row counts — walks `.resources`, which a
    # DltResource does not have. Returning one fails at the first --sample
    # rather than at build time, so the shape is asserted in the tests.
    @dlt.source(name=spec.name)
    def fan_out_source():
        return fan_out

    return fan_out_source()


# Note there is no build_<name> anywhere in this module. The runtime looks for
# one per resource and falls back to build_resource, and every per-event
# endpoint here differs only in data the spec already carries — so one function
# serves all twelve, and a thirteenth needs no Python.


def _authed_session(spec, paced):
    """An authed session, paced if the run supplied a pacer.

    `paced_session` is dlt's own retrying client with the spec's budget wrapped
    around `send`, so pacing covers every request made here without this module
    having to remember to ask before each one.
    """
    if paced is not None:
        session = paced_session(spec, paced)
    else:
        from dlt.sources.helpers.requests.retry import Client

        session = Client(raise_for_status=False).session

    auth = _auth(spec)
    if not callable(auth):
        # Every other auth type resolves to a config dict for dlt's declarative
        # client, which a bare session cannot use. Swoogo is oauth2, which
        # resolves to an object; anything else here is a spec mistake.
        # RuntimeError, not TypeError: nobody passed a bad argument — the spec
        # names an auth type that cannot sign a bare session's requests.
        raise RuntimeError(  # noqa: TRY004
            f"{spec.name}: the fan-out needs an auth type that produces a "
            f"request signer, got {spec.api['auth']['type']!r}."
        )
    session.auth = auth
    return session


def _since(cursor, lookback_seconds):
    """The lower bound for this run, or None on a first load.

    The lookback re-reads a little of what was already loaded. That overlap is
    free — every resource merges on its primary key — and it covers records
    written while the previous run was mid-fan-out, which at 20 requests a
    minute is a window measured in minutes rather than milliseconds.
    """
    import pendulum

    last = getattr(cursor, "last_value", None)
    if not last:
        return None
    moment = pendulum.instance(last) if not isinstance(last, str) else pendulum.parse(last)
    return moment.subtract(seconds=lookback_seconds).format(_TIMESTAMP_FORMAT)


def _event_ids(spec, session):
    """Every event id in the account.

    Read from the API rather than from the warehouse. The warehouse copy would
    be cheaper, but it is only correct once `events` has loaded at least once,
    and a connector whose first run silently fetches nothing is a bad trade for
    the handful of requests this costs.

    Fetched once per run, not once per resource. Each of the twelve per-event
    resources needs the same list, and re-reading it for each one spent twelve
    list calls — 120 credits of a 2000-credit budget — restating a fact that
    cannot change mid-run.
    """
    cached = _EVENT_IDS.get(spec.name)
    if cached is not None:
        return cached

    ids = []
    for record in _paged(spec, session, _EVENTS_PATH,
                         {"fields": "id", "per-page": _EVENT_PAGE_SIZE}):
        event_id = record.get("id")
        if event_id is not None:
            ids.append(event_id)
    if not ids:
        raise RuntimeError(
            f"{spec.name}: /events returned no events, so every per-event "
            f"resource would load zero rows. Refusing to report that as success."
        )
    _EVENT_IDS[spec.name] = ids
    return ids


# The run's event worklist. Process-scoped, which is run-scoped under Airflow.
_EVENT_IDS = {}


def _pages(spec, session, endpoint, event_id, cursor_field, since):
    """One event's worth of a child endpoint, filtered server-side."""
    params = dict(endpoint.get("params") or {})
    params["event_id"] = event_id
    page_size = endpoint.get("page_size")
    if page_size:
        params[endpoint.get("page_size_param", "limit")] = page_size
    if since:
        # Swoogo's own filter grammar. Bounding server-side is what makes an
        # hourly fan-out affordable: an unchanged event answers with an empty
        # first page instead of its entire registrant list.
        params["search"] = f"{cursor_field}>={since}"
    yield from _paged(spec, session, endpoint["path"], params)


def _paged(spec, session, path, params):
    """Walk Swoogo's page-number envelope: items[] plus _meta.pageCount."""
    url = f"{spec.base_url}{path}"
    page = 1
    while True:
        response = session.get(url, params=dict(params, page=page), timeout=_TIMEOUT)
        response.raise_for_status()
        body = response.json()
        items = body.get("items") or []
        yield from items

        total_pages = (body.get("_meta") or {}).get("pageCount")
        if total_pages is None:
            # No page count to trust, so an empty page is the only stop signal.
            if not items:
                return
        elif page >= int(total_pages):
            return
        page += 1
