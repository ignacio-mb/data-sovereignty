"""Lever: the resources its spec cannot express declaratively.

Two shapes live here, both incremental in ways the declarative path cannot do
on its own:

  opportunities   a `search_window` resource. GET /opportunities filters
                  server-side on `updated_at_start`, so the fetch itself is
                  simple; what the declarative path cannot do is remember a
                  cursor between runs and turn it into that query parameter.

                  Unlike Pylon's `search_window` (the reference this strategy
                  name comes from), there is no split between a "window"
                  endpoint and a "search" endpoint: Lever's GET /opportunities
                  accepts BOTH `created_at_start`/`created_at_end` and
                  `updated_at_start`/`updated_at_end` on the same URL. So a
                  first run (no persisted cursor) fetches everything with no
                  time filter at all — cheap enough at Lever's page size and
                  rate limit that it needs no chunking — and every run after
                  that adds `updated_at_start`. One endpoint, one function.

  the nine per-opportunity children (notes, interviews, feedback, offers,
  panels, referrals, resumes, applications, forms)
                  a `parent_watermark` resource, one function serving all
                  nine (`build_resource`, the same way Swoogo's twelve
                  per-event resources share one function). None of the nine
                  has an account-wide endpoint — only
                  /opportunities/:id/<child> — and none except `feedback` even
                  carries its own `updatedAt`, so "was this specific child
                  updated" is not a question these endpoints can answer
                  cheaply. What CAN be asked cheaply is "which opportunities
                  changed", using the exact same `updated_at_start` filter
                  `opportunities` itself uses — confirmed against the live API
                  (a 100-opportunity sample, cross-referencing each child
                  record's own timestamps against its parent's `updatedAt`)
                  that a child changing moves its parent's `updatedAt` too, in
                  all but one case out of a hundred, and that one was a 6-
                  second lag comfortably inside the lookback buffer. So the
                  worklist is "opportunities whose updatedAt moved since the
                  watermark", and every opportunity on it gets ALL NINE
                  endpoints re-read in full — cheap per opportunity, since
                  none of these ever holds more than a handful of rows.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import threading

import dlt

from ..runtime import column_hints, make_transformer, paced_session

log = logging.getLogger(__name__)

# Fetching notes/interviews/etc. for one opportunity is independent of every
# other opportunity's — nothing here has to happen in order, so the fan-out
# fetches several concurrently rather than one full round-trip at a time.
#
# Measured, not guessed: sequential fetching against the live API ran at
# ~3.2 requests/second even against a 10/second budget, because each
# request's own round-trip (~300ms) already exceeds the pacer's 100ms
# interval — with nothing else in flight, the pacer never gets the chance to
# be the bottleneck. 8 concurrent workers reached ~7.35/second; this many
# gives enough requests in flight that EndpointPacer's own shared lock (see
# ingest/pacing.py) becomes the actual limiter, which is the point — the
# budget is enforced once, centrally, however many threads are asking for a
# turn, so raising this number cannot push the real request rate past what
# `rate_limits` in the spec allows.
_FAN_OUT_WORKERS = 16

# (connect, read) seconds, explicit for the same reason Swoogo's is: `requests`
# waits forever by default, and this resource holds the source's pool slot of
# one while it does.
_TIMEOUT = (10, 60)


def build_opportunities(spec, resource, paced=None, initial_value=None):
    """The candidate pipeline, incremental on `updatedAt`.

    `initial_value` is unused by the standard `ingest run` path — build_source
    never passes it, so every normal invocation keeps today's behaviour: a
    genuine first run has no lower bound and reads full history. It exists as
    an explicit, narrow escape hatch for bounding a ONE-OFF first run (a
    smoke test against production before committing to the full historical
    load) via dlt's own mechanism rather than a hand-rolled workaround: dlt
    seeds `cursor.last_value` from it before anything is fetched, and only
    when no cursor is already persisted — so it has no effect on any run
    after the first, and the run it does affect ends by persisting a real
    cursor, exactly as if that had been the true starting point all along.
    """
    incremental = resource.incremental
    endpoint = incremental.get("endpoint") or resource.endpoint
    if not endpoint.get("path"):
        raise RuntimeError(
            f"{spec.name}.{resource.name}: search_window needs `incremental.endpoint.path`."
        )
    cursor_field = incremental.get("cursor_field")
    if not cursor_field:
        raise RuntimeError(
            f"{spec.name}.{resource.name}: search_window needs `incremental.cursor_field`."
        )
    lookback_ms = int(incremental.get("lookback_seconds") or 0) * 1000
    transform = make_transformer(resource)

    def _fetch(since):
        session = _authed_session(spec, paced)
        yield from _pages(spec, session, endpoint, since)

    decorate = dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        # Nested objects (tags, stageChanges, applications, ...) are
        # stringified by the transformer, same as the declarative path, so a
        # new Lever field can never mint a table here.
        max_table_nesting=0,
    )

    # Built outside the signature (rather than inline as the default, the
    # usual pattern here) only because `initial_value` — a closure variable —
    # has to reach it; the object itself still has to BE the default
    # argument, which is the hook dlt uses to bind persisted state to the
    # call, so building it inside the function body would give a cursor that
    # forgets its last value between runs.
    #
    # on_cursor_value_missing='include': confirmed against the live API, not
    # the docs — some real opportunities have a null `updatedAt` (Lever's own
    # note on the equivalent feedback field: it is only set once something
    # changes AFTER creation, so a candidate nobody has touched since being
    # added has none). dlt's default ('raise') fails the whole run on the
    # first one; 'exclude' would silently drop that candidate from the
    # warehouse forever, every run, since a None cursor value never compares
    # greater than anything. 'include' is the only option that does not lose
    # a row over a field Lever itself never populated.
    incremental_cursor = dlt.sources.incremental(
        cursor_field, initial_value=initial_value, on_cursor_value_missing="include"
    )

    @decorate
    def opportunities(cursor=incremental_cursor):
        since = _since(cursor, lookback_ms)
        log.info("opportunities: fetching%s", f" since {since}" if since else " (no cursor yet — full read)")
        for record in _fetch(since):
            yield transform(record)

    # A source, not the bare resource. The CLI's samplers and the run
    # summary's row counts both walk `.resources`, which a DltResource does
    # not have — returning one fails at the first --sample rather than at
    # build time, so the shape is asserted in the tests.
    @dlt.source(name=spec.name)
    def opportunities_source():
        return opportunities

    return opportunities_source()


def build_resource(spec, resource, paced=None, initial_watermark=None):
    """One of the nine per-opportunity children.

    Generic across all nine: everything that differs between them — the path,
    which fields are timestamps — is in the spec, same as Swoogo's
    `build_resource` serving its twelve per-event endpoints.

    The worklist is fetched with `_FAN_OUT_WORKERS` requests in flight at
    once (see its own comment for the measurements behind that number) —
    fetching one opportunity's notes has nothing to do with fetching
    another's, so nothing about correctness depends on doing them one at a
    time. What DOES still happen strictly in order, on the calling thread
    only: reading `dlt.current.resource_state()`, `yield`ing transformed
    records to dlt, and the final watermark write. Only the network call
    itself — `_fetch_one` — runs off-thread.

    `initial_watermark` is `build_opportunities`' `initial_value`, adapted to
    this resource's hand-rolled state instead of dlt's: unused by the
    standard `ingest run` path (build_source never passes it), and only takes
    effect when NO watermark is already persisted for this resource — a
    genuine first run. A raw epoch-ms int, not a datetime: unlike
    `initial_value`, nothing here ever routes it through `_parse_ts`, since
    the watermark this resource tracks is never itself a column.
    """
    incremental = resource.incremental
    if incremental.get("parent") != "opportunities":
        raise RuntimeError(
            f"{spec.name}.{resource.name}: parent_watermark needs `incremental.parent: opportunities` "
            f"— the only parent this extension knows how to watch."
        )
    endpoint = incremental.get("endpoint") or resource.endpoint
    path_template = endpoint.get("path") or ""
    if "{opportunity_id}" not in path_template:
        raise RuntimeError(
            f"{spec.name}.{resource.name}: parent_watermark needs `incremental.endpoint.path` "
            f"containing a {{opportunity_id}} placeholder, got {path_template!r}."
        )
    lookback_ms = int(incremental.get("lookback_seconds") or 0) * 1000
    transform = make_transformer(resource)

    decorate = dlt.resource(
        name=resource.name,
        primary_key=resource.primary_key,
        write_disposition=resource.write_disposition,
        columns=column_hints(resource),
        max_table_nesting=0,
    )

    # One session per WORKER THREAD, not per opportunity: a fresh
    # requests.Session pays a new TCP+TLS handshake before it ever gets to
    # send the request it was built for, and at thread-pool scale that
    # handshake cost is paid on every single one of the worklist's requests
    # instead of once per thread. thread-local rather than a plain dict
    # keyed by thread id: each ThreadPoolExecutor.map() call below spins up
    # its own fresh worker threads, so there is nothing to clean up between
    # resources or between runs.
    _local = threading.local()

    def _session_for_this_thread():
        session = getattr(_local, "session", None)
        if session is None:
            session = _authed_session(spec, paced)
            _local.session = session
        return session

    def _fetch_one(item):
        """Runs in a worker thread: everything network-bound, nothing that
        touches dlt state or yields — those stay on the main thread, since
        neither dlt's state helpers nor a generator's `yield` are safe to
        call from anywhere else."""
        opportunity_id, opportunity_updated_at = item
        path = path_template.format(opportunity_id=opportunity_id)
        records = list(_pages(spec, _session_for_this_thread(), {"path": path}, None))
        return opportunity_id, opportunity_updated_at, records

    @decorate
    def fan_out():
        # A hand-rolled watermark, not dlt.sources.incremental(): the value
        # that has to advance is "how far this run's WORKLIST reached", not
        # "the highest value among yielded CHILD records" — and those differ
        # the moment an opportunity in the worklist has zero rows on this
        # particular endpoint, which is the common case (referrals came back
        # empty for 92% of even HIRED candidates, in the live sample this was
        # sized against). incremental() would then never see that
        # opportunity's timestamp at all, so it would never advance past it —
        # and every later run would keep re-including it in the worklist
        # query, forever, since nothing ever tells the cursor it was already
        # checked. dlt.current.resource_state() is the escape hatch: a value
        # this function controls completely, advanced once per opportunity
        # regardless of whether that opportunity yielded anything here.
        state = dlt.current.resource_state()
        since = state.get("watermark")
        if since is None and initial_watermark is not None:
            since = initial_watermark
        query_since = (since - lookback_ms) if since else None
        worklist_session = _authed_session(spec, paced)
        worklist = _changed_opportunities(spec, worklist_session, query_since)
        log.info(
            "%s: fanning out over %d opportunit%s%s (%d concurrent)", resource.name, len(worklist),
            "y" if len(worklist) == 1 else "ies",
            f" changed since {query_since}" if query_since else " (no watermark yet — full read)",
            _FAN_OUT_WORKERS,
        )

        highest_seen = since or 0
        # executor.map() preserves the worklist's own order for the caller
        # (this loop), which is what keeps the watermark's own semantics
        # simple — but the WORK itself still runs concurrently: workers pick
        # up the next unstarted item as soon as they are free, regardless of
        # whether this loop has consumed their predecessor's result yet.
        with concurrent.futures.ThreadPoolExecutor(max_workers=_FAN_OUT_WORKERS) as executor:
            for opportunity_id, opportunity_updated_at, records in executor.map(_fetch_one, worklist):
                # Advances even on zero rows — see above. `or 0` covers the
                # rare opportunity with a null updatedAt (the same edge case
                # `build_opportunities` handles via on_cursor_value_missing):
                # never lets a missing value regress the watermark backwards.
                highest_seen = max(highest_seen, opportunity_updated_at or 0)
                for record in records:
                    # Most of these nine do NOT echo their own parent id in
                    # the payload (verified per endpoint against the live
                    # API) — the URL already scopes the request, so Lever
                    # does not bother. Always set rather than checked-then-
                    # set: uniform behaviour across the two that DO echo it
                    # (interviews, applications) and the seven that do not.
                    record["opportunityId"] = opportunity_id
                    # Provenance, not a Lever field: the parent's watermark
                    # value AT THE TIME this child was fanned out, so "why
                    # hasn't this been re-checked yet" is answerable from the
                    # warehouse alone.
                    record["_opportunity_updated_at"] = opportunity_updated_at
                    yield transform(record)

        # Only reached if the whole worklist was walked without raising — a
        # crash partway through must not advance past opportunities this run
        # never actually got to.
        state["watermark"] = highest_seen

    @dlt.source(name=spec.name)
    def fan_out_source():
        return fan_out

    return fan_out_source()


def _authed_session(spec, paced):
    """A session signed with Lever's basic auth: the API key as username, no
    password.

    Not built through the runtime's `_auth()`: that function returns a config
    DICT shaped for dlt's declarative REST client, which a bare `requests`
    session cannot use directly (Swoogo's own extension hits the same wall for
    its oauth2 case). `requests.Session.auth` accepts a plain `(user, pass)`
    tuple as basic auth natively, so this reads the credential itself instead.
    """
    if paced is not None:
        session = paced_session(spec, paced)
    else:
        from dlt.sources.helpers.requests.retry import Client

        session = Client(raise_for_status=False).session

    token_env = spec.token_env
    token = os.environ.get(token_env, "").strip()
    if not token:
        raise RuntimeError(f"{spec.name}: {token_env} is not set. Add it to .env (never to the spec).")
    session.auth = (token, "")
    return session


def _since(cursor, lookback_ms):
    """The lower bound for this run, in epoch milliseconds — Lever's own unit
    — or None on a first load.

    `cursor.last_value` is a tz-aware datetime, not the raw epoch-ms int Lever
    sends: `updatedAt` is listed in this resource's `timestamp_columns`, so
    `flatten_record` has already run it through `_parse_ts` by the time dlt's
    incremental machinery inspects the yielded record and captures this value
    — the transform runs first, INSIDE the same generator that yields to dlt.
    Converting back to milliseconds here is what makes it the shape Lever's own
    `updated_at_start` parameter wants.
    """
    last = getattr(cursor, "last_value", None)
    if not last:
        return None
    return int(last.timestamp() * 1000) - lookback_ms


def _pages(spec, session, endpoint, since):
    """Walk Lever's offset envelope: data[] plus next/hasNext."""
    params = dict(endpoint.get("params") or {})
    page_size = endpoint.get("page_size")
    if page_size:
        params[endpoint.get("page_size_param", "limit")] = page_size
    if since is not None:
        # Lever's own filter grammar. Omitting the end bound means "through
        # now" — the correct steady-state behaviour, and it sidesteps any
        # clock-skew edge case a fixed end timestamp could introduce.
        params["updated_at_start"] = since

    url = f"{spec.base_url}{endpoint['path']}"
    offset = None
    while True:
        query = dict(params)
        if offset:
            query["offset"] = offset
        response = session.get(url, params=query, timeout=_TIMEOUT)
        response.raise_for_status()
        body = response.json()
        yield from body.get("data") or []

        if not body.get("hasNext"):
            return
        offset = body.get("next")
        if not offset:
            return


def _changed_opportunities(spec, session, since):
    """[(opportunity_id, updatedAt), ...] changed since `since` (ms epoch, or
    None for "everything") — the shared worklist all nine per-opportunity
    resources fan out over.

    Cached per (spec, since) for the life of the process. After the first
    successful run, every one of the nine converges on the same watermark
    value — each one's watermark is "how far the worklist reached", which is
    identical for all nine, not anything endpoint-specific — so without this
    cache, nine resources would independently ask Lever the identical
    question nine times per run. Process-scoped, which is run-scoped under
    Airflow, same as Swoogo's `_EVENT_IDS`.

    Narrowed to `id`+`updatedAt` via `include` — confirmed against the live
    API to return exactly those two keys and nothing else — because the full
    opportunity payload (stageChanges, applications, resume metadata, ...)
    would cost real bytes on every one of the up to ~200k rows this walks on
    a first run, for fields nothing here reads.

    A known, accepted gap: an opportunity with a null `updatedAt` (the same
    rare case `build_opportunities` handles via on_cursor_value_missing) may
    never satisfy an `updated_at_start`-filtered query at all, on either this
    endpoint or `opportunities`' own. Such an opportunity has, by definition,
    never had anything happen to it since creation — including, presumably,
    a note or interview that would need fanning out — so the practical cost
    of this gap is small. It is caught in full on the very first run
    regardless, same as everything else.
    """
    key = (spec.name, since)
    cached = _WORKLIST_CACHE.get(key)
    if cached is not None:
        return cached
    endpoint = {"path": "/opportunities", "page_size": 100,
                "params": {"include": ["id", "updatedAt"]}}
    result = [(record["id"], record.get("updatedAt"))
              for record in _pages(spec, session, endpoint, since)]
    _WORKLIST_CACHE[key] = result
    return result


_WORKLIST_CACHE = {}
