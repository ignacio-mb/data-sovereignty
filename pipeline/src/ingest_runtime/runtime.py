"""Turn a source spec into things that fetch data.

This is the part that used to be per-connector Python. A spec says what the
endpoints are, how they page, which columns are timestamps and which nested
scalars matter; the functions here turn that into dlt column hints, a rate-limit
pacer, and a record transformer. What they deliberately do NOT do is invent
behaviour the spec did not describe.

Five fetch strategies exist. Two are built here, from configuration alone:

    full_refresh      pull the whole collection every run and merge on the key.
                      Right for small entity sets; it is also the only strategy
                      under which absence is meaningful, so it is what makes
                      soft-delete reconciliation possible.
    cursor            an incremental cursor pushed into the API's own query
                      parameter, so the server sends only what changed. dlt
                      tracks the high-water mark in pipeline state and binds it
                      to the request; the spec supplies the record field, the
                      parameter name, and optionally a lookback. This is the
                      shape most REST APIs actually have, and a connector using
                      it needs no Python at all.

The other three name an algorithm this module recognises but does not implement.
A spec declaring one must ship an `extension.py` beside it supplying the fetch,
and build_source raises if it does not:

    search_window     an incremental cursor over a filterable field, with a
                      separate windowed endpoint for backfill. Needed whenever
                      the API filters its list endpoint on creation time but
                      offers a search endpoint filtering on update time — the
                      two answer different questions and using the wrong one
                      silently misses rows.
    parent_watermark  no cross-entity endpoint exists, so the worklist comes
                      from the warehouse: children whose parent changed more
                      recently than the newest child already loaded.
    parent_fanout     the child endpoint exists but is scoped to one parent, so
                      it must be called once per parent. Distinct from
                      parent_watermark in what drives the worklist: EVERY
                      in-scope parent is visited each run, and the child's own
                      cursor bounds what comes back. Using the watermark rule
                      here would lose rows, because a child changing does not
                      have to touch its parent's updated_at — a new registrant
                      does not modify the event it registered for.

Recognising the three without implementing them still earns its keep: it is what
makes the failure "you owe this connector a fetch function" instead of an
unknown-strategy error, and it keeps the soft-delete rules (which differ by
strategy) expressible in the spec.

Auth types and paginator shorthands are registries (`auth.py`, `paginators.py`)
rather than branches here, so a new one is an entry at the edge instead of a diff
through the middle of this file.
"""

from __future__ import annotations

import logging

from .ingest.pacing import EndpointPacer
from .ingest.transform import flatten_record, strip_html
from .spec import DECLARATIVE_STRATEGIES

log = logging.getLogger(__name__)

_TIMESTAMP = {"data_type": "timestamp", "precision": 6}
_BOOL = {"data_type": "bool"}

# Re-exported under the old name: the CLI reads it to warn that --start/--end do
# not reach a delegated resource.
_DECLARATIVE_STRATEGIES = DECLARATIVE_STRATEGIES


def pacer(spec):
    """Spread requests across each endpoint family's published budget.

    Proactive rather than reactive: the limits come from the spec, which the
    add-source research step fills in from the API's own documentation, so the
    pacer never has to discover them by being told to stop.

    Constructing one paces nothing by itself — it has to reach the request path,
    which is what `paced_session` is for. That seam was briefly open: the CLI built
    a pacer, handed it to nothing, and reported its (always empty) counters in the
    run summary, so the stack claimed to pace itself and did not.
    """
    return EndpointPacer(spec.rate_limits)


def _routes(spec):
    """[(path, family)] longest path first, so a more specific route wins.

    A family groups endpoints sharing one published budget; a resource declaring
    none gets its own name, and so its own budget.

    Every endpoint a resource may call, not just its primary one: a strategy with
    a separate backfill or search endpoint bills those separately, and reading
    only the first route sent them to whichever family happened to prefix-match.
    Longest-first ordering is what then keeps `/issues/search` from being
    swallowed by `/issues`.
    """
    routes = [
        (_match_prefix(endpoint.get("path", f"/{resource.name}")), family)
        for resource in spec.resources
        for endpoint, family in resource.all_endpoints
    ]
    routes.sort(key=lambda route: len(route[0]), reverse=True)
    return routes


def _match_prefix(path):
    """The literal part of a path template, up to its first placeholder.

    A templated path never appears in a real URL: `/issues/{issue_id}/messages`
    is not a substring of `/issues/abc123/messages`, so matching it whole means
    every request to it falls through to whatever shorter route happens to
    prefix-match — `/issues`, billed to the list endpoint's budget. The literal
    prefix (`/issues/`) matches, and longest-first ordering keeps it from
    swallowing siblings like `/issues/search`.
    """
    return path.split("{", 1)[0]


def _family_for(url, routes):
    from urllib.parse import urlparse

    path = urlparse(url).path
    for route, family in routes:
        if route and route in path:
            return family
    # Counted under a name that says what happened rather than dropped: a summary
    # showing requests against an unrecognised route is how you find out a
    # paginator is following a link nobody declared.
    return "unmatched"


def paced_session(spec, pacer):
    """dlt's own session, with the spec's rate limits applied before each request.

    Built from `dlt.sources.helpers.requests.retry.Client` — exactly what dlt would
    have constructed for itself — so the retry, timeout and backoff behaviour
    configured through the RUNTIME__REQUEST_* variables is preserved. Passing a
    bare `requests.Session` here would silently drop all of it.

    Pacing wraps `send`, so it covers every request the declarative source makes,
    including the paginator's follow-ups. Adapter-level retries of a single request
    are not re-paced: a retry carries its own backoff, and this budget exists to
    make 429s rare rather than to govern recovery from one.
    """
    from dlt.sources.helpers.requests.retry import Client

    session = Client(raise_for_status=False).session
    routes = _routes(spec)
    send = session.send

    def paced_send(request, **kwargs):
        pacer.wait(_family_for(request.url, routes))
        return send(request, **kwargs)

    session.send = paced_send
    return session


def session_for(spec, paced=None, extension=None):
    """An authed, paced session for an extension that fetches by hand.

    The declarative path hands its auth config to dlt's rest_api client, which
    knows how to turn a mapping into a request signer. An extension driving a
    bare session has no such layer, so this converts the same spec-declared auth
    into something `requests` can use — every registered type, not just the one
    the first extension happened to need. Swoogo's fan-out used to require
    oauth2 for exactly that reason: it received a config dict for anything else
    and could do nothing with it.
    """
    from dlt.sources.helpers.rest_client import auth as rest_auth

    from .auth import build as build_auth

    if paced is not None:
        session = paced_session(spec, paced)
    else:
        from dlt.sources.helpers.requests.retry import Client

        session = Client(raise_for_status=False).session

    resolved = build_auth(spec, extension=extension)
    if callable(resolved):
        session.auth = resolved
        return session

    kind = resolved.get("type")
    if kind == "bearer":
        session.auth = rest_auth.BearerTokenAuth(token=resolved["token"])
    elif kind == "api_key":
        session.auth = rest_auth.APIKeyAuth(
            name=resolved["name"], api_key=resolved["api_key"], location=resolved["location"])
    elif kind == "http_basic":
        session.auth = rest_auth.HttpBasicAuth(
            username=resolved["username"], password=resolved["password"])
    else:  # pragma: no cover - a registered type with no session equivalent
        raise RuntimeError(
            f"{spec.name}: auth type {kind!r} produced a config dict this session "
            f"cannot apply. Return a callable request signer from the extension's "
            f"build_auth(spec) instead."
        )
    return session


def column_hints(resource):
    """dlt column hints for exactly the load-bearing columns.

    Not everything — inference is fine for the rest, and over-hinting means
    every schema change is a code change. What must be hinted is anything the
    incremental logic compares: a cursor that dlt typed as text would compare
    lexicographically, which mostly works and then does not. The tombstone flag
    is hinted for the same reason: the soft-delete predicate tests it.
    """
    hints = {column: dict(_TIMESTAMP) for column in resource.hint_columns}
    if resource.soft_delete:
        hints["_deleted"] = dict(_BOOL)
    return hints


def endpoint_params(resource, endpoint=None):
    """Query parameters an endpoint always needs, page size included.

    `limit` is only the most common spelling of page size, not a universal one,
    and some APIs return a sparse projection unless asked for the columns by name
    — an omitted `fields` there costs you the cursor column rather than an error.

    Shared with extensions: the fan-out path assembled the same two things by
    hand, so a spec that changed its page-size parameter fixed the declarative
    resources and left the delegated ones on the old spelling.
    """
    endpoint = endpoint if endpoint is not None else resource.endpoint
    params = dict(endpoint.get("params") or {})
    page_size = endpoint.get("page_size")
    if page_size:
        params[endpoint.get("page_size_param", "limit")] = page_size
    return params


def make_transformer(resource):
    """A callable turning one API record into the row that lands.

    Nested objects are JSON-stringified rather than exploded, and only the
    scalars the spec promotes become real columns. That is what stops a new
    custom field on the source side from silently minting a warehouse column —
    or, worse, a whole child table — on the next run.
    """
    promotions = resource.promote
    html_fields = resource.html_text
    timestamps = resource.timestamp_columns
    tombstoned = resource.soft_delete

    def transform(record):
        row = flatten_record(record, timestamps)
        for path, target in promotions.items():
            row[target] = _dig(record, path)
        for source_field, target in html_fields.items():
            row[target] = strip_html(record.get(source_field) or "")
        if tombstoned:
            # Present from the first load so the column is never null-typed;
            # the reconciliation pass only ever flips it to true.
            row.setdefault("_deleted", False)
        return row

    return transform


def _dig(record, dotted):
    """`account.id` -> record["account"]["id"], or None at any missing step."""
    current = record
    for part in dotted.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def extensions(spec):
    """The connector's own module, loaded from beside its spec, or None."""
    from .extensions import load

    return load(spec)


# ── Building the actual dlt source ───────────────────────────────────────────
#
# dlt ships a declarative REST source, and a spec is very nearly its config
# already. Using it rather than a hand-written client is what makes a connector
# a file: pagination, auth and incremental filtering are dlt's problem, and the
# spec only has to name which of its behaviours apply.


def build_source(spec, selected=None, extension=None, paced=None):
    """A dlt source for `spec`, covering the resources in `selected`.

    Resources whose strategy the declarative layer cannot express are handed to
    the extension module. If the spec names one and it does not supply the
    resource, that is an error rather than a silent omission: a connector that
    quietly skips an endpoint looks exactly like one whose source has no data.

    `paced` is the run's EndpointPacer. Passing it is what makes the spec's
    `rate_limits` take effect; without it the source fetches as fast as the API
    will answer.
    """
    from dlt.sources.rest_api import rest_api_source

    from .auth import build as build_auth
    from .extensions import builder_for

    extension = extension if extension is not None else extensions(spec)
    wanted = list(selected or spec.resource_names)

    declarative, delegated = [], []
    for name in wanted:
        resource = spec.resource(name)
        (declarative if resource.is_declarative else delegated).append(resource)

    if delegated and extension is None:
        names = ", ".join(r.name for r in delegated)
        raise RuntimeError(
            f"{spec.name}: {names} need a fetch strategy the declarative config "
            f"cannot express, but the spec declares no `extensions: true` (and no "
            f"{spec.extension_path.name} beside it)."
        )

    sources = []
    if declarative:
        client = {
            "base_url": spec.base_url,
            "auth": build_auth(spec, extension=extension),
            "headers": spec.api.get("headers") or {},
        }
        if paced is not None:
            client["session"] = paced_session(spec, paced)
        sources.append(rest_api_source({
            "client": client,
            "resource_defaults": {"write_disposition": "merge"},
            "resources": [_resource_config(spec, r, extension) for r in declarative],
        }, name=spec.name))

    for resource in delegated:
        builder = builder_for(spec, resource, extension)
        if builder is None:
            raise RuntimeError(
                f"{spec.name}: {spec.extension_path.name} supplies neither "
                f"build_{resource.name}() nor build_resource() for "
                f"{resource.name!r} (strategy {resource.strategy})."
            )
        # The pacer goes to the extension too. Without this a delegated resource
        # is silently unpaced while the run still reports the budget as applied —
        # the same failure the declarative path already had once, and worse here,
        # because a source whose every resource is delegated would publish a rate
        # limit it never once obeyed.
        sources.append(builder(spec, resource, paced=paced))

    return sources


def _incremental_config(resource):
    """dlt's endpoint incremental, for the `cursor` strategy.

    dlt binds the high-water mark from pipeline state into the request itself, so
    the server filters instead of us discarding — the difference between an
    hourly run that reads yesterday and one that reads a year.

    `lookback_seconds` becomes dlt's `lag`: re-read a little of what was already
    loaded so records written while the previous run was mid-fetch are not
    skipped. Free under merge, which is the default disposition.
    """
    incremental = resource.incremental
    config = {
        "cursor_path": incremental["cursor_field"],
        "start_param": incremental["cursor_param"],
    }
    for spec_key, dlt_key in (("end_param", "end_param"),
                              ("initial_value", "initial_value"),
                              ("lookback_seconds", "lag")):
        value = incremental.get(spec_key)
        if value is not None:
            config[dlt_key] = value
    return config


def _resource_config(spec, resource, extension=None):
    """One resource, as dlt's EndpointResource."""
    from .paginators import build as build_paginator

    endpoint = {
        "path": resource.endpoint.get("path", f"/{resource.name}"),
        "method": resource.endpoint.get("method", "GET"),
        "data_selector": resource.data_selector(spec),
        "paginator": build_paginator(spec, resource, extension),
    }
    params = endpoint_params(resource)
    if params:
        endpoint["params"] = params
    body = resource.endpoint.get("json")
    if body:
        endpoint["json"] = body
    if resource.strategy == "cursor":
        endpoint["incremental"] = _incremental_config(resource)

    return {
        "name": resource.name,
        "primary_key": resource.primary_key,
        "write_disposition": resource.write_disposition,
        "columns": column_hints(resource),
        # Nested objects are stringified by the transformer rather than exploded
        # into child tables, so a new field upstream cannot mint a table here.
        "max_table_nesting": 0,
        "endpoint": endpoint,
        "processing_steps": [{"map": make_transformer(resource)}],
    }
