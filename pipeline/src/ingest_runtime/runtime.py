"""Turn a source spec into things that fetch data.

This is the part that used to be per-connector Python. A spec says what the
endpoints are, how they page, which columns are timestamps and which nested
scalars matter; the functions here turn that into dlt column hints, a rate-limit
pacer, and a record transformer. What they deliberately do NOT do is invent
behaviour the spec did not describe.

Four fetch strategies exist, and they are code rather than configuration
because each is an algorithm, not a setting:

    full_refresh      pull the whole collection every run and merge on the key.
                      Right for small entity sets; it is also the only strategy
                      under which absence is meaningful, so it is what makes
                      soft-delete reconciliation possible.
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

**Only `full_refresh` is built here** — see _DECLARATIVE_STRATEGIES. dlt's
declarative REST source covers it end to end, so a spec using nothing else needs
no Python whatsoever, which is the case worth optimising for.

The other three name an algorithm this module recognises but does not implement: a
spec declaring one must also declare an `extensions:` module supplying it, and
build_source raises if it does not. Recognising the name still earns its keep —
it is what makes the failure "you owe this connector a fetch function" instead of
an unknown-strategy error, and it keeps the soft-delete rules (which differ by
strategy) expressible in the spec.

A connector needing something none of these describes writes an extension module
too. That is a deliberate seam, not a failure — see extensions().
"""

from __future__ import annotations

import importlib
import logging

from .ingest.pacing import EndpointPacer
from .ingest.transform import flatten_record, strip_html

log = logging.getLogger(__name__)

_TIMESTAMP = {"data_type": "timestamp", "precision": 6}
_BOOL = {"data_type": "bool"}


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
    """
    routes = [
        (resource.endpoint.get("path", f"/{resource.name}"), resource.family)
        for resource in spec.resources
    ]
    routes.sort(key=lambda route: len(route[0]), reverse=True)
    return routes


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
    """The module holding what the spec could not express, or None.

    Every connector wants to believe it is ordinary. Pylon is the case that
    proves otherwise: it returns pages claiming `has_next_page: true` while
    carrying no data, and its messages have no cross-issue endpoint, so the
    worklist is a warehouse query rather than an API cursor. Neither is
    expressible as configuration, and pretending otherwise would mean either a
    config language that grows a branch per API, or a connector that quietly
    loses rows.

    So the escape hatch is explicit and named in the spec. A source with none of
    this has no Python at all.
    """
    if not spec.extensions:
        return None
    module = f"{__package__}.sources.{spec.extensions}"
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as error:
        raise RuntimeError(
            f"{spec.name} declares extensions: {spec.extensions!r} but {module} "
            f"does not exist. Either write it or drop the key from the spec."
        ) from error


# ── Building the actual dlt source ───────────────────────────────────────────
#
# dlt ships a declarative REST source, and a spec is very nearly its config
# already. Using it rather than a hand-written client is what makes a connector
# a file: pagination, auth and incremental filtering are dlt's problem, and the
# spec only has to name which of its behaviours apply.


def _auth(spec):
    """Auth block for dlt, reading the secret from the env var the spec names.

    The token is never in the spec — only the NAME of the variable holding it.
    That is what lets a spec live in a public repo.
    """
    import os

    auth = spec.api["auth"]
    kind = auth["type"]
    token = os.environ.get(auth["token_env"], "").strip()
    if not token:
        raise RuntimeError(
            f"{spec.name}: {auth['token_env']} is not set. "
            f"Add it to .env (never to the spec)."
        )
    if kind == "bearer":
        return {"type": "bearer", "token": token}
    if kind == "api_key":
        return {"type": "api_key", "api_key": token,
                "name": auth.get("header", "Authorization"), "location": "header"}
    if kind == "http_basic":
        return {"type": "http_basic", "username": auth.get("username", token),
                "password": auth.get("password", token)}
    if kind == "oauth2_client_credentials":
        return _client_credentials(spec, auth, token)
    raise RuntimeError(
        f"{spec.name}: auth type {kind!r} is not one dlt can build. "
        f"Known: bearer, api_key, http_basic, oauth2_client_credentials. "
        f"Anything else needs an extension."
    )


def _client_credentials(spec, auth, credential):
    """OAuth2 client-credentials, for APIs whose token expires mid-run.

    The other three types are static: the env var holds the value that goes on
    every request. Here it holds the *credential used to mint* short-lived
    bearer tokens, so the auth object has to outlive any single request and
    re-mint on expiry. dlt's OAuth2ClientCredentials already does that; the only
    thing it does not do is put the credential where a given API wants it.

    Two placements, because both are common:

      body          client_id/client_secret as form fields — dlt's default, and
                    what most providers document.
      basic_header  the pair pre-joined and Base64'd into `Authorization: Basic`.
                    `token_env` then holds that finished string rather than
                    either half, which is deliberate: the encoding is
                    `base64(urlencode(id):urlencode(secret))`, and hand-rolling
                    it silently produces a non-working credential whenever the
                    secret contains a reserved character. Providers that want
                    this generally show the encoded value in their UI, so taking
                    it verbatim removes the step that can be got wrong.

    One object serves the whole run, cached per source. Both the declarative
    path and every extension builder ask for auth independently, so without the
    cache a connector with twelve delegated resources mints thirteen tokens per
    run — each with its own expiry clock, each spending from the same budget the
    pacer is trying to protect.
    """
    from dlt.common.configuration import configspec
    from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials

    cached = _TOKEN_SOURCES.get(spec.name)
    if cached is not None:
        return cached

    token_url = auth.get("token_url")
    if not token_url:
        raise RuntimeError(
            f"{spec.name}: auth type oauth2_client_credentials needs `token_url` "
            f"— the endpoint that exchanges the credential for a bearer token."
        )
    placement = auth.get("credentials_in", "body")
    extra = dict(auth.get("token_request_data") or {})

    if placement == "body":
        return _TOKEN_SOURCES.setdefault(spec.name, OAuth2ClientCredentials(
            access_token_url=token_url,
            client_id=auth.get("client_id", ""),
            client_secret=credential,
            access_token_request_data=extra,
        ))
    if placement != "basic_header":
        raise RuntimeError(
            f"{spec.name}: credentials_in must be 'body' or 'basic_header', "
            f"got {placement!r}."
        )

    # Content-Type carries the charset because some token endpoints (Swoogo's
    # among them) reject the bare form media type.
    #
    # @configspec is not decoration: dlt's rest_api runs every auth object
    # through resolve_configuration, which rejects anything that is not one.
    @configspec
    class _BasicHeaderCredentials(OAuth2ClientCredentials):
        def build_access_token_request(self):
            return {
                "headers": {
                    "Authorization": f"Basic {credential}",
                    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                },
                "data": {"grant_type": "client_credentials", **extra},
            }

    # client_id/client_secret are inherited required fields, and dlt resolves
    # them before it ever calls the override that ignores them — leave them
    # unset and resolution goes looking in secrets.toml and fails. The
    # credential is the encoded pair, so it belongs in client_secret; there is
    # no separate id to give.
    return _TOKEN_SOURCES.setdefault(spec.name, _BasicHeaderCredentials(
        access_token_url=token_url,
        client_id="unused-in-basic-header-mode",
        client_secret=credential,
    ))


# Per-source token sources, alive for the length of the process. An Airflow task
# is a fresh process per run, so this is a run-scoped cache in practice.
_TOKEN_SOURCES = {}


def _paginator(spec, resource):
    """Pagination, resource-level override falling back to the source default.

    Returned as dlt's config dict rather than an object so the whole thing stays
    inspectable — printing the config is how you debug a connector that pages
    once and stops.
    """
    declared = resource.endpoint.get("paginator") or spec.pagination.get("kind")
    if not declared:
        # dlt's own detection. Right often enough to be a sensible default, and
        # a source whose paging it cannot infer will say so on the first run.
        return "auto"
    if isinstance(declared, dict):
        return declared
    if declared == "cursor":
        page = spec.pagination
        return {
            "type": "cursor",
            "cursor_path": page.get("cursor_path", "meta.next_cursor"),
            "cursor_param": page.get("cursor_param", "cursor"),
        }
    return declared


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

    extension = extension if extension is not None else extensions(spec)
    wanted = list(selected or spec.resource_names)

    declarative, delegated = [], []
    for name in wanted:
        resource = spec.resource(name)
        if resource.strategy in _DECLARATIVE_STRATEGIES:
            declarative.append(resource)
        else:
            delegated.append(resource)

    if delegated and extension is None:
        names = ", ".join(r.name for r in delegated)
        raise RuntimeError(
            f"{spec.name}: {names} need a fetch strategy the declarative config "
            f"cannot express, but the spec declares no `extensions` module."
        )

    sources = []
    if declarative:
        client = {
            "base_url": spec.base_url,
            "auth": _auth(spec),
            "headers": spec.api.get("headers") or {},
        }
        if paced is not None:
            client["session"] = paced_session(spec, paced)
        sources.append(rest_api_source({
            "client": client,
            "resource_defaults": {"write_disposition": "merge"},
            "resources": [_resource_config(spec, r) for r in declarative],
        }, name=spec.name))

    for resource in delegated:
        builder = getattr(extension, f"build_{resource.name}", None) or \
            getattr(extension, "build_resource", None)
        if builder is None:
            raise RuntimeError(
                f"{spec.name}: extension module supplies neither "
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


_DECLARATIVE_STRATEGIES = {"full_refresh"}


def _resource_config(spec, resource):
    """One resource, as dlt's EndpointResource."""
    endpoint = {
        "path": resource.endpoint.get("path", f"/{resource.name}"),
        "method": resource.endpoint.get("method", "GET"),
        "data_selector": spec.pagination.get("data_selector"),
        "paginator": _paginator(spec, resource),
    }
    # Query parameters the endpoint always needs. `limit` is only the most
    # common spelling of page size, not a universal one, and some APIs return a
    # sparse projection unless asked for the columns by name — an omitted
    # `fields` there costs you the cursor column rather than an error.
    params = dict(resource.endpoint.get("params") or {})
    page_size = resource.endpoint.get("page_size")
    if page_size:
        params[resource.endpoint.get("page_size_param", "limit")] = page_size
    if params:
        endpoint["params"] = params

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
