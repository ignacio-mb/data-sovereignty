"""Turn a source spec into things that fetch data.

This is the part that used to be per-connector Python. A spec says what the
endpoints are, how they page, which columns are timestamps and which nested
scalars matter; the functions here turn that into dlt column hints, a rate-limit
pacer, and a record transformer. What they deliberately do NOT do is invent
behaviour the spec did not describe.

Three fetch strategies exist, and they are code rather than configuration
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

A connector needing something none of these describes writes an extension
module. That is a deliberate seam, not a failure — see extensions().
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
    """
    return EndpointPacer(spec.rate_limits)


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
    raise RuntimeError(
        f"{spec.name}: auth type {kind!r} is not one dlt can build. "
        f"Known: bearer, api_key, http_basic. Anything else needs an extension."
    )


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


def build_source(spec, selected=None, extension=None):
    """A dlt source for `spec`, covering the resources in `selected`.

    Resources whose strategy the declarative layer cannot express are handed to
    the extension module. If the spec names one and it does not supply the
    resource, that is an error rather than a silent omission: a connector that
    quietly skips an endpoint looks exactly like one whose source has no data.
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
        sources.append(rest_api_source({
            "client": {
                "base_url": spec.base_url,
                "auth": _auth(spec),
                "headers": spec.api.get("headers") or {},
            },
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
        sources.append(builder(spec, resource))

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
    page_size = resource.endpoint.get("page_size")
    if page_size:
        endpoint["params"] = {"limit": page_size}

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
