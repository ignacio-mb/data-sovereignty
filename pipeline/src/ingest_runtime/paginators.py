"""Paginator shorthands, registered by name.

Three ways a spec can say how an endpoint pages, in increasing order of escape:

    a registered shorthand   `cursor` — this module expands it, reading the
                             cursor path and parameter from the spec.
    a dlt paginator name     passed through untouched, so everything dlt already
                             knows how to do stays available without this file
                             growing an entry for it.
    a raw config mapping     passed through untouched. This is the real escape
                             hatch and it already earns its keep: Swoogo's
                             page-number envelope arrived through it without a
                             line of code here.

`extension` hands paginator construction to the connector's own extension.py,
for APIs whose paging is a behaviour rather than a shape — Pylon's, which claims
`has_next_page: true` while serving nothing.
"""

from __future__ import annotations

_SHORTHANDS = {}


def paginator_shorthand(name):
    """Register an expander for `pagination.kind: <name>`.

    An expander takes (spec, resource) and returns dlt's paginator config.
    """
    def register(expander):
        _SHORTHANDS[name] = expander
        return expander
    return register


def registered():
    return tuple(sorted(_SHORTHANDS))


def build(spec, resource, extension=None):
    """Pagination for one resource: its own override, else the source default.

    Returned as dlt's config dict rather than an object wherever possible so the
    whole thing stays inspectable — printing the config is how you debug a
    connector that pages once and stops.
    """
    declared = resource.endpoint.get("paginator") or spec.pagination.get("kind")
    if not declared:
        # dlt's own detection. Right often enough to be a sensible default, and
        # a source whose paging it cannot infer will say so on the first run.
        return "auto"
    if isinstance(declared, dict):
        return declared
    if declared == "extension":
        builder = getattr(extension, "build_paginator", None) if extension else None
        if builder is None:
            raise RuntimeError(
                f"{spec.name}.{resource.name}: paginator 'extension' needs "
                f"build_paginator(spec, resource) in {spec.extension_path.name}."
            )
        return builder(spec, resource)
    expander = _SHORTHANDS.get(declared)
    if expander is not None:
        return expander(spec, resource)
    # A dlt paginator name. Not validated against dlt's own registry here: dlt
    # raises a clear error naming the unknown paginator, and duplicating its list
    # would go stale the first time dlt adds one.
    return declared


@paginator_shorthand("cursor")
def _cursor(spec, resource):
    page = spec.pagination
    endpoint = resource.endpoint
    return {
        "type": "cursor",
        "cursor_path": endpoint.get("cursor_path") or page.get("cursor_path", "meta.next_cursor"),
        "cursor_param": endpoint.get("cursor_param") or page.get("cursor_param", "cursor"),
    }
