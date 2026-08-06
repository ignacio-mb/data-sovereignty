# The extension contract

Most connectors need no Python. `source.yml` describes the endpoints, the
paging, the incremental filter and the expectations, and the runtime builds a
dlt source from it — that is what "a connector is a spec, not a module" means,
and it is the case worth optimising for.

This file is about the minority that cannot be described that way, and what they
owe the runtime in exchange for the escape hatch.

## When you need one

Only when a resource's **fetch algorithm** is not one the declarative layer
builds. Two are built with no code:

| strategy | what it does |
|---|---|
| `full_refresh` | pull the whole collection each run and merge on the key. The only strategy under which absence is meaningful, so it is what makes soft-delete reconciliation possible. |
| `cursor` | push the high-water mark into the API's own query parameter, so the server sends only what changed. `cursor_field` → dlt's cursor path, `cursor_param` → the filter parameter, optional `end_param`, `initial_value`, `lookback_seconds`. |

Three name an algorithm the runtime recognises and does not implement. Declaring
one obliges you to supply it:

| strategy | why configuration cannot express it |
|---|---|
| `search_window` | two endpoints for one table, filtered on different columns. Steady state uses the one that filters on update time; backfill walks the one that filters on creation time, in slices the API will answer. |
| `parent_watermark` | no cross-entity endpoint exists, so the worklist is a warehouse query: children whose parent changed more recently than the newest child already loaded. |
| `parent_fanout` | the child endpoint is scoped to one parent, so it is called once per parent. Every in-scope parent is visited each run; the child's own cursor bounds what comes back. |

Auth and pagination have their own valves, for the same reason and with the same
bar: `api.auth.type: extension` (supply `build_auth`) and `paginator: extension`
(supply `build_paginator`). Reach for those only when the behaviour is one API's
peculiarity. A scheme two APIs share belongs in the registries — `auth.py` and
`paginators.py` — where it is one decorated function and its tests.

Read `sources/pylon/` before writing your own. It is the reference: two
delegated strategies, a paginator that lies, and everything else declarative.

## Where it lives

`sources/<name>/extension.py`, beside the spec that declares it. The spec says
`extensions: true` — nothing more, because the loader derives the path and a
module name would only be something for the file to drift from.

The loader (`ingest_runtime/extensions.py`) executes the file by path under the
synthetic package `ds_source_ext.<name>`. Consequences worth knowing:

- **No relative imports into the runtime.** The module is not part of the
  `ingest_runtime` package. Import the public surface instead:
  `from ingest_runtime.extension_api import ...`. Everything not exported there
  is internal and may move.
- **The file is executed once per process** and cached by path, so module-level
  state is run-scoped under Airflow (one task, one process). Define `reset()`
  if you keep any — see below.
- **`sources/` is mounted read-only** in the containers and baked into the
  image. Do not write beside the spec at runtime.

## What a builder must return

```python
def build_<resource_name>(spec, resource, paced=None) -> dlt source
def build_resource(spec, resource, paced=None) -> dlt source   # the fallback
```

The runtime looks for `build_<resource>` first, then `build_resource`. A
connector whose twelve endpoints differ only in data the spec already carries
writes one function; one that genuinely differs writes several.

**Return a dlt _source_, not a bare resource.** Everything downstream — the
CLI's `--sample` printers, the run summary's row counts — walks `.resources`,
which a `DltResource` does not have. Wrap it:

```python
@dlt.source(name=spec.name)
def _source():
    return my_resource

return _source()
```

## Obligations

These are not style. Each one is a failure that has actually happened here.

1. **Route every request through the session you were given.**
   `session_for(spec, paced)` returns dlt's retrying client with the spec's rate
   budget wrapped around `send`, already authed for any registered auth type.
   Building a bare `requests.Session` silently discards the pacing, the retry
   policy and the timeouts — and the run still reports the budget as applied.

2. **Set an explicit timeout on every request.** `requests` defaults to *no*
   timeout. A call that never answers blocks the generator forever while holding
   the source's pool slot of one: not a slow run, a stopped pipeline, whose only
   symptom is a task that never finishes.

3. **Use `endpoint_params(resource, endpoint)`** rather than assembling page
   size by hand. Page size and its parameter name are spec facts, and a copy
   here keeps the old spelling the day the spec changes it.

4. **Use `make_transformer(resource)`** for the record shape. It applies
   `promote`, `html_text`, timestamp parsing and the tombstone default. Yielding
   raw API records instead is how a new upstream field mints a warehouse column.

5. **`--start`/`--end` do not reach you.** `build_source` takes no window, so an
   extension bounds itself by its own cursor. The CLI warns about this and
   refuses to tombstone a delegated resource for exactly that reason. If your
   strategy has a backfill path, drive it from the absence of a cursor (see
   `build_issues` in the Pylon reference), not from flags you will never see.

6. **Fail loudly on a short read.** Returning what you collected so far, when
   you know there was more, is the one failure this stack exists to prevent: it
   is indistinguishable from a source that had no data. Raise.

7. **Define `reset()` if you keep run-scoped state.** The loader calls it
   between runs and the test harness calls it between cases. Without it, a
   cached worklist leaks from one run into the next and tests reach into your
   module to clear it by hand.

## Optional hooks

```python
def build_auth(spec)                      # api.auth.type: extension
def build_paginator(spec, resource)       # endpoint.paginator: extension
def reset()                               # drop run-scoped caches
```

## Reading the warehouse

`parent_watermark` needs to ask what is already loaded. Use
`warehouse_rows(build_query)`: it hands your callable the table qualifier, so
the same query works against ClickHouse and against the duckdb smoke
destination, and it returns no rows rather than raising when a table does not
exist yet — which is every connector's first run.

```python
from ingest_runtime.extension_api import warehouse_rows

def _pending(parent, child):
    def query(qualified):
        return f"SELECT id FROM {qualified(parent)} WHERE ..."
    return [row[0] for row in warehouse_rows(query)]
```

## Proving it

`ingest validate --source <name>` checks that the extension exists, imports, and
supplies a builder for every delegated resource. The generic contract suite then
builds a source from every spec — reference specs included — and loads it into
duckdb against `fixtures/`, so an extension that returns the wrong shape fails in
CI rather than at the first `--sample`.

Anything the generic mock cannot express (an auth endpoint, a sparse projection,
a page that lies) goes in `fixtures/server.py`, which the harness imports and
calls as `register(mock, spec, fixtures)`.
