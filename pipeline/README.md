# ingest-runtime

Spec-driven ingestion into ClickHouse, built on [dlt](https://dlthub.com) 1.x.

```
any REST API ──ingest (merge on the spec's primary key)──▶ ClickHouse, database raw_<source>
```

**Nothing here is API-specific**, and that is now literally true: the one
per-connector module that used to live in this package (`sources/swoogo.py`) sits
beside its spec instead. A connector is `sources/<name>/source.yml`: what the
endpoints are, how they page, which fields are timestamps, what is incremental,
when it runs, and what "arrived correctly" means. The runtime turns that into dlt
column hints, a rate-limit pacer, a record transformer and a REST source.
`sources/pylon/` is a complete worked example.

Which connectors schedule is `status:` in each spec, cross-checked against
`sources/CONNECTED`. See [`docs/sources.md`](../docs/sources.md), which is
generated.

## Five fetch strategies

Strategies are code rather than configuration because each is an algorithm, not a
setting.

**`full_refresh`** pulls the whole collection every run and merges on the key. Right
for small entity sets, and the only strategy under which absence is meaningful —
which is what makes soft-delete reconciliation possible. **It is also the one the
declarative layer builds by itself**, so a spec using only `full_refresh` needs no
Python at all.

**`search_window`** is an incremental cursor over a filterable field, with a separate
windowed endpoint for backfill. Needed when an API filters its list endpoint on
creation time but offers a search endpoint filtering on update time — the two answer
different questions, and using the wrong one silently misses rows.

**`parent_watermark`** covers the case where no cross-entity endpoint exists, so the
worklist comes from the warehouse: children whose parent changed more recently than
the newest child already loaded. It is evaluated at extract time, after this run's
parents have landed.

**`parent_fanout`** is for a child endpoint that exists but is scoped to one parent,
so it has to be called once per parent — Swoogo's `/registrants?event_id=…`. It
differs from `parent_watermark` in what drives the worklist: EVERY in-scope parent is
visited each run, and the child's own cursor bounds what comes back. The watermark
rule would lose rows here, because a child changing need not touch its parent's
`updated_at` — a new registrant does not modify the event it registered for.
`incremental.cursor_field` is optional: omit it and the resource is re-read in full
per parent, which is the honest answer for an endpoint whose timestamps are mostly
null.

The last three are **not** built declaratively: a spec declaring one must also declare
an `extensions:` module and supply the Python that fetches it. `build_source` raises
rather than skipping the resource — a connector that quietly drops an endpoint looks
exactly like one whose source has no data.

## Usage

```bash
uv run ingest sources                                              # what is connected
uv run ingest run --source <name>                                  # incremental
uv run ingest run --source <name> --start 2026-01-01 --end 2026-02-01
uv run ingest run --source <name> --destination duckdb --sample 3  # local smoke test
```

Whether `--start/--end` filters on creation or update time is a property of the API
and is written in the spec — read it before promising what a backfill covers.

While the stack is up, ingest through Airflow (`make ingest SOURCE=<name>`) rather
than directly: each source has a pool of one, and two concurrent runs would race its
incremental cursor. The duckdb destination is always safe — separate dlt pipeline
name, so it cannot touch production state.

## Things that will bite you

- **`--mark-deleted` tombstones anything absent from the run's loads.** A resource
  marked `soft_delete: full_history` is only eligible when the run covered all of
  history — `--start` at or before the spec's `backfill_start`, and an end at roughly
  now. `soft_delete: always` resources are eligible every run because they are fully
  re-fetched. The guard lives in `cli.py`; applying `always` to a resource that is
  only ever fetched incrementally would tombstone everything outside the window.
- **A crashed run leaves a pending load package.** `pipeline.run(source)` would load
  *that* package and return without extracting, silently skipping this run's fetch.
  The CLI drains it first and excludes its load id from the soft-delete.
- **Nested JSON is stringified, not exploded.** `max_table_nesting=0` plus the spec's
  explicit `promote` list means a new custom field upstream can never mint a surprise
  column or child table. Promote the ones you need, by name.
- **Rate limits only apply if the pacer reaches the request path.** `build_source`
  takes `paced=` and installs a session that waits on the spec's `rate_limits` before
  each request. Constructing an `EndpointPacer` and not passing it paces nothing —
  and looks fine, because the run summary still reports its (empty) counters.
- **The database must exist before dlt connects.** ClickHouse selects it while
  connecting, so `ensure_database` runs first; `warehouse/init/` cannot cover it
  because that only runs once, on an empty volume.

## Tests

`uv run pytest` from the repo root. Everything is mocked through `requests-mock`; no
network, no credentials. `test_build_source.py` drives a spec with no Python end to
end into duckdb — two pages, so the paginator is genuinely exercised — which is what
licenses the runtime having no per-API code in it.
