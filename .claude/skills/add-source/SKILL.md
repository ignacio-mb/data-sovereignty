---
name: add-source
description: Add a new ingestion source alongside Pylon — a second API, a database, or a file drop. Triggers — "also ingest Zendesk", "add Salesforce data", "pull from our production database", "I want another source", "how do I add a new connector?".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Adding a source

The stack is built around Pylon but nothing about the shape is Pylon-specific.
A new source is a fourth uv workspace member that follows the same contract.

## Before writing any code

Ask three things, because they determine most of the design:

1. **What is the incremental key?** An `updated_at` the API can filter on is the
   good case. Only a `created_at` filter (Pylon's situation) means you need two
   modes. No cursor at all means full refresh every run — fine for small
   reference data, not for events.
2. **How does the API paginate, and what is its rate limit?** Both go into
   settings as named constants, not scattered through the code.
3. **What is the primary key, and are deletions visible?** Merge needs a stable
   key. If deletes are invisible, you need an absence-based reconciliation pass
   like Pylon's weekly one.

## The shape to copy

`pipeline/` is the worked example. Create `sources/<name>/` as a sibling
workspace member with the same layout:

```
pyproject.toml            console script, dlt[postgres]
src/<name>_pipeline/
  cli.py                  Click CLI; --destination, --summary-json
  warehouse.py            build_pipeline(); non-prod destinations get their own
                          pipeline name so smoke runs cannot touch the cursor
  ingest/
    settings.py           API URL, rate limits, page sizes, backfill epoch
    client.py             the HTTP client and its paginator
    transform.py          flatten nested JSON, parse timestamps
    hints.py              explicit dlt column hints on cursor columns only
    source.py             @dlt.source(max_table_nesting=0)
tests/conftest.py         requests-mock that implements the API's real semantics
```

Then: add the member to the root `pyproject.toml`, symlink its console script in
`docker/airflow/Dockerfile`, and add a DAG modelled on `pylon_ingest_hourly`.

## The four patterns worth carrying over verbatim

**`EndpointPacer`** (`pipeline/src/pylon_pipeline/ingest/pacing.py`) — space
requests proactively per endpoint family rather than hammering until a 429.
Thirty lines, no dependencies, source-agnostic. Copy it as is.

**`max_table_nesting=0` plus explicit flattening** — stringify nested objects
and promote only the fields you actually query. This is what stops a new
user-defined field in the upstream tool from minting a surprise warehouse column
or child table.

**Hint only the load-bearing columns** — cursor timestamps and boolean flags. Let
everything else be inferred. Type drift on a cursor column silently breaks
incremental loading.

**A mock that implements the API's semantics, not canned responses** — the Pylon
conftest actually filters by timestamp and paginates. That is what makes the
end-to-end CLI tests worth running.

## The rest of the stack

Adding a source is not finished when data lands:

- **Quality** — a new suite module in `quality/src/pylon_quality/suites/`, and a
  checkpoint name in `cli.py`. Identity, freshness and referential checks.
- **Modeling** — `base_<source>_*` transforms, then conform into the existing
  `dim_*` where the entities overlap. Two sources describing the same accounts
  is the interesting case and the reason `dim_account` exists.
- **The pool** — reuse `pylon_pipeline` only if the sources share a rate limit
  budget. Otherwise give the new source its own pool so one slow backfill does
  not block the other's hourly run.

## Rename first

If a second source is going in, `pylon_quality` and the `pylon_pipeline` pool
become misnomers. Renaming is cheap now and annoying later.
