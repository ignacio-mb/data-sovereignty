---
name: ingest
description: Load a connected source into the warehouse now, or backfill a historical date range. Triggers — "ingest the latest data", "sync Pylon now", "pull new tickets", "backfill January", "load last quarter", "we're missing data from March", "re-fetch everything", "run the pipeline".
allowed-tools: Bash, Read, AskUserQuestion
---

# Loading a source

Which source is it? `ls sources/` lists them. Everything below is per source —
each has its own DAG, its own `raw_<source>` database, and its own cursor.

## Run it through Airflow, not directly

```bash
make ingest                          # incremental, right now
make backfill START=2026-01-01       # a window; END defaults to now
make backfill START=2026-01-01 END=2026-04-01
```

Both trigger a DAG. **Do not run `ingest run --destination clickhouse` by hand
while the stack is up.** Airflow serialises each source through a pool of one; an
out-of-band run shares that source's dlt working directory and cursor, and will
interleave with a scheduled run into an inconsistent state.

The one safe direct invocation is the smoke test — a separate pipeline name that
cannot touch production state:

```bash
docker compose --profile cli run --rm airflow-cli \
  ingest run --destination duckdb --sample 3
```

## Which mode does the user actually want

Worth getting right: the two modes answer different questions, and the wrong one
silently returns incomplete data.

**Incremental** (`make ingest`) fetches records the source reports as *changed*
since the last run. This is the steady state and it is what "get the latest data"
means.

**Window** (`make backfill`) fetches records over a date range — and *which* date
depends on what the API can filter on. Read the source's spec before answering:

```bash
grep -A4 'incremental:' sources/<name>.yml
```

If `window.filters_on` is a creation timestamp, a backfill of March will **not**
pick up a January record edited in March. If the user wants "everything that
changed in March", that is not a backfill — the incremental run already covers
it. Saying so is more useful than running the wrong one.

Use `make backfill` when data is genuinely missing for a period, or the stack is
new and needs history loaded.

## Watching a run

```bash
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d <source>_ingest_hourly --limit 5
```

Or point the user at http://localhost:8080.

**Tell them how long it will take before they start.** A first backfill is bounded
by the source's rate limit, not by data volume — the spec's `rate_limits` and
`backfill_start` give you the arithmetic. A budget of 10 requests per minute
against six years of history is hours, and the pipeline paces itself to stay
under the limit rather than getting throttled. Say that upfront instead of letting
someone watch a seemingly hung task.

## After it runs

Ingestion writes a run summary into `ops.pipeline_runs`, and the DAG follows it
with that source's raw quality checkpoint. To confirm what landed, use the
`pipeline-status` skill rather than querying tables ad hoc.

## Failure modes worth recognising

- **Fails immediately on auth** — the source's token env var (see `api.auth.token_env`
  in its spec) is missing or revoked. Check it is *set*; never print it.
- **A run that ends with work still pending is often by design.** A resource
  fetched one-parent-at-a-time may carry a wall-clock budget so a scheduled job
  ends cleanly and resumes next run. Check the spec's `budget_minutes` before
  calling it a failure — repeated runs converge.
- **A run that loaded zero rows is not necessarily broken.** The cursor advances
  past an empty scan by design, so a quiet source is a legitimate no-op. Whether
  ingestion *ran* is a question for `ops.pipeline_runs`, not for row counts.
- **Pagination warnings in the logs** — some APIs return a page claiming more data
  while sending none. Where a source's extension handles that, it retries the same
  cursor a bounded number of times before giving up. An occasional one is noise; a
  run that dies on it is worth reporting upstream.

## Not this skill's job

Modeling. This skill gets raw data into `raw_<source>.*` and stops there.
Transforms, metrics and dashboards are authored elsewhere — see the router.
