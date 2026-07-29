---
name: ingest
description: Pull Pylon data into the warehouse now, or backfill a historical date range. Triggers — "ingest Pylon data", "pull the latest tickets", "backfill January", "load last quarter", "we're missing data from March", "re-fetch everything".
allowed-tools: Bash, Read, AskUserQuestion
---

# Ingesting Pylon data

## Run it through Airflow, not directly

```bash
make ingest                          # incremental, right now
make backfill START=2026-01-01       # a window; END defaults to now
make backfill START=2026-01-01 END=2026-04-01
```

Both trigger a DAG. **Do not run `ingest run --destination clickhouse` by hand
while the stack is up.** Airflow serializes ingestion through a pool of one; an
out-of-band run shares the same dlt working directory and incremental cursor and
will interleave with a scheduled run into an inconsistent state.

The one safe direct invocation is the smoke test, which uses a separate pipeline
name and cannot touch production state:

```bash
docker compose --profile cli run --rm airflow-cli \
  ingest run --destination duckdb --sample 3
```

## Which mode does the user actually want

This is the question worth getting right, because the two modes answer different
questions and the wrong one silently returns incomplete data.

**Incremental** (`make ingest`) finds issues *updated* since the last run. This
is the steady state and it is what "get the latest data" means.

**Window** (`make backfill`) finds issues *created* in a date range. The Pylon
API only filters `GET /issues` on `created_at` — that is a hard constraint, not
a design choice. So a backfill of March will **not** pick up a January ticket
that was updated in March. If the user wants "everything that changed in March",
that is not a backfill; the hourly incremental run already covers it.

Use `make backfill` when: data is genuinely missing for a period, or the stack
is new and needs history loaded.

## Watching a run

```bash
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d pylon_ingest_hourly --limit 5
```

Or point the user at http://localhost:8080. A first backfill from 2019 takes
hours — Pylon allows 10 issue-list requests per minute and the pipeline paces
itself to stay under that rather than getting throttled. Say so upfront instead
of letting them watch a seemingly hung task.

## After it runs

Ingestion writes a run summary into `ops.pipeline_runs` and the hourly DAG
follows it with the raw quality checkpoint. To confirm what landed, switch to
the `pipeline-status` skill rather than querying tables ad hoc.

## Failure modes worth recognising

- **The task fails immediately on auth** — `PYLON_API_KEY` is missing or
  revoked. Check it is set (never print it).
- **The run ends after ~25 minutes with messages still pending** — that is the
  designed behaviour, not a failure. The per-issue message fetch carries a
  wall-clock budget so an hourly job ends cleanly; the watermark resumes next
  run. Repeated hourly runs converge.
- **`glitched` in the logs** — Pylon occasionally returns a page claiming there
  is more data while sending none. The paginator retries the same cursor ten
  times before giving up. An occasional one is noise; a run that dies on it is
  worth reporting upstream.
- **A run that loaded zero issues** is not necessarily broken. The cursor now
  advances past an empty scan by design, so a quiet tenant is a no-op.
