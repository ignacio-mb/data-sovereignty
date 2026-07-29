---
name: ingest
description: Pull a connected source's data into the warehouse now, or backfill a historical date range. Triggers — "ingest the latest data", "pull the latest tickets", "backfill January", "load last quarter", "we're missing data from March", "re-fetch everything".
allowed-tools: Bash, Read, AskUserQuestion
---

# Ingesting a source

## First, which source

The repo ships with none, and every command takes one:

```bash
make sources          # what is connected, and on what schedule
```

Nothing listed means nothing is connected — route to `add-source` rather than
guessing a name. Exactly one listed is still worth naming in the command, so the
output says what it acted on.

## Run it through Airflow, not directly

```bash
make ingest SOURCE=<name>                          # incremental, right now
make backfill SOURCE=<name> START=2026-01-01       # a window; END defaults to now
make backfill SOURCE=<name> START=2026-01-01 END=2026-04-01
```

Both trigger a generated DAG. **Do not run `ingest run --source <name>` by hand
while the stack is up.** Airflow serializes each source through a pool of one; an
out-of-band run shares that source's dlt working directory and incremental cursor
and will interleave with a scheduled run into an inconsistent state.

The one safe direct invocation is the smoke test, which uses a separate pipeline
name and cannot touch production state:

```bash
docker compose --profile cli run --rm airflow-cli \
  ingest run --source <name> --destination duckdb --sample 3
```

## Which mode does the user actually want

This is the question worth getting right, because the two modes can answer
different questions, and the wrong one silently returns incomplete data.

**Incremental** (`make ingest`) advances each resource's cursor — for most specs,
records *updated* since the last run. This is the steady state and it is what "get
the latest data" means.

**Window** (`make backfill`) fetches a date range. What that range filters on is a
property of the API, and it is written in the spec: a resource with
`incremental.strategy: search_window` has a windowed endpoint filtering on the
update field, while a plainer one may only filter on creation time. **Read the
source's spec before promising what a backfill will cover.** Where the API filters
only on creation, a backfill of March will *not* pick up a January record edited in
March — and if that is what the user wants, the incremental run already covers it.

Use `make backfill` when data is genuinely missing for a period, or the stack is new
and needs history loaded.

## Watching a run

```bash
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d <name>_ingest --limit 5
```

Or point the user at http://localhost:8080. A first full backfill can take hours:
the runtime paces itself against the rate limits declared in the spec rather than
getting throttled. Read `rate_limits` and say roughly how long upfront, instead of
letting them watch a seemingly hung task.

## After it runs

Ingestion writes a run summary into `ops.pipeline_runs`, and the ingest DAG follows
it with that source's raw quality check. To confirm what landed, switch to the
`pipeline-status` skill rather than querying tables ad hoc.

## Failure modes worth recognising

- **The task fails immediately on auth** — the variable named by the spec's
  `api.auth.token_env` is unset, or the token is revoked. Check it is *set*, never
  print it. A source connected while the stack was already up may simply never have
  had its token added to `.env`.
- **The task queues forever without starting** — its Airflow pool does not exist.
  Pools are created by `airflow-init` from the specs, so a source added while the
  stack was running needs `make up` before its DAG can acquire one.
- **A run that loaded zero rows is not necessarily broken.** The cursor advances
  past an empty scan by design, so a quiet source is a no-op.
- **A run ends on a wall-clock budget with work still pending** — designed behaviour
  where a spec sets one, not a failure. The watermark resumes next run, and repeated
  runs converge.
- **A spec declaring `extensions:` fails with "module does not exist"** — it claims
  a fetch strategy the declarative config cannot express, and the Python module
  implementing it was never written. Either write it, or reduce the spec to
  declarative strategies.
