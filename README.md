# Data Sovereignty

A self-hosted data stack you drive from Claude Code — ingestion, orchestration,
data quality, warehouse and BI in one repo, on your own hardware. Point it at a
REST API and it lands the data, validates it, and serves it through Metabase.
Nothing leaves your machine except the calls to the APIs you connect and the
Metabase license check.

```
any REST API ──(dlt)──▶ ClickHouse ──(Metabase transforms)──▶ semantic layer
                        ├─ raw_<source>.*  one database per source
                        ├─ analytics.*     base_ → dim_ → fact_ → metrics_
                        └─ ops.*           quality results, run history

Airflow schedules it. Great Expectations verifies it. Metabase models and
serves it. Claude Code operates all of it through .claude/skills.
```

Every component is open source and self-hosted: [dlt](https://dlthub.com),
[Great Expectations](https://greatexpectations.io),
[Airflow](https://airflow.apache.org),
[Metabase](https://github.com/metabase/metabase),
[ClickHouse](https://github.com/ClickHouse/ClickHouse), Docker.

**One caveat about "fully open source."** Metabase is open source, but
*transforms*, the *Library* and *git-sync* are Enterprise features behind a
license token. Everything up to and including the warehouse — ingestion,
scheduling, quality, the `ops` observability schema — needs no token. Without
one, Metabase still boots and charts your data; it just won't build the modeled
layer. `make mb-audit` tells you which side of that line you're on.

## Quick start

```bash
make env      # create .env, generate Airflow secrets
```

Fill in three values in `.env`: `PYLON_API_KEY` (or whichever source you're
connecting), `MB_PREMIUM_EMBEDDING_TOKEN` (your Metabase licence), and
`MB_ADMIN_PASSWORD` (pick one — it becomes the Metabase admin login).

```bash
make build    # build the Airflow image (~5 min the first time)
make up       # start services, bootstrap Metabase, provision an API key
make mb-audit # confirm the licence grants transforms, remote_sync and library
make ingest   # first ingest
```

Then ask Claude: *"what's the state of my pipeline?"*

## Connecting a source

A source is a file. `sources/<name>.yml` declares the API, how it pages, what is
incremental, when it runs, and what "correct" means for its tables — and the
runtime does the rest. Most connectors need no Python.

Ask Claude to *"connect the Zendesk API"* and the `add-source` skill will
research the API's own documentation for endpoints, pagination and rate limits,
ask you the things research can't answer, generate the connector, and prove it
loads. `sources/pylon.yml` is the reference; its comments explain each field.

Three things are worth knowing before you connect one:

- **Each source gets its own `raw_<source>` database, dlt pipeline and Airflow
  pool of one.** The pool stops concurrent runs racing that source's incremental
  cursor; separate databases stop one source's soft-delete pass seeing another's
  tables as deleted.
- **The incremental question decides whether you lose rows.** An API often
  exposes `updated_at` on a record while its list endpoint filters only on
  creation time — syncing incrementally there never returns an old record edited
  today, and nothing errors. The skill asks; answer carefully.
- **Odd APIs get an escape hatch, not a contorted spec.** `extensions:` names a
  Python module for behaviour configuration shouldn't have to describe. Pylon
  uses it for pages that claim more data and deliver none, and for a worklist
  computed from the warehouse rather than a cursor.

## Services

| Service | URL | Notes |
|---|---|---|
| Metabase | http://localhost:3100 | Enterprise v1.63.x |
| Airflow | http://localhost:8080 | simple auth, all users admin |
| Data docs | http://localhost:8081 | Great Expectations validation docs |
| Warehouse | `http://localhost:8124` (HTTP), `9001` (native) | ClickHouse — `make ch` |

Ports avoid the `metabase-demo` stack's defaults so both can run at once. Change
them in `.env`.

## Everyday commands

`make help` lists everything. The ones you'll use:

| Command | What it does |
|---|---|
| `make ingest` | Trigger the ingest DAG now |
| `make backfill START=2026-01-01` | Backfill a date range |
| `make quality` | Run the raw + mart data-quality checkpoints |
| `make status` | Service health and URLs |
| `make ch` | A clickhouse-client shell on the warehouse |
| `make mb-transforms` | Rebuild the transform layer from the manifest |
| `make test` | Offline test suite (mocked APIs, no network) |
| `make nuke` | Destroy everything, including data |

DAG-integrity tests need Airflow, deliberately outside the default environment:
`uv run --group dag-tests pytest airflow/tests`.

## Lifecycle notes

- **`warehouse-data` and `dlt-state` are a matched pair.** The incremental cursor
  lives in `dlt-state`; the data it describes lives in `warehouse-data`. Destroy
  one without the other and the pipeline believes it already loaded rows that are
  gone. `make nuke` removes both.
- **`make up` is staged on purpose.** Metabase must be bootstrapped and an API key
  written into `.env` before Airflow starts, or Airflow inherits an empty
  `MB_API_KEY`. A bare `docker compose up -d` skips that ordering; re-run
  `make up` to heal it.
- **Warehouse init SQL runs once**, on first initialisation of an empty volume.
  Per-source databases are created at ingest time instead, so a source added
  later still gets one.
- **Don't ingest by hand against the production warehouse while the stack is up.**
  Airflow serialises through the pool; an out-of-band run races the cursor. Use
  `make ingest`. `--destination duckdb` is always safe — separate pipeline name.

## Running it on AWS

The stack runs unattended on a single private EC2 instance, and **merging to
`main` deploys it**: CI gates the merge, GitHub Actions asks SSM to run the
deploy, and the result appears as a check on the merge commit. Nothing is exposed
— the security group has no ingress rules and the UIs are reached through an SSM
tunnel (`make tunnels`).

`terraform/` builds the host; [docs/deploy.md](docs/deploy.md) is the runbook.

## Layout

| Path | Contents |
|---|---|
| `sources/` | One YAML per connector — the source contract |
| `pipeline/` | The ingestion runtime and CLI |
| `quality/` | Great Expectations suites and the `dq` CLI |
| `metabase/` | Transform manifest + SQL, and the `mbx` CLI |
| `airflow/dags/` | Ingest, backfill and reconcile DAGs |
| `terraform/` | The AWS host |
| `.claude/skills/` | How Claude operates this stack |

[CLAUDE.md](CLAUDE.md) has the architecture map, the ClickHouse specifics, and
the rules that keep the stack reproducible — read it before changing anything.

## What's built, and what isn't

**Verified end to end for Pylon**: the stack boots, ingestion lands real data,
the quality checkpoint is green against it, and the Metabase bootstrap provisions
its own API key.

**The multi-source path is partly built.** The source contract, the spec-driven
runtime, per-source databases and cursors, and the `add-source` skill are in
place — and the runtime is proven byte-identical to the hand-written Pylon code
it replaces. Still to come: quality suites generated from a spec's `quality`
block, and per-source DAG generation. Until those land, a second source produces
a valid spec whose DAG and checkpoint have to be written by hand.

**Modeling is not this repo's job.** It runs the platform — ingestion,
orchestration, hosting, and the Metabase instance. Authoring transforms, metrics
and dashboards happens in a separate project driven by `mb-cli`, which ships its
own skills for it (`mb skills get data-workflow`). The `manifest.yml` contract
and `mbx transforms` stay here as the orchestration seam — the hourly DAG builds
whatever the manifest declares — but what goes in it is decided elsewhere.
