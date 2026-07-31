# Data Sovereignty

A self-hosted ingestion stack you drive from Claude Code — ingestion,
orchestration, data quality, warehouse and BI in one repo, on your own hardware.
Point it at a REST API and it lands the data, validates it, and serves it through
Metabase. Nothing leaves your machine except the calls to the APIs you connect and
the Metabase license check.

**One source ships connected: Swoogo**, because this repo is also where we run
it. Everything in `sources/` is live — each spec generates an unpaused hourly
ingest DAG the moment the stack comes up, and fails every hour without its
credential. **If you are not us, delete `sources/swoogo.yml`** and the stack goes
back to scheduling nothing but the smoke DAG, which exists to prove the plumbing.

Then ask Claude to connect a source you actually have.

```
any REST API ──(dlt)──▶ ClickHouse ──▶ Metabase
                        ├─ raw_<source>.*  one database per source
                        └─ ops.*           quality results, run history

Airflow schedules it. Great Expectations verifies it. Metabase serves it.
Claude Code operates all of it through .claude/skills.
```

Every component is open source and self-hosted: [dlt](https://dlthub.com),
[Great Expectations](https://greatexpectations.io),
[Airflow](https://airflow.apache.org),
[Metabase](https://github.com/metabase/metabase),
[ClickHouse](https://github.com/ClickHouse/ClickHouse), Docker.

**One caveat about "fully open source."** Everything this repo does — ingestion,
scheduling, quality, the `ops` observability schema, hosting the instance — needs
no license token, and Metabase boots and charts your data without one. A token
only unlocks Enterprise features, none of which this stack uses. `make bootstrap`
reports which side of that line the instance is on.

## Prerequisites

On the machine you drive this from:

| Tool | Why | Check |
|---|---|---|
| Docker (with compose) | every service runs in it | `docker compose version` |
| [uv](https://docs.astral.sh/uv/) | runs the tests and the CLIs | `uv --version` |
| `jq` | the Metabase bootstrap parses JSON with it | `jq --version` |
| `openssl` | generates the Airflow secrets in `make env` | `openssl version` |
| [`mb`](https://www.npmjs.com/package/@metabase/cli) (needs Node) | the only supported way this repo talks to Metabase | `mb --version` |

`make up` checks for these before it starts anything. It used to boot four
containers first and then abort on a missing `mb`, leaving a half-configured
stack with no admin account.

Docker needs roughly **6 GB** of memory available: ClickHouse, Metabase, two
Postgres instances and four Airflow services. Below that, Metabase and the
Airflow triggerer are the first to be killed.

Deploying to AWS needs more (`aws` CLI, the Session Manager plugin, Terraform) —
see [docs/deploy.md](docs/deploy.md).

## Quick start

```bash
make env      # create .env, generate Airflow secrets
```

Fill in two values in `.env`: `MB_PREMIUM_EMBEDDING_TOKEN` (your Metabase licence)
and `MB_ADMIN_PASSWORD` (pick one — it becomes the Metabase admin login).

```bash
make build    # build the Airflow image (~5 min the first time)
make up       # start services, bootstrap Metabase, provision an API key
make smoke    # prove the plumbing: CLIs, warehouse, Metabase, dlt state
```

There is nothing to ingest yet. Ask Claude to *"connect the Zendesk API"* — or
whichever source you want — and then *"what's the state of my pipeline?"*

## Connecting a source

A source is a file. `sources/<name>.yml` declares the API, how it pages, what is
incremental, when it runs, and what "correct" means for its tables. Everything
else follows from it: the DAGs are generated per spec, the expectations are
generated per spec, and the database, dlt pipeline and Airflow pool are named
after it. Most connectors need no Python at all.

Ask Claude to *"connect the Zendesk API"* and the `add-source` skill will research
the API's own documentation for endpoints, pagination and rate limits, ask you the
things research can't answer, generate the connector, and prove it loads. Then add
the token to `.env` under the name the spec's `token_env` gives, and re-run
`make up` so the source's Airflow pool is created.

`.claude/skills/add-source/reference/pylon.yml` is a complete worked example, and
its comments explain every field. It lives in the skill rather than in `sources/`
on purpose: a reference an agent reads, not a connector the stack runs.

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
| `make sources` | List the connected sources |
| `make ingest SOURCE=x` | Trigger a source's ingest DAG now |
| `make backfill SOURCE=x START=2026-01-01` | Backfill a date range |
| `make quality SOURCE=x` | Validate a source's raw contract |
| `make smoke` | Prove the stack's plumbing, touching no data |
| `make status` | Service health and URLs |
| `make ch` | A clickhouse-client shell on the warehouse |
| `make test` | Offline test suite (mocked APIs, no network) |
| `make nuke` | Destroy everything, including data |

Every pipeline command takes `SOURCE=`, because there is no default source to
mean — the repo ships with none.

DAG-integrity tests need Airflow, deliberately outside the default environment:
`make test-dags`.

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
  `make ingest SOURCE=x`. `--destination duckdb` is always safe — separate pipeline
  name.
- **Airflow pools are created by `airflow-init` from the specs.** Connect a source
  while the stack is running and re-run `make up`, or its ingest task has no pool
  to acquire and simply queues.

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
| `sources/` | One YAML per connector — the source contract. Empty until you connect one. |
| `pipeline/` | The ingestion runtime and `ingest` CLI |
| `quality/` | The `dq` CLI and the suite builder that reads each spec |
| `airflow/dags/` | `stack_smoke`, and the generator that turns specs into DAGs |
| `warehouse/`, `docker/` | The ClickHouse and Airflow images |
| `terraform/`, `scripts/` | The AWS host and its deploy machinery |
| `.claude/skills/` | How Claude operates this stack |

[CLAUDE.md](CLAUDE.md) has the architecture map, the ClickHouse specifics, and
the rules that keep the stack reproducible — read it before changing anything.

## What's built, and what isn't

**The spec-driven path is complete.** One `sources/<name>.yml` produces the fetch,
the DAGs, the expectations, the warehouse database and the cursor. A source using
only paged full-refresh endpoints needs no Python; the test suite drives such a
spec end to end into duckdb, so "no Python" is verified rather than claimed.

**Verified end to end against a real API**: the stack boots, ingestion lands real
data, the quality checkpoint is green against it, and the Metabase bootstrap
provisions its own API key.

**Incremental strategies beyond full refresh need an extension module.**
`search_window` and `parent_watermark` are algorithms, not settings, so a spec
declaring one also declares `extensions:` and supplies a Python module for it. The
reference example uses both — it documents the shape of a hard connector, and
copying it means writing that module too.

**Modeling is not this repo's job, and neither is scheduling it.** What lives here
is ingestion, the orchestration of ingestion, quality on the raw layer, the
warehouse, and the Metabase instance itself. Transforms, metrics, dashboards and
the runs that build them belong to a separate project. The seam between the two is
the warehouse: this repo lands trustworthy raw tables and hands over a provisioned
instance pointed at them.
