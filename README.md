# Data Sovereignty

A self-hosted ingestion stack you drive from Claude Code — ingestion,
orchestration, data quality, warehouse and BI in one repo, on your own hardware.
Point it at a REST API and it lands the data, validates it, and serves it through
Metabase. Nothing leaves your machine except the calls to the APIs you connect and
the Metabase license check.

**It arrives empty.** No source is connected, so nothing is scheduled and nothing
is fetched until you connect one — a fresh checkout has exactly one DAG, and its
job is to prove the plumbing works.

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

### Worked example: Linear, from a prompt

**1. Ask for it, on your laptop.**

```bash
claude
```

> connect the Linear API — I want issues, projects and teams in the warehouse

The skill reads Linear's own documentation for endpoints, pagination and rate
limits, asks you the few things research cannot settle, writes
`sources/linear.yml`, and proves it loads against duckdb. Nothing touches the
warehouse yet.

Do this on a laptop rather than on the instance. The instance's checkout is a
deploy artefact and a deploy resets it, so a spec written there is lost work.

**2. Merge it.** Open a PR and merge; the deploy lands the spec on the instance
and `raw_linear`, the DAGs and the pool follow from it.

**3. Give it its token.** The spec names the variable it wants — `token_env:
LINEAR_API_KEY`. Put the value in your local `.env`, then:

```bash
make secrets-push
```

You do not have to tell that script about Linear. It reads every `token_env:`
out of `sources/*.yml`, so a source that declares its variable is a source whose
secret gets carried.

**4. Create its pool**, because pools are built from the specs at init and this
one did not exist when the stack came up:

```bash
make remote CMD="make secrets-pull && make up"
```

**5. Run it.**

```bash
make remote CMD="make ingest SOURCE=linear"
```

Then `make remote CMD="make quality SOURCE=linear"`, and the rows are in
`raw_linear.*` in ClickHouse, visible in Metabase through `make tunnels`.

What that gave you, none of it written by hand: a `raw_linear` database, three
DAGs (`linear_ingest`, `linear_backfill`, `linear_reconcile`), an Airflow pool of
one that stops concurrent runs racing the cursor, and a generated expectation
suite that fails the run if the primary key is null, duplicated, or the table
arrives empty.

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
