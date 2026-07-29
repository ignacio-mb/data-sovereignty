# Data Sovereignty

A self-hosted, end-to-end data stack you drive from Claude Code — ingestion,
orchestration, data quality, warehouse and BI in one repo, on your own hardware.
Pylon tickets land in your own warehouse, get validated, modeled and published as
a Metabase semantic layer, with nothing leaving your machine except the calls to
the Pylon API and the Metabase license check.

```
Pylon API ──(dlt)──▶ ClickHouse ──(Metabase transforms)──▶ semantic layer
                     ├─ raw_pylon.*    six flat tables, merged on id
                     ├─ analytics.*    base_ → dim_ → fact_ → metrics_
                     └─ ops.*          quality results, run history

Airflow schedules it. Great Expectations verifies it. Metabase models and
serves it. Claude Code operates all of it through the skills in .claude/skills.
```

Every component is open source and self-hosted: [dlt](https://dlthub.com),
[Great Expectations](https://greatexpectations.io),
[Apache Airflow](https://airflow.apache.org),
[Metabase](https://github.com/metabase/metabase),
[ClickHouse](https://github.com/ClickHouse/ClickHouse), Docker.

**One honest caveat about "fully open source."** Metabase itself is open source,
but *transforms*, the *Library* and *git-sync* are Enterprise features behind a
license token. Everything up to and including the warehouse — ingestion,
scheduling, quality, the `ops` observability schema — runs on OSS alone and needs
no token. Without one, Metabase still boots and still charts your data; it just
won't build the modeled layer. `make mb-audit` tells you which side of that line
you are on instead of letting you discover it later.

## Prerequisites

- Docker Desktop (running) with ~8 GB available to the VM
- [uv](https://docs.astral.sh/uv/) for the Python workspace
- `jq` and `openssl` on PATH
- A Pylon admin API key
- A Metabase Enterprise license token

## Quick start

```bash
make env      # create .env, generate Airflow secrets
```

Fill in three values in `.env`:

| Variable | Where it comes from |
|---|---|
| `PYLON_API_KEY` | Pylon → Settings → API |
| `MB_PREMIUM_EMBEDDING_TOKEN` | Your Metabase Enterprise license |
| `MB_ADMIN_PASSWORD` | Pick one; it becomes the Metabase admin login |

```bash
make build    # build the Airflow image (~5 min the first time)
make up       # start services, bootstrap Metabase, provision an API key
make mb-audit # confirm the license grants transforms, remote_sync and library
make ingest   # first ingest
```

Then ask Claude: *"what's the state of my pipeline?"*

Without the license token the stack still comes up and ingestion works, but
Metabase behaves like OSS: transforms, the Library and git-sync stay locked, so
the modeling and semantic layers cannot be built. `make mb-audit` says so
explicitly rather than letting you find out later.

## Services

| Service | URL | Notes |
|---|---|---|
| Metabase | http://localhost:3100 | Enterprise v1.63.x |
| Airflow | http://localhost:8080 | simple auth, all users admin |
| Data docs | http://localhost:8081 | Great Expectations validation docs |
| Warehouse | `http://localhost:8124` (HTTP), `9001` (native) | ClickHouse — `make ch` |

Ports avoid the `metabase-demo` stack's defaults (3000/5433) so both can run at
once. Change them in `.env`.

## Everyday commands

`make help` lists everything. The ones you'll use:

| Command | What it does |
|---|---|
| `make ingest` | Trigger the hourly ingest DAG now |
| `make backfill START=2026-01-01` | Backfill a date range |
| `make quality` | Run raw + mart data-quality checkpoints |
| `make status` | Service health and URLs |
| `make mb-transforms` | Rebuild the transform layer from the manifest |
| `make ch` | Open a clickhouse-client shell on the warehouse |
| `make test` | Offline test suite (mocked API, no network) |
| `make mb-cli-local` | Rebuild the image against a local mb-cli checkout |
| `make nuke` | Destroy everything, including data |

DAG-integrity tests need Airflow, which is deliberately outside the default
environment: `uv run --group dag-tests pytest airflow/tests`.

## Lifecycle notes

- **`warehouse-data` and `dlt-state` are a matched pair.** The dlt incremental
  cursor lives in the `dlt-state` volume; the data it describes lives in
  `warehouse-data`. Deleting one without the other leaves the pipeline
  convinced it has already loaded rows that are gone. `make nuke` removes both.
- **`make up` is staged on purpose.** Metabase must be bootstrapped and an API
  key written into `.env` before the Airflow containers start, or they inherit
  an empty `MB_API_KEY`. A bare `docker compose up -d` skips that ordering;
  re-run `make up` afterwards to heal it.
- **Warehouse init SQL runs once**, on first initialization of an empty volume.
  Editing `warehouse/init/*.sql` does nothing until the volume is recreated.
- **Don't run `pylon ingest --destination clickhouse` by hand while the stack is
  up.** Airflow serializes ingest through a pool of one; an out-of-band run
  races the cursor. Use `make ingest`. Local `--destination duckdb` smoke runs
  are safe — they use a separate pipeline name and can't touch production state.

## Running it on AWS

The stack also runs unattended on a single private EC2 instance, and **merging
to `main` deploys it**: CI gates the merge, GitHub Actions asks SSM to run the
deploy on the instance, and the result appears as a check on the merge commit.
Nothing is exposed to the internet — the security group has no ingress rules,
and the UIs are reached through an SSM tunnel (`make tunnels`).

`terraform/` builds the host; [docs/deploy.md](docs/deploy.md) is the runbook —
first deploy, day-to-day, rollback, and what to do when the instance is lost.

## Data quality: gating checks and advisory ones

`dq run` records one row per expectation in `ops.gx_results` and renders data
docs to <http://localhost:8081>.

An expectation marked `meta={"severity": "warn"}` is **advisory**: recorded and
reported, but it does not fail the checkpoint or redden the DAG. Anything not
marked is gating, so being ignorable is opt-in.

Freshness is the motivating case, and currently the only advisory check. It
asserts that `issues.updated_at` is recent — which measures when your *tenant*
last touched a ticket, not whether ingestion works. On a quiet weekend it fails
while every part of the pipeline is healthy, and a check that reddens the DAG for
a non-problem teaches you to stop reading red DAGs. Whether ingestion actually
ran is a question about runs rather than rows, and `ops.pipeline_runs` answers
it.

Failures lead with the sentence the suite author wrote, not the machinery:

```
warnings (not fatal):
  raw_pylon.issues: issues.updated_at is within the last 24h (advisory: ...) observed=1
```

## The warehouse is ClickHouse

Self-hosted `clickhouse/clickhouse-server`, replacing the Postgres warehouse.
Metabase and Airflow keep their own Postgres application databases — ClickHouse
cannot serve either.

**ClickHouse has no schemas.** Each of `raw_pylon`, `analytics` and `ops` is a
*database*, and Metabase shows them where it would show Postgres schemas. Unlike
the Postgres arrangement, all three are created by `warehouse/init/`: a
ClickHouse client selects its database while connecting, so dlt fails with
`Code: 81. Database raw_pylon does not exist` before it can create anything.

Four things were worth the trouble to get right:

- **Metabase Enterprise v1.63 bundles the ClickHouse driver.** No plugin jar, no
  `/plugins` mount. The connection sets `scan-all-databases` so one connection
  sees all three databases.
- **dlt needs an empty dataset name *and* an empty `dataset_table_separator`.**
  Leave either set and tables arrive as `raw_pylon.raw_pylon___issues` instead of
  `raw_pylon.issues`.
- **Great Expectations works, but not through its own `[clickhouse]` extra**,
  which is unsatisfiable on Python 3.12: it pins `sqlalchemy<2` while requiring
  `clickhouse-sqlalchemy>=0.3`, which requires `sqlalchemy>=2`. The dialect is a
  direct dependency instead, and the datasource is GX's generic `add_sql`.
- **GX's native column expectations do not compile on ClickHouse.** They render
  `CAST(1, 'Decimal(None, None)')`, which ClickHouse rejects with `Code: 43
  ILLEGAL_TYPE_OF_ARGUMENT` — a `clickhouse-sqlalchemy` limitation affecting
  every `expect_column_*`. Uniqueness and not-null are therefore written as SQL
  (`UnexpectedRowsExpectation`), which reports a count rather than the offending
  values. Custom SQL is otherwise unaffected: `NOT EXISTS`, bare `HAVING` and
  interval arithmetic all work.

The `ops` tables are MergeTree, with `mb_transform_runs` a ReplacingMergeTree
keyed on `run_id` — ClickHouse has no `ON CONFLICT`, so re-inserting a run is
the upsert.

## Repository layout

| Path | Contents |
|---|---|
| `pipeline/` | The Pylon dlt pipeline (`pylon` CLI) |
| `quality/` | Great Expectations suites and the `dq` CLI |
| `metabase/` | Transform manifest + SQL, and the `mbx` CLI |
| `airflow/dags/` | Ingest, backfill and reconcile DAGs |
| `docs/` | Stop-gate deliverables from the modeling process |
| `.claude/skills/` | How Claude operates this stack |

See [CLAUDE.md](CLAUDE.md) for the architecture map and the rules that keep the
stack reproducible.

## What is built, and what waits on data

The platform is complete and verified end to end: the stack boots, the smoke DAG
passes, ingestion is wired, quality suites run, and the Metabase bootstrap
provisions its own API key.

**The transform manifest ships empty on purpose.** Modeling is stop-gated on
real data — which Pylon custom fields your tenant populates, whether
`csat_responses` has anything in it, whether `resolution_time` is ever set. None
of that is knowable from the API documentation, and guessing it into SQL
produces marts full of zeros that look healthy.

**Modeling is not this repo's job.** This repo runs the platform: ingestion,
orchestration, hosting, and the Metabase instance. Authoring transforms, metrics
and dashboards happens in a separate project driven by `mb-cli`, which ships its
own skills for exactly that (`mb skills get data-workflow`, `mb skills get
transform`). The `manifest.yml` contract and `mbx transforms` remain here as the
orchestration seam — the hourly DAG builds whatever the manifest declares — but
what goes in it is decided and written elsewhere.
