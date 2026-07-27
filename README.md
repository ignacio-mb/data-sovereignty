# Data Sovereignty

A self-hosted, end-to-end data stack you drive from Claude Code. Pylon tickets
land in your own Postgres warehouse, get validated, modeled and published as a
Metabase semantic layer — with nothing leaving your machine except the calls to
the Pylon API and the Metabase license check.

```
Pylon API ──(dlt)──▶ Postgres warehouse ──(Metabase transforms)──▶ semantic layer
                     ├─ raw_pylon.*    six flat tables, merged on id
                     ├─ analytics.*    base_ → dim_ → fact_ → metrics_
                     └─ ops.*          quality results, run history

Airflow schedules it. Great Expectations verifies it. Metabase models and
serves it. Claude Code operates all of it through the skills in .claude/skills.
```

## Prerequisites

- Docker Desktop (running) with ~8 GB available to the VM
- [uv](https://docs.astral.sh/uv/) for the Python workspace
- `jq` and `openssl` on PATH
- A Pylon admin API key
- A Metabase Enterprise license token

## Quick start

```bash
make env      # create .env, generate Airflow secrets
# fill in PYLON_API_KEY, MB_PREMIUM_EMBEDDING_TOKEN, MB_ADMIN_PASSWORD
make build    # build the Airflow image (pylon + dq + mbx + mb)
make up       # start services, bootstrap Metabase, provision an API key
make ingest   # trigger the first ingest
```

Then ask Claude: *"what's the state of my pipeline?"*

## Services

| Service | URL | Notes |
|---|---|---|
| Metabase | http://localhost:3100 | Enterprise v1.63.x |
| Airflow | http://localhost:8080 | simple auth, all users admin |
| Data docs | http://localhost:8081 | Great Expectations validation docs |
| Warehouse | `postgres://localhost:5434/warehouse` | `make psql` |

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
| `make test` | Offline test suite (mocked API, no network) |
| `make nuke` | Destroy everything, including data |

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
- **Don't run `pylon ingest --destination postgres` by hand while the stack is
  up.** Airflow serializes ingest through a pool of one; an out-of-band run
  races the cursor. Use `make ingest`. Local `--destination duckdb` smoke runs
  are safe — they use a separate pipeline name and can't touch production state.

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
