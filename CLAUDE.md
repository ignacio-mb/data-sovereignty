# Working in this repo

A self-hosted data stack: any REST API → ClickHouse → Metabase, orchestrated by Airflow
and validated by Great Expectations. Designed to be operated by prompt.

**Start with `.claude/skills/data-stack/SKILL.md`** — it routes to the right leaf
skill for the task. This file is the map and the rules; the skills are the
procedures.

## Layout

| Path | What lives there |
|---|---|
| `sources/` | One YAML per connector — the source contract |
| `pipeline/` | `ingest` CLI — a source spec → `raw_<source>.*` via dlt |
| `quality/` | `dq` CLI — Great Expectations suites, results → `ops.*` |
| `metabase/` | `mbx` CLI — transforms, semantic layer, git-sync |
| `metabase/transforms/manifest.yml` | The transform contract. Read by `mbx` **and** by the mart quality suites. |
| `airflow/dags/` | Four DAGs; all shell out, none import the packages |
| `docs/` | Stop-gate deliverables from modeling |
| `warehouse/init/` | Runs once, on first init of an empty volume |
| `terraform/` | The AWS host. `docs/deploy.md` is the runbook. |

The three Python packages are uv workspace members sharing one environment, so
`ingest`, `dq` and `mbx` are always installed together.

## Warehouse

```
raw_pylon.*   dlt owns this. Six tables, merged on id, nested JSON stringified.
analytics.*   Metabase transforms own this. base_ → dim_ → fact_ → metrics_.
ops.*         The pipeline's self-knowledge: gx_results, pipeline_runs,
              mb_transform_runs. Feeds the Pipeline Health dashboard.
```

**ClickHouse has no schemas — each of those is a DATABASE.** Metabase shows them
where it would show Postgres schemas, and everything addresses tables as
`database.table`.

All three are created by the init SQL, which differs from the usual "let the
tool own its own schema" arrangement. ClickHouse forces it: a client selects its
database as part of connecting, so dlt fails with `Code: 81. Database raw_pylon
does not exist` during its pre-run sync, before it can create anything.
Ownership of the *contents* is unchanged — dlt owns the tables in `raw_pylon`,
Metabase transforms own `analytics`, `dq ops-init` owns `ops`.

**dlt writes into `raw_<source>` directly, with an EMPTY dataset name.** With a
dataset set, tables arrive as `raw_pylon.raw_pylon___issues`; empty, dlt falls
through to the bare table name. Do *not* also blank `dataset_table_separator` —
it changes nothing here (there is no prefix left to separate) and it does reach
the staging dataset, turning `_staging___issues` into `_stagingissues`.

The database is set per source at build time rather than in compose, because a
single global would put every source in one database sharing one soft-delete
pass. See `build_pipeline` and `ensure_database`.

## Data quality on ClickHouse

**GX's native column expectations do not compile.** Every `expect_column_*`
renders `CAST(1, 'Decimal(None, None)')`, which ClickHouse rejects with
`Code: 43 ILLEGAL_TYPE_OF_ARGUMENT` — a `clickhouse-sqlalchemy` limitation, not
something we can configure around. Uniqueness and not-null are therefore written
as `UnexpectedRowsExpectation` SQL, which reports a count rather than the
offending values. Custom SQL is otherwise fine: bare `HAVING` and interval
arithmetic both work.

**Referential checks are `LEFT ANTI JOIN`, never correlated `NOT EXISTS`.**
ClickHouse rejects a subquery referencing an outer column (`Code: 1 ... only
supported for constants and CTE`). It does not always reach that path on tiny
inputs, so a fixture can pass while real data fails — which is exactly how this
one was found.

**GX's own `[clickhouse]` extra is unusable** on Python 3.12: it pins
`sqlalchemy<2` while requiring `clickhouse-sqlalchemy>=0.3`, which requires
`sqlalchemy>=2`. The dialect is a direct dependency and the datasource is GX's
generic `add_sql`.

**Advisory vs gating.** An expectation carrying `meta={"severity": "warn"}` is
recorded and reported but does not fail the checkpoint. Anything unmarked gates,
so being ignorable is opt-in. Freshness is the only advisory check: it measures
when the *tenant* last touched a record, not whether ingestion works, so on a
quiet weekend it fails while the pipeline is healthy — and a check that reddens
the DAG for a non-problem teaches you to stop reading red DAGs. Whether a run
happened is a question about runs, and `ops.pipeline_runs` answers it.

## Hard rules

**Transforms are authored in this repo, never in the Metabase UI.** The manifest
and its SQL files are the source of truth; `mbx transforms` overwrites whatever
is in the instance. A UI edit is lost work.

**Plain table transforms only — never `table-incremental`.** Metabase v63's
git-sync serializer drops template tags from incremental transforms on import
and reports success anyway, silently breaking them. Full refresh is adequate at
these volumes. This is a correctness rule, not a preference.

**DAGs shell out; they never import the pipeline packages.** dlt, Great
Expectations and Airflow all pin large dependency trees. Keeping them in
separate virtualenvs (`/opt/data-venv` vs Airflow's own) means never having to
reconcile the three.

**Declare grain in the manifest.** It becomes both the post-build assertion and
the mart quality check. A transform without a grain is untested.

**One definition per metric.** Variants are that metric plus a segment plus a
breakout. Two SQL expressions for the same idea will drift.

**Secrets live only in `.env`.** Never echo a value, never paste one into a
command that gets logged, never commit one. To check configuration, test whether
a variable is *set*.

**Ingest through Airflow while the stack is up.** A pool of one serializes dlt
runs; an out-of-band `ingest run --destination clickhouse` races the incremental
cursor. `--destination duckdb` is always safe — separate pipeline name.

**`warehouse-data` and `dlt-state` are a matched pair.** The cursor describes
data in the warehouse. Destroy one without the other and the pipeline believes
it already loaded rows that no longer exist.

**The AWS checkout is a deploy artefact, not a workspace.** Merging to `main`
resets `/data/data-sovereignty` to it. Nothing the running stack writes may be a
tracked path — that is why `DS_SCHEMA_DIR` points dlt's schema export at the
`dlt-state` volume. Work on a laptop and merge; `make hold` first if you must
work on the box. `docs/deploy.md`.

**Non-secret tunables belong in `docker-compose.yml`, not `.env.example`.**
`.env` is created once and never re-synced, so a value there shadows the compose
default forever — pinning an image tag in `.env` turns a later bump in git into
a silent no-op on a long-lived host.

## Metabase goes through mb-cli

`mb` is the interface to Metabase. Everything in `metabase/src/mb_tools/` shells
out to it through `mb.py`, and `scripts/bootstrap_metabase.sh` uses it too. The
CLI already owns credential resolution, retries, redaction, capability
preflight, and a versioned self-describing contract; re-implementing any of that
against the REST API means owning it forever.

Raw REST is a last resort, allowed only where no command exists. Today that is
exactly four things, each marked at the call site: the health poll,
password-based session login, creating a database connection, and creating an
API key. **If a future `mb` release adds a command for one of them, delete the
curl.** Before writing any new REST call, check:

```bash
mb --help --json | jq -r '.commands[].command'   # the whole surface
mb <command> --help --json                       # input/output JSON Schema
mb skills get transform --max-bytes 0            # skills shipped with the binary
```

Prefer those over anything written here — this file goes stale, they do not.

**`--full` when you filter on nested fields.** List projections are compact by
default and drop nested structures: `mb db list` has no `details`, so a filter
on `details.host` matches nothing and reports "not found" rather than failing.
That silently created a duplicate warehouse connection on every bootstrap run
until it was caught. `mb.run(..., full=True)` sets the flag.

### Building against a local mb-cli

The image installs the pinned published `@metabase/cli` by default. To run
against your own checkout — the usual case when changing mb-cli itself:

```bash
make mb-cli-local                       # defaults to ~/dev/mb-cli/mb-cli
make mb-cli-local MB_CLI_SRC=/path/to/mb-cli
make mb-cli-published                   # back to the pinned release
```

It packs the working copy into `docker/airflow/vendor/`, which the Dockerfile
prefers over the registry. The tarball is git-ignored, so a clean checkout still
builds reproducibly.

## Verification

```bash
make test          # offline: mocked API, duckdb, no network, no secrets
uv run ruff check .
docker compose --profile cli run --rm airflow-cli airflow dags test stack_smoke
```

DAG tests need Airflow, which is deliberately outside the default environment:
`uv sync --group dag-tests && uv run pytest airflow/tests`.

The deploy path cannot be exercised on a laptop. What can:

```bash
terraform -chdir=terraform validate
docker run --rm -v "$PWD:/mnt" -w /mnt koalaman/shellcheck:stable -S warning scripts/*.sh
```

## Conventions

Python is plain and typed by convention rather than annotation, matching the
inherited pipeline code. Comments explain constraints the code cannot show — an
API's 30-day window cap, why a LEFT JOIN is avoided — not what the next line
does. Line length 110, `ruff` enforces the rest.
