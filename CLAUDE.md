# Working in this repo

A self-hosted data stack: Pylon → ClickHouse → Metabase, orchestrated by Airflow
and validated by Great Expectations. Designed to be operated by prompt.

**Start with `.claude/skills/data-stack/SKILL.md`** — it routes to the right leaf
skill for the task. This file is the map and the rules; the skills are the
procedures.

## Layout

| Path | What lives there |
|---|---|
| `pipeline/` | `pylon` CLI — Pylon API → `raw_pylon.*` via dlt |
| `quality/` | `dq` CLI — Great Expectations suites, results → `ops.*` |
| `metabase/` | `mbx` CLI — transforms, semantic layer, git-sync |
| `metabase/transforms/manifest.yml` | The transform contract. Read by `mbx` **and** by the mart quality suites. |
| `airflow/dags/` | Four DAGs; all shell out, none import the packages |
| `docs/` | Stop-gate deliverables from modeling |
| `warehouse/init/` | Runs once, on first init of an empty volume |
| `terraform/` | The AWS host. `docs/deploy.md` is the runbook. |

The three Python packages are uv workspace members sharing one environment, so
`pylon`, `dq` and `mbx` are always installed together.

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

**dlt writes into `raw_pylon` directly, with an empty dataset name AND an empty
`dataset_table_separator`.** Leave either set and the tables come out as
`raw_pylon.raw_pylon___issues`. See `build_pipeline`.

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
runs; an out-of-band `pylon ingest --destination clickhouse` races the incremental
cursor. `--destination duckdb` is always safe — separate pipeline name.

**`warehouse-data` and `dlt-state` are a matched pair.** The cursor describes
data in the warehouse. Destroy one without the other and the pipeline believes
it already loaded rows that no longer exist.

**The AWS checkout is a deploy artefact, not a workspace.** Merging to `main`
resets `/data/data-sovereignty` to it. Nothing the running stack writes may be a
tracked path — that is why `PYLON_SCHEMA_DIR` points dlt's schema export at the
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
