# Working in this repo

A self-hosted data stack: Pylon → Postgres → Metabase, orchestrated by Airflow
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

The three Python packages are uv workspace members sharing one environment, so
`pylon`, `dq` and `mbx` are always installed together.

## Warehouse

```
raw_pylon.*   dlt owns this. Six tables, merged on id, nested JSON stringified.
analytics.*   Metabase transforms own this. base_ → dim_ → fact_ → metrics_.
ops.*         The pipeline's self-knowledge: gx_results, pipeline_runs,
              mb_transform_runs. Feeds the Pipeline Health dashboard.
```

Only `ops` is created by the init SQL. `raw_pylon` is created by dlt on first
ingest and `analytics` by the first transform run — pre-creating them takes
ownership away from the tool that manages them.

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
runs; an out-of-band `pylon ingest --destination postgres` races the incremental
cursor. `--destination duckdb` is always safe — separate pipeline name.

**`warehouse-data` and `dlt-state` are a matched pair.** The cursor describes
data in the warehouse. Destroy one without the other and the pipeline believes
it already loaded rows that no longer exist.

## Discovering Metabase behaviour

Do not guess at `mb` command shapes. The CLI self-describes, and it ships skills
versioned with the binary:

```bash
mb <command> --help --json     # input and output JSON Schema
mb skills list
mb skills get transform --max-bytes 0
```

Prefer that over anything written here — this file goes stale, those do not.

## Verification

```bash
make test          # offline: mocked API, duckdb, no network, no secrets
uv run ruff check .
docker compose --profile cli run --rm airflow-cli airflow dags test stack_smoke
```

DAG tests need Airflow, which is deliberately outside the default environment:
`uv sync --group dag-tests && uv run pytest airflow/tests`.

## Conventions

Python is plain and typed by convention rather than annotation, matching the
inherited pipeline code. Comments explain constraints the code cannot show — an
API's 30-day window cap, why a LEFT JOIN is avoided — not what the next line
does. Line length 110, `ruff` enforces the rest.
