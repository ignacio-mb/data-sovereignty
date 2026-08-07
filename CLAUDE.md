# Working in this repo

A self-hosted ingestion stack: any REST API → ClickHouse, orchestrated by Airflow
and validated by Great Expectations, with Metabase hosted on top to read the
result. Designed to be operated by prompt.

**Its scope is landing raw data and orchestrating that well.** It does not model:
no transforms, no marts, no metrics, no semantic layer. What the rows *mean* is
decided in whatever project owns the warehouse's meaning, and keeping the seam
there is deliberate — scheduling someone else's model is owning it.

**Three sources ship connected: Swoogo** (`sources/swoogo.yml`)**, Customer.io**
(`sources/customerio.yml`)**, and Lever** (`sources/lever.yml`), because this
checkout is also where they are operated from. Everything in `sources/` is
live — each spec generates an unpaused ingest DAG on the schedule it declares,
plus backfill and reconcile DAGs, on any stack that comes up, and needs the
variable in its `token_env` set or that DAG fails on every tick. So a fork gets
all three whether it wants them or not: **delete the specs in `sources/` if you
are not us**, and the stack goes back to scheduling nothing.

Lever's `opportunities` — the only resource on any shipped spec whose fetch
depends on a persisted incremental cursor rather than a full re-fetch every
run — has no `soft_delete`. The runtime's tombstone pass only trusts a run it
can prove covered full history, and `--start`/`--end` never reach a
strategy the declarative config can't express (`build_source` takes no
window; a delegated resource bounds itself solely by its own cursor — see
`ingest/runtime.py`), so `full_history` there would be silently skipped by
every reconcile run forever. Lever's own hard-deletes are rare (GDPR-driven);
archived-but-not-deleted candidates stay visible via `archivedAt`, a normal
field on every load.

`airflow/tests/test_dag_integrity.py::TestWhatThisCheckoutShips` pins the list,
so adding or removing a spec fails a test until this paragraph agrees with it.

Pylon is the other shape: a worked reference example for an agent to copy
(`.claude/skills/add-source/reference/pylon.yml`), deliberately NOT in
`sources/` so nothing schedules it.

**Start with `.claude/skills/data-stack/SKILL.md`** — it routes to the right leaf
skill for the task. This file is the map and the rules; the skills are the
procedures.

## Layout

| Path | What lives there |
|---|---|
| `sources/` | One YAML per connector — the source contract. **Empty on a fresh checkout.** |
| `pipeline/` | `ingest` CLI — a source spec → `raw_<source>.*` via dlt |
| `quality/` | `dq` CLI — expectations generated from each spec, results → `ops.*` |
| `airflow/dags/` | `stack_smoke`, plus DAGs generated per spec. None import the packages. |
| `docs/` | The AWS deploy runbook |
| `warehouse/init/` | Runs once, on first init of an empty volume |
| `terraform/` | The AWS host. `docs/deploy.md` is the runbook. |

The two Python packages are uv workspace members sharing one environment, so
`ingest` and `dq` are always installed together. `quality` depends on `pipeline`
for the spec parser and nothing else; the dependency never runs the other way.

## Warehouse

```
raw_<source>.*  dlt owns this, one database per connected source. Merged on the
                spec's primary key, nested JSON stringified. None on a fresh
                checkout.
ops.*           The pipeline's self-knowledge: gx_results, pipeline_runs.
```

**ClickHouse has no schemas — each of those is a DATABASE.** Metabase shows them
where it would show Postgres schemas, and everything addresses tables as
`database.table`.

Only `ops` is created by the init SQL. `raw_<source>` is created by
`ensure_database` before dlt connects, not by the init file: ClickHouse selects
its database as part of connecting, so dlt would fail with `Code: 81. Database
raw_x does not exist` during its pre-run sync, before it could create anything —
and the init file runs once on an empty volume, so it could never cover a source
connected later. Ownership of the *contents* belongs to whatever writes them: dlt
owns the tables in `raw_<source>`, `dq ops-init` owns `ops`. There is no
`analytics` database, because nothing here writes one.

**dlt writes into `raw_<source>` directly, with an EMPTY dataset name.** With a
dataset set, tables arrive as `raw_x.raw_x___things`; empty, dlt falls through to
the bare table name. Do *not* also blank `dataset_table_separator` — it changes
nothing here (there is no prefix left to separate) and it does reach the staging
dataset, turning `_staging___things` into `_stagingthings`.

The database is set per source at build time rather than in compose, because a
single global would put every source in one database sharing one soft-delete
pass. See `build_pipeline` and `ensure_database`.

## Data quality on ClickHouse

**Expectations are generated from the spec, never hand-written per source.**
`sources/<name>.yml` declares both what to fetch and what "arrived correctly"
means for it, and `quality/…/suites/raw.py` turns the second half into GX objects.
Identity checks — primary key not null, primary key unique, at least one row — are
not opt-in: every resource gets them whether the spec author thought about them or
not. Everything else (`quality.required`, `freshness`, `max_deleted_fraction`,
`references`, `not_null`) is declared. Adding a check for one source means editing
that source's spec; adding a *kind* of check means editing `raw.py`.

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
so being ignorable is opt-in. Freshness is advisory by default: it measures when
the *source* last changed a record, not whether ingestion works, so on a quiet
weekend it fails while the pipeline is healthy — and a check that reddens the DAG
for a non-problem teaches you to stop reading red DAGs. Whether a run happened is
a question about runs, and `ops.pipeline_runs` answers it. A source that really
does change hourly can set `severity: error` and gate on it.

## Hard rules

**Never add a spec to `sources/` to demonstrate something.** Anything there is
connected: it schedules an unpaused DAG and demands a credential on every
clone, so a spec added to illustrate a point becomes someone else's failing DAG.
Examples go in a skill's `reference/` directory, where Pylon lives. The bar for
`sources/` is "we actually run this" — today that is Swoogo and Customer.io, and
`TestWhatThisCheckoutShips` fails until the list and the docs agree.

**Nothing here models the data.** No transforms, no marts, no metrics, no
semantic layer, and no DAG task that builds one. If a task would make this
pipeline responsible for the meaning of the rows rather than their arrival, it
belongs in another project. `test_no_dag_builds_or_validates_a_model` enforces it.

**A connector is a spec, not a module.** `sources/<name>.yml` is the whole
contract: endpoints, paging, incremental strategy, schedule, timeouts, pool, and
expectations. Nothing about a particular API may be compiled into `pipeline/` or
`quality/`. The one seam is `extensions:`, for fetch behaviour the declarative
config genuinely cannot express — an explicit, named escape hatch, not a habit.

**DAGs are generated, never hand-written per source.** `airflow/dags/source_dags.py`
builds them from the specs. A hand-written DAG would be a second place to change
a schedule or a timeout, and the two would drift. It reads YAML directly rather
than importing the spec parser, because of the next rule.

**DAGs shell out; they never import the pipeline packages.** dlt, Great
Expectations and Airflow all pin large dependency trees. Keeping them in
separate virtualenvs (`/opt/data-venv` vs Airflow's own) means never having to
reconcile the three.

**Secrets live only in `.env`.** Never echo a value, never paste one into a
command that gets logged, never commit one. To check configuration, test whether
a variable is *set*. A spec names the variable holding its token (`token_env`) and
never the token; the containers read `.env` wholesale, so connecting a credential
is one line there and no compose change.

That rule is an instruction, and an instruction is not an enforcement. The
`permissions.deny` list in `.claude/settings.json` is the enforcement: the harness
refuses the read before the tool runs, so `.env`, `.deploy.env`, `.dlt/secrets.toml`
and the Terraform state and tfvars cannot enter a transcript. `.env.example` is
deliberately *not* denied — it is the file to read and to edit. The `Bash(cat .env*)`
entries are speed bumps, not a boundary: deny rules match the command string, and a
shell has a hundred other ways to print a file. What actually holds on that side is
not granting broad `Bash` allows.

**Ingest through Airflow while the stack is up.** A per-source pool of one
serializes that source's dlt runs; an out-of-band `ingest run --source x` races its
incremental cursor. `--destination duckdb` is always safe — separate pipeline name.
The pools are created by `airflow-init` from the specs, so a source added while the
stack is up needs `docker compose up airflow-init` (or `make up`) before its DAG
can acquire one.

**`localhost` names a port, not an instance.** `make tunnels` forwards the
instance's services to **3200/8180/8181/8224**, deliberately clear of the local
stack's 3100/8080/8081/8124. They used to be the same numbers, and an SSH tunnel
binds `127.0.0.1` while Docker binds `0.0.0.0` — loopback wins, so with both
running `localhost:3100` silently *was* production while every container kept
serving underneath. `make bootstrap` rotated the instance's Metabase API key
that way, believing it was talking to a laptop, and nothing in the output said
otherwise.

Two guards enforce it, because the port separation only holds for tunnels this
repo opened:

- `scripts/assert_local_stack.sh` (via `make up` / `make bootstrap`) refuses
  when *any* non-Docker process shares one of the stack's ports. Every listener
  must be Docker — checking that one of them is passes a live tunnel, which is
  how the first version of the check failed.
- `ingest run` refuses a production-destination run whose warehouse host is
  loopback. Inside the containers compose injects `warehouse-db`, so the
  legitimate paths never see it. `--destination duckdb` is exempt.

Neither is bypassable by accident: `DS_SKIP_LOCAL_CHECK` and
`DS_ALLOW_HOST_INGEST` exist, and both mean "I have checked by hand which
instance this reaches."

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

This repo *hosts* Metabase and nothing more: bring it up, run the setup wizard,
register the warehouse connection, sync its schema. It builds nothing inside it.

`mb` is the interface, and `scripts/bootstrap_metabase.sh` is the one place this
repo talks to it. The CLI already owns credential resolution, retries, redaction,
capability preflight, and a versioned self-describing contract; re-implementing any
of that against the REST API means owning it forever.

Raw REST is a last resort, allowed only where no command exists. Today that is
exactly four things, each marked at the call site: the health poll,
password-based session login, creating a database connection, and creating an
API key. **If a future `mb` release adds a command for one of them, delete the
curl.** Before writing any new REST call, check:

```bash
mb --help --json | jq -r '.commands[].command'   # the whole surface
mb <command> --help --json                       # input/output JSON Schema
```

Prefer those over anything written here — this file goes stale, they do not.

**`--full` when you filter on nested fields.** List projections are compact by
default and drop nested structures: `mb db list` has no `details`, so a filter
on `details.host` matches nothing and reports "not found" rather than failing.
That silently created a duplicate warehouse connection on every bootstrap run
until it was caught. `scripts/bootstrap_metabase.sh` carries the live example, on
the `mb db list --full` that finds the warehouse connection.

## Verification

```bash
make test          # offline: mocked API, duckdb, no network, no secrets
uv run ruff check .
docker compose --profile cli run --rm airflow-cli airflow dags test stack_smoke
```

DAG tests need Airflow, which is deliberately outside the default environment:
`make test-dags` (or `uv sync --group dag-tests && uv run pytest airflow/tests`).
They pin which specs `sources/` ships, generate DAGs from the skill's reference
spec to check the invariants that protect the warehouse, and separately assert
that a directory with no specs schedules only `stack_smoke`.

That last one is a property of the generator, not of this checkout, and used to
be described here as a guarantee about the shipped state. It never was: the
fixture points at an empty `tmp_path`, so it passed identically whatever
`sources/` contained — which is how a connected source and four documents
claiming an empty one coexisted. `TestWhatThisCheckoutShips` is the one that
reads the real directory.

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
