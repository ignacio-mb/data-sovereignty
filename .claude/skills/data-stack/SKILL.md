---
name: data-stack
description: Router for operating the self-hosted ingestion stack — connecting REST API sources, orchestration, data quality, and pipeline health. Triggers — "ingest the latest data", "connect a new API", "is my pipeline healthy?", "verify the state of my data stack", "backfill last quarter", "why did the quality check fail?", "bring the stack up", "what changed in the warehouse?"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Operating the data stack

This repo is a self-hosted ingestion pipeline: any REST API → ClickHouse, with
Metabase hosted on top to read the result. You drive it through `make` targets and
two CLIs, never by clicking in the Metabase UI.

**Everything in `sources/` is connected and live**, and Swoogo, Customer.io, Lever
and YouTube all ship that way — a fork that is not ours should delete them. Run
`make sources` before
almost anything else. If it lists nothing, the honest answer to most questions is
"no source is connected yet" and the next step is `add-source`; if it lists a source
whose credential is unset, its hourly DAG is failing and that is usually the real
question. Either way, do not go hunting for data that was never meant to be there.

**Read this file, then load exactly one leaf skill for the task.** Each leaf is
short. Loading all of them wastes context you will want for the actual work.

## Route

| The user wants | Load |
|---|---|
| Start/stop the stack, first-time setup, "is it running?" | `stack-ops` |
| Pull data now, backfill a date range | `ingest` |
| "Is the pipeline healthy?", "verify the whole state" | `pipeline-status` |
| Run or interpret data-quality checks | `data-quality` |
| Connect a new API as a source | `add-source` |
| Work out how a third-party API behaves | `api-research` |

Read the leaf with `Read .claude/skills/<name>/SKILL.md`. If a request spans two
(e.g. "backfill Q1 and check it landed"), load them in sequence, not upfront.

## Out of scope: anything about what the data means

This repo lands raw data, orchestrates the landing, and hosts the instance that
reads it. It does **not** model: no transforms, no marts, no metrics, no segments,
no dashboards, no column metadata, and no method for versioning Metabase content.
Nothing here builds a table downstream of `raw_`, and **scheduling** that work is
out of scope too — owning the schedule for someone else's model is owning the model.

Asked to model data, define a metric, or set up content sync: say plainly that this
repo hosts the warehouse and the instance, and that the work happens against them
from whichever project owns the modeling. Do not re-derive the method here, and do
not add a DAG task for it — `test_no_dag_builds_or_validates_a_model` will fail, by
design.

`mb` is still the interface to Metabase, and it self-describes, so discover its
surface at runtime rather than trusting anything written here:

```bash
mb --help --json | jq -r '.commands[].command'
mb <command> --help --json     # input/output JSON Schema
mb skills list                 # the skills shipped with the binary
```

## Shared contract

**Answer first.** Lead with what the user asked, then the evidence. "Ingestion
is healthy — last run 12 minutes ago, 340 issues, all checks passed" beats a
transcript of five commands.

**Never print secrets.** `.env` holds each connected source's API token, the
Metabase license and the API key. Read it only to check whether a variable is *set*,
and never echo a value or paste one into a command line that gets logged.

**Prefer `make`.** Targets carry the right flags, the right container and the
right ordering. `make help` lists them. Reach past make only when diagnosing.

**Say what you did not check.** "Ingest looks fine, I did not look at whether the
quality checks ran" is useful. Implying full coverage you did not verify is not.

## The five-second orientation

```
raw_<source>.* one database per connected source, loaded by dlt, merged on the
               primary key. A fresh checkout has none.
ops.*          gx_results, pipeline_runs
```

A source is `sources/<name>.yml` — what the API is, how it pages, what is
incremental, when it runs, and what "arrived correctly" means. Everything else is
derived from it: the DAGs, the expectations, the database, the pool.
`.claude/skills/add-source/reference/pylon.yml` is a worked example, kept out of
`sources/` precisely so nothing runs it.

- `ingest` ingests. `dq` validates. `mb` is the Metabase CLI, used by bootstrap
  to provision the instance, and it owns everything about how Metabase is used.
- Airflow runs all of it hourly; each source has a pool of one, so its runs
  cannot race that source's incremental cursor.
- Anything long-running belongs in a DAG, not in your shell.
