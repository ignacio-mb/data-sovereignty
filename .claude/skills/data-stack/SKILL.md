---
name: data-stack
description: Router for operating the self-hosted data stack — ingestion from any REST API, orchestration, data quality, and pipeline health. Triggers — "ingest Pylon data", "connect a new API", "is my pipeline healthy?", "verify the state of my success engineering department", "backfill last quarter", "why did the quality check fail?", "bring the stack up", "what changed in the warehouse?", "add a metric", "version Metabase content in git", "sync my dashboards"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Operating the data stack

This repo is a self-hosted pipeline: any REST API → ClickHouse → Metabase.
Pylon is the first source, not the only shape it fits. You drive it
through `make` targets and three CLIs, never by clicking in the Metabase UI.

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

## Out of scope: everything about using Metabase

This repo runs the platform — ingestion, orchestration, hosting, and the
Metabase instance itself. It does **not** author transforms, metrics, segments,
dashboards or column metadata, and it does not carry a method for versioning
Metabase content.

All of that belongs to `mb-cli`, whose skills are written and maintained by the
Metabase engineers who build the product, versioned with the binary, and
therefore never stale the way a copy here would be:

```bash
mb skills list
mb skills get data-workflow --max-bytes 0    # end-to-end data work
mb skills get transform --max-bytes 0        # authoring transforms
mb skills get git-sync --max-bytes 0         # versioning content in git
```

Every command also self-describes — `mb transform create --help --json` returns
its input and output JSON Schema. Trust that over anything written here.

Asked to model data, define a metric, or set up content sync, say plainly that
this repo hosts the warehouse and the instance but that work happens against
them from the mb-cli project, and point at those skills. Do not re-derive the
method here.

What *does* stay here is the orchestration around it: `make mb-transforms`
builds whatever `manifest.yml` declares, and `make mb-sync` runs an export. Both
are a clean no-op when there is nothing configured. Running them is this repo's
job; deciding what they should contain is not.

## Shared contract

**Answer first.** Lead with what the user asked, then the evidence. "Ingestion
is healthy — last run 12 minutes ago, 340 issues, all checks passed" beats a
transcript of five commands.

**The repo is the source of truth.** Transforms come from
`metabase/transforms/manifest.yml` and its SQL files. A transform edited in the
Metabase UI is overwritten by the next `mbx transforms` run — that is intended.
If a user has UI changes worth keeping, port them into the repo first.

**Never print secrets.** `.env` holds the Pylon key, the Metabase license and
the API key. Read it only to check whether a variable is *set*, and never echo a
value or paste one into a command line that gets logged.

**Prefer `make`.** Targets carry the right flags, the right container and the
right ordering. `make help` lists them. Reach past make only when diagnosing.

**Say what you did not check.** "Ingest looks fine, I did not look at the
transform layer" is useful. Implying full coverage you did not verify is not.

## The five-second orientation

```
raw_<source>.* one database per source, loaded by dlt, merged on the primary key
analytics.*    base_ -> dim_ -> fact_ -> metrics_, built by Metabase transforms
ops.*          gx_results, pipeline_runs, mb_transform_runs
```

A source is `sources/<name>.yml` — what the API is, how it pages, what is
incremental, when it runs. `raw_pylon` is simply the first one.

- `pylon` ingests. `dq` validates. `mbx` builds whatever the manifest declares.
  `mb` is the Metabase CLI, and owns everything about how Metabase is used.
- Airflow runs all of it hourly; each source has a pool of one, so its runs
  cannot race that source's incremental cursor.
- Anything long-running belongs in a DAG, not in your shell.
