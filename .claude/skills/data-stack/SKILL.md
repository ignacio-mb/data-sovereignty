---
name: data-stack
description: Router for operating the self-hosted Pylon data stack — ingestion, data quality, modeling, semantic layer, and pipeline health. Triggers — "ingest Pylon data", "is my pipeline healthy?", "verify the state of my success engineering department", "backfill last quarter", "why did the quality check fail?", "add a metric", "bring the stack up", "what changed in the warehouse?"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Operating the data stack

This repo is a self-hosted pipeline: Pylon → Postgres → Metabase. You drive it
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
| Add or change a transform, model new data | `model-data` |
| Add a metric, segment, dashboard, or column metadata | `semantic-layer` |
| Version Metabase content in git | `metabase-sync` |
| Ingest from a source that is not Pylon | `add-source` |

Read the leaf with `Read .claude/skills/<name>/SKILL.md`. If a request spans two
(e.g. "backfill Q1 and check it landed"), load them in sequence, not upfront.

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
raw_pylon.*    six tables, loaded by dlt, merged on id
analytics.*    base_ -> dim_ -> fact_ -> metrics_, built by Metabase transforms
ops.*          gx_results, pipeline_runs, mb_transform_runs
```

- `pylon` ingests. `dq` validates. `mbx` models. `mb` is the raw Metabase CLI.
- Airflow runs all of it hourly; a pool of one serializes ingestion.
- Anything long-running belongs in a DAG, not in your shell.

## When you need Metabase detail

The `mb` CLI ships its own skills, versioned with the binary. Load them at the
point of use rather than guessing at command shapes:

```bash
mb skills list
mb skills get transform --max-bytes 0
```

Every command also self-describes: `mb transform create --help --json` returns
its input and output JSON Schema. Trust that over anything written here.
