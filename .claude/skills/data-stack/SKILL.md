---
name: data-stack
description: Router for operating the self-hosted ingestion stack — connecting APIs as sources, loading and backfilling them, data quality, and pipeline health. Triggers — "connect a new API", "ingest the latest data", "backfill last quarter", "is my pipeline healthy?", "why did the quality check fail?", "bring the stack up", "what landed in the warehouse?", "set up a new sync".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Operating the ingestion stack

This repo gets data **in**: any REST API → ClickHouse, scheduled by Airflow and
validated by Great Expectations. You drive it through `make` targets and three
CLIs. A source is a file — `sources/<name>.yml` — and Pylon is the first one, not
the shape everything is built around.

**Read this file, then load exactly one leaf skill.** Each leaf is short. Loading
all of them wastes context you will want for the actual work.

## Route

| The user wants | Load |
|---|---|
| Start/stop the stack, first-time setup, "is it running?" | `stack-ops` |
| Connect a new API as a source | `add-source` |
| Work out how a third-party API behaves | `api-research` |
| Load or backfill a source that already exists | `ingest` |
| "Is it healthy?", "did the sync work?" | `pipeline-status` |
| Run or interpret the quality checks | `data-quality` |

Read the leaf with `Read .claude/skills/<name>/SKILL.md`. If a request spans two
("backfill Q1 and check it landed"), load them in sequence, not upfront.

## Out of scope: modeling, and everything about using Metabase

These skills stop at `raw_<source>.*`. They do **not** author transforms, metrics,
segments, dashboards or column metadata, and they do not carry a method for
versioning Metabase content.

That work belongs to `mb-cli`, whose skills are written by the Metabase engineers
who build the product and versioned with the binary, so they never go stale the
way a copy here would:

```bash
mb skills list
mb skills get data-workflow --max-bytes 0    # end-to-end data work
mb skills get transform --max-bytes 0        # authoring transforms
mb skills get git-sync --max-bytes 0         # versioning content in git
```

Every command self-describes too — `mb transform create --help --json` returns its
input and output JSON Schema. Trust that over anything written here.

Asked to model data or define a metric, say plainly that this repo lands the raw
data and hosts the warehouse, but that modeling happens against them from the
mb-cli project — and point at those skills rather than re-deriving the method.

What *does* stay here is the orchestration: `make mb-transforms` builds whatever
`metabase/transforms/manifest.yml` declares, as a step in the ingest DAG. Running
it is this repo's job; deciding what goes in it is not.

## Shared contract

**Answer first.** Lead with what the user asked, then the evidence. "Ingestion is
healthy — last run 12 minutes ago, 340 new rows, all checks passed" beats a
transcript of five commands.

**Name the source.** With several connected, "ingestion is fine" is wrong if one
of three has been failing for a day. `ls sources/` is always the first thing to
check.

**Read the spec before answering questions about behaviour.** Schedule, rate
limits, what is incremental, what "fresh" means and which resources are required
are all declared in `sources/<name>.yml`. Guessing at them is how you tell someone
a backfill will take minutes when it will take hours.

**Never print secrets.** `.env` holds every source's API token plus the Metabase
licence and API key. Read it only to check whether a variable is *set*; never echo
a value or paste one into a command that gets logged.

**Prefer `make`.** Targets carry the right flags, container and ordering.
`make help` lists them. Reach past make only when diagnosing.

**Say what you did not check.** "Ingest looks fine, I did not look at the quality
results" is useful. Implying coverage you did not verify is not.

## The five-second orientation

```
sources/<name>.yml   the connector contract: API, pagination, incremental, schedule
raw_<source>.*       one database per source, loaded by dlt, merged on primary key
ops.*                gx_results, pipeline_runs, mb_transform_runs
analytics.*           modeled tables — built here, authored elsewhere
```

- `ingest` loads. `dq` validates. `mbx` runs whatever the manifest declares. `mb`
  is the Metabase CLI and owns everything about how Metabase is used.
- Airflow schedules each source separately; each has a **pool of one**, so its
  runs cannot race that source's incremental cursor.
- Anything long-running belongs in a DAG, not in your shell. An out-of-band
  ingest against the production warehouse races the cursor;
  `--destination duckdb` is always safe.
