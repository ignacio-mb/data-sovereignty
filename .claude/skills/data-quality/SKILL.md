---
name: data-quality
description: Run the ingest quality checkpoints, interpret failures, and add expectations. Triggers — "run the quality checks", "why did validation fail?", "is the ingested data correct?", "add a check for X", "the freshness check keeps failing", "show me the data docs".
allowed-tools: Bash, Read, Edit, Grep
---

# Data quality

One checkpoint per source, checking the **ingest contract**: did the rows arrive,
are the keys keys, is anything stale, do children point at parents that exist.

```bash
make quality                                    # every checkpoint
docker compose --profile cli run --rm airflow-cli dq run --checkpoint raw_<source>
```

Results land in `ops.gx_results` — one row per expectation — and render to data
docs at http://localhost:8081.

## Where the expectations come from

**A source's own spec.** `sources/<name>.yml` has a `quality:` block, and the
suite is built from it rather than hand-written per source:

```yaml
quality:
  required: [issues]              # absent -> hard failure, not a skip
  freshness:                      # advisory by default; see below
    table: issues
    column: updated_at
    hours: 24
    severity: warn
  max_deleted_fraction: 0.5       # more tombstoned than this means a partial fetch
  references:                     # -> LEFT ANTI JOIN checks
    - {child: issues.account_id, parent: accounts.id}
  not_null:
    issues: [created_at]
```

Every resource additionally gets identity checks for free — its primary key
unique and present, and a row count above zero.

To add a check, edit the spec. That keeps the thing that describes the source and
the thing that tests it from disagreeing.

## Absence is not failure

A resource that has never yielded a row has no table: dlt creates one on first
write. Validating the tables that exist beats failing all of them, so a missing
table is *skipped with a message* — unless the spec lists it under `required`,
which is how a source says "nothing here means anything without this one".

## Gating vs advisory

An expectation carrying `severity: warn` is recorded and reported but does **not**
fail the checkpoint. Anything unmarked gates, so being ignorable is opt-in.

Freshness is the usual advisory case, and the reason the distinction exists: it
measures when the *source system* last changed a record, not whether ingestion
works. On a quiet weekend it fails while every part of the pipeline is healthy,
and a check that reddens the DAG for a non-problem teaches people to stop reading
red DAGs. Whether a run happened is a question about runs — `ops.pipeline_runs`
answers it.

## Interpreting a failure

```sql
SELECT asset, description, observed_value, severity
  FROM ops.gx_results
 WHERE NOT success ORDER BY validated_at DESC LIMIT 20;
```

Lead with `description` — it is the sentence the spec author wrote. Then map the
failure to a cause rather than reporting it verbatim:

| Failure | What it usually means |
|---|---|
| Primary key not unique | The dlt merge key broke. Serious — nothing downstream can be trusted until it is understood. |
| Freshness | Either ingestion stopped or the source is genuinely quiet. Check `ops.pipeline_runs` before blaming the pipeline. |
| Tombstone fraction over the cap | A reconcile marked rows deleted after a partial fetch. Check whether a run was interrupted. |
| Orphaned foreign key | Parent and child loaded in different runs, or the parent was deleted upstream. Often self-heals on the next full run. |
| Row count zero | The fetch returned nothing. Distinguish "the source is empty" from "the request was wrong" before calling it either. |

## Two judgement calls

**A failing check is information, not an emergency.** Say what broke, what it
implies for trusting the data, and what you would do — not just that it is red.

**Do not fix a failure by loosening the expectation.** If a uniqueness check
fails, there are duplicate rows and everything reading that table is wrong;
widening the check hides it. The only legitimate reason to change an expectation
is that the expectation was wrong about the source — and that is worth saying out
loud when you do it.

## Thresholds

`GX_FRESHNESS_HOURS` in `.env` sets the default (24). A per-source value belongs
in that source's spec. If a source is genuinely quiet at weekends, widening it
beats a check that cries wolf every Monday — a check nobody believes is worse
than no check.

## Not this skill's job

Checking modeled tables. This checkpoint validates what ingestion landed in
`raw_<source>.*`. Whether a transform's output is correct is a question for
whoever authors the transforms — see the router.
