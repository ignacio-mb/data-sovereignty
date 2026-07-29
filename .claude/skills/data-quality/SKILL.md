---
name: data-quality
description: Run Great Expectations checkpoints, interpret failures, and add new expectations. Triggers — "run the quality checks", "why did validation fail?", "is the data correct?", "add a check for X", "the freshness check keeps failing", "show me the data docs".
allowed-tools: Bash, Read, Edit, Grep
---

# Data quality

One checkpoint per source. `raw_pylon` checks the ingest contract — that the
tables dlt landed say what the pipeline claims they say.

```bash
make quality
docker compose --profile cli run --rm airflow-cli dq run --checkpoint raw_pylon
```

Results land in `ops.gx_results` (one row per expectation) and render to data
docs at http://localhost:8081.

## Where expectations come from

Hand-written in `quality/src/quality_runtime/suites/raw_pylon.py`. Identity (`id`
unique and present), freshness, tombstone sanity, and referential checks between
children and parents.

## Interpreting a failure

```sql
SELECT asset, expectation, column_name, observed_value, details
  FROM ops.gx_results
 WHERE NOT success ORDER BY validated_at DESC LIMIT 20;
```

Map the failure to a cause rather than reporting it verbatim:

| Failure | What it usually means |
|---|---|
| `id` not unique in a raw table | The dlt merge key broke. Serious — investigate before modeling on top of it. |
| Freshness on `issues.updated_at` | Either ingestion stopped, or the tenant is genuinely quiet. Check `ops.pipeline_runs` before blaming the pipeline. |
| Tombstone fraction over 50% | A `--mark-deleted` pass ran after a partial fetch. Check whether a reconcile run was interrupted. |
| Orphan `account_id` / `issue_id` | Parent and child were loaded in different runs, or a parent was deleted in Pylon. Often self-heals on the next full run. |

## Two judgement calls

**A failing check is information, not an emergency.** Say what broke, what it
implies for trusting the numbers, and what you would do — not just that it is
red.

**Do not fix a failure by loosening the expectation.** If `id` is no longer
unique, the merge key is broken and every row count downstream is wrong. Widening
the check hides that. The only legitimate reason to change an expectation is that
the expectation itself was wrong about the business, and that is worth saying
out loud when you do it.

## Thresholds

`GX_FRESHNESS_HOURS` in `.env` (default 24). If the tenant has genuinely quiet
weekends, raising it beats having a check that cries wolf every Monday — a check
nobody believes is worse than no check.
