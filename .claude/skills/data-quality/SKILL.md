---
name: data-quality
description: Validate a source's raw contract with Great Expectations, interpret failures, and add new checks. Triggers — "run the quality checks", "why did validation fail?", "is the data correct?", "add a check for X", "the freshness check keeps failing", "show me the data docs".
allowed-tools: Bash, Read, Edit, Grep
---

# Data quality

One contract per source, validated against the tables dlt landed: do they say what
the spec claims they say? Nothing downstream of raw is checked here — what the rows
*mean* is not this repo's question.

```bash
make sources                 # what is connected
make quality SOURCE=<name>
docker compose --profile cli run --rm airflow-cli dq run --source <name>
```

Results land in `ops.gx_results` (one row per expectation, checkpoint recorded as
`raw_<source>`) and render to data docs at http://localhost:8081.

If `make sources` lists nothing, there is nothing to validate — the repo ships with
no sources connected. Route to the `add-source` skill.

## Where expectations come from

**Generated from `sources/<name>.yml`, not hand-written.** The builder is
`quality/src/quality_runtime/suites/raw.py`; it reads the spec and produces the GX
objects. Two kinds of check:

- **Always present, per resource** — primary key not null, primary key unique, at
  least one row. Not declared and not optional: a spec author cannot forget them.
  A composite `primary_key` is asserted unique as a tuple rather than per column.
- **Declared in the spec's `quality:` block** — `required` (tables whose absence is
  a hard failure rather than a quiet skip), `freshness` (table, column, hours,
  severity), `max_deleted_fraction`, `references` (child/parent edges, checked as
  `LEFT ANTI JOIN`), and `not_null` (extra columns per table).

So **adding a check for one source is a spec edit**, and adding a new *kind* of
check is a `raw.py` edit. Prefer the first. A check only one source could ever want
belongs in that source's spec.

A table that has never received a row does not exist — dlt creates it on first
write — so it is skipped with a printed note rather than failed, unless the spec
lists it under `required`.

## Interpreting a failure

```sql
SELECT asset, expectation, column_name, observed_value, details
  FROM ops.gx_results
 WHERE NOT success ORDER BY validated_at DESC LIMIT 20;
```

Map the failure to a cause rather than reporting it verbatim:

| Failure | What it usually means |
|---|---|
| Primary key not unique | The dlt merge key broke. Serious — every downstream count is suspect. |
| Freshness | Either ingestion stopped, or the source is genuinely quiet. Check `ops.pipeline_runs` before blaming the pipeline — it answers "did a run happen", which is the actual question. |
| Tombstone fraction over the declared limit | A `--mark-deleted` pass ran after a partial fetch. Check whether a reconcile run was interrupted. |
| An orphaned foreign key | Parent and child were loaded in different runs, or the parent was deleted upstream. Often self-heals on the next full run. |
| "at least one row" on a table that used to have data | A fetch failed silently — the table exists from an earlier run, but this one landed nothing. |

## Two judgement calls

**A failing check is information, not an emergency.** Say what broke, what it
implies for trusting the numbers, and what you would do — not just that it is red.

**Do not fix a failure by loosening the expectation.** If a primary key is no
longer unique, the merge key is broken and every row count is wrong. Widening the
check hides that. The only legitimate reason to change an expectation is that it
was wrong about the source, and that is worth saying out loud when you do it.

## Thresholds

Freshness lives in the spec, per source: `quality.freshness.hours`, with
`severity: warn` (recorded and reported, does not fail) or `severity: error`
(gates). Warn is the right default — a check that reddens the DAG when the source
is merely quiet teaches people to stop reading red DAGs.

`GX_FRESHNESS_HOURS` in `.env` overrides **every** source's SLO at once. It is for
loosening things for one run without editing a contract, not for expressing one.
If a source is legitimately quiet at weekends, raise it in that source's spec.
