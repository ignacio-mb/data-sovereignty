---
name: model-data
description: Add or change a Metabase transform — the base/dim/fact/metrics layers built from raw Pylon data. Triggers — "model this data", "add a transform", "I need a table with X", "change how resolution time is calculated", "build the fact table", "why is this metric wrong?".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, AskUserQuestion
---

# Modeling

## The contract

`metabase/transforms/manifest.yml` declares every transform; the SQL lives in
`metabase/transforms/sql/`. `mbx transforms` builds them **in manifest order**,
runs each one, and asserts its declared grain. The same manifest generates the
mart quality suites.

```bash
make mb-transforms                                        # build everything
docker compose --profile cli run --rm airflow-cli mbx transforms --only fact_issue
docker compose --profile cli run --rm airflow-cli mbx transforms --dry-run
```

Never author transforms in the Metabase UI. The next `mbx transforms` overwrites
them, and they will not be in git.

## The layers, and why they exist

```
base_     one per raw table: typed, renamed, hot JSON fields extracted
dim_      conformed dimensions (dim_date, dim_account, dim_agent, dim_team)
fact_     grain-locked ledgers (fact_issue: one row per issue)
metrics_  wide, join-free marts a person can filter and group without SQL
```

Two rules carry most of the weight:

**One definition per metric.** "First response time" is computed once, in
`fact_issue`. "First response time for enterprise accounts" is that same column
plus a segment plus a breakout — never a second, subtly different computation.
The moment two SQL files both define it, they will drift and nobody will know
which number is right.

**The column should already exist.** Push conditional logic down into the
transform layer as pre-computed columns and numeric flags (`is_breached`,
`is_reopened`, `first_response_minutes`) so metrics stay pure `sum()` and
`count(distinct)`. This makes metrics composable, caching effective, and the
semantic layer legible to someone who does not write SQL.

## Adding a transform

1. **Look at the real data first.** Do not model from assumptions:
   ```bash
   make psql -- -c "\d raw_pylon.issues"
   make psql -- -c "SELECT custom_fields FROM raw_pylon.issues WHERE custom_fields IS NOT NULL LIMIT 3"
   ```
   Nested Pylon JSON arrives as text, not exploded into columns — deliberately,
   so a new custom field cannot mint a surprise column. Extracting the hot ones
   is exactly what `base_` is for.

2. **Write the SQL** in `metabase/transforms/sql/`, prefixed by layer
   (`10_base_`, `20_dim_`, `30_fact_`, `40_metrics_`). Read against the previous
   layer, not against `raw_pylon`, below `base_`.

3. **Declare it in the manifest**, positioned after its dependencies:
   ```yaml
   - name: fact_issue
     sql: 30_fact_issue.sql
     description: One row per issue, with SLA flags precomputed.
     grain: [issue_id]
     not_null: [issue_id, created_at]
   ```
   The `grain` is not optional bookkeeping — it becomes both the post-build
   assertion and the mart quality check.

4. **Build and verify.**
   ```bash
   make mb-transforms
   docker compose --profile cli run --rm airflow-cli dq run --checkpoint marts
   ```

## When the modeling is substantial, use the stop gates

For a new domain — not for adding one column — the `docs/` deliverables exist so
the decisions get reviewed before they are baked into SQL:

- `00_source_inventory.md` — what is actually in the warehouse, with exact row
  counts, read from `information_schema`. Never from memory or documentation.
- `01_gap_report.md` — for each requested metric: buildable exactly, buildable
  approximately, or not buildable. **Write the honest verdict.** A metric that
  quietly returns zeros because the source field is empty is worse than a
  documented gap.
- `02_assumptions.md` — every judgement call as a named constant
  (`FIRST_RESPONSE_TARGET_MINUTES = 60`), so revisiting one is a one-line change
  rather than an archaeology project.

Stop after each and get the user's agreement. These are the decisions only they
can make.

## Traps

- **Plain table transforms only.** Never `table-incremental`. Metabase v63's
  git-sync serializer drops template tags on incremental transforms during
  import and reports success anyway, which silently breaks them. Full refresh is
  entirely adequate at Pylon ticket volumes.
- **Prefer updating a transform to recreating it.** Recreating mints a new
  entity id and leaves a `_2`-suffixed file in the sync repo. `mbx transforms`
  already updates in place via the name registry.
- **A grain assertion failure is a real bug.** It means a join fans out and
  every metric on that table is inflated. Fix the SQL.
- **Metabase does not always notice new columns.** `mbx transforms` syncs the
  schema at the end; if a column still does not appear, re-run it.
