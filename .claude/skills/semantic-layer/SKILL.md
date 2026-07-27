---
name: semantic-layer
description: Add or change Metabase metrics, segments, column metadata, Library publishing, and dashboards. Triggers — "add a metric", "define first response time", "create a segment for enterprise accounts", "these column names are ugly", "build a dashboard", "publish this to the Library".
allowed-tools: Bash, Read, Write, Edit, Grep, AskUserQuestion
---

# The semantic layer

The layer that lets someone answer a question without writing SQL. It sits on
top of the `metrics_*` marts and is built by `mbx`, not by clicking.

```bash
make mb-semantics    # metrics and segments
make mb-metadata     # display names, semantic types, foreign keys
make mb-dashboards   # the two dashboards
```

Definitions live in `metabase/src/mb_tools/definitions.py`. Every subcommand is
idempotent and matches existing objects by name.

## The one rule that matters

**A metric is defined exactly once.**

"Total tickets" is one metric. "Enterprise tickets", "tickets last quarter" and
"tickets by team" are all that same metric plus a segment or a breakout — not
new metrics. The moment two definitions of the same idea exist, they drift, and
then two dashboards disagree and nobody knows which is right.

When a user asks for a metric that already exists in a narrower form, say so and
offer the segment instead of building a second one.

The corollary is that metrics stay arithmetically simple — `sum()`,
`count(distinct)`, and arithmetic *over* those. If a definition needs a `CASE`,
that conditional belongs in the transform layer as a precomputed column
(`model-data` skill), not in the metric.

## Metrics vs segments vs measures

- **Segment** — a saved filter. "Enterprise accounts", "breached SLA". Prefer
  filtering on a precomputed numeric flag column over a string comparison.
- **Metric** — a reusable aggregation living in a collection, usable across
  questions and dashboards. This is what a business user picks from.
- **Measure** — an aggregation saved onto a table. Narrower scope; reach for a
  metric unless the aggregation only makes sense for that one table.

If you need detail on the mechanics, load the CLI's own guidance rather than
guessing — it is versioned with the binary:

```bash
mb skills get data-workflow --max-bytes 0
mb skills get metadata --max-bytes 0
```

## Metadata is part of the product

`mbx metadata` applies display names, semantic types, foreign keys, and hides
the columns nobody should see (`_dlt_*` bookkeeping, raw JSON blobs).

Write descriptions that carry the caveat with the number: what is included, what
is excluded, and the known error. That description follows the metric into the
query builder and into search — it is the only place a reader will see the
caveat at the moment they need it. "Median first response, business hours only,
excludes tickets closed without a reply" is worth ten words of extra typing.

Be precise with names. If a column counts billed seats, call it billed seats,
not users. The gap between those two is exactly where trust dies.

## Library publishing is a trust boundary

Publishing marks a table as an official starting point. Publish the `metrics_*`
marts and the blessed dimensions; leave `base_`, `fact_` and anything holding
personal data unpublished but still queryable. Publishing everything makes the
boundary meaningless.

## Two known traps

**`mb dashboard create` silently drops `parameters` and `parameter_mappings`.**
They have to be re-sent in a follow-up `dashboard update` using the real
dashcard ids, and verified with `dashboard get <id> --full` — the compact view
reports zero mappings even when they are set correctly. `mbx dashboards` already
does the two passes; if you build a dashboard by hand, do the same.

**Dashboards use a 24-column grid.** The per-chart default of 12 is half a row,
not full width. Editing `dashcards` replaces the whole array, so an omitted card
is a deleted card.

## Non-additive metrics

Backlog is a point-in-time count and percentiles do not average. Neither rolls
up across days. Keep percentiles confined to the monthly mart, expose
numerator/denominator pairs rather than precomputed rates so they aggregate
correctly, and say so in the description. A user summing daily backlog across a
month gets a meaningless number, and the description is the only thing standing
between them and that.
