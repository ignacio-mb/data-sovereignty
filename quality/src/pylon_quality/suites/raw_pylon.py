"""Expectations for the six raw tables dlt loads from Pylon.

These check the ingest contract, not business meaning: primary keys behave like
primary keys, the data is fresh, tombstones stay plausible, and children point at
parents that exist. Anything about what the numbers *mean* belongs in the mart
suites, after modeling.
"""

from great_expectations import expectations as gxe

from ..config import RAW_SCHEMA, freshness_hours

# Every resource merges on id, so a duplicate id means the merge key broke.
ENTITY_TABLES = ("issues", "issue_messages", "accounts", "users", "teams", "contacts")

# Tables that get the soft-delete reconciliation pass. A run that tombstones
# nearly everything means the "absent from this load" predicate misfired —
# usually a partial fetch that should never have been eligible.
SOFT_DELETE_TABLES = ("accounts", "users", "teams", "contacts")
MAX_DELETED_FRACTION = 0.5

# dlt only creates a table once a resource yields its first row, so a tenant
# with no teams simply has no raw_pylon.teams — absence there is a fact about
# the tenant, not a fault. issues is the exception: nothing else in the warehouse
# means anything without it, so its absence stays a hard failure rather than a
# table quietly dropped from the run.
REQUIRED_TABLES = ("issues",)


def _identity(table):
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="id"),
        gxe.ExpectColumnValuesToBeUnique(column="id"),
        gxe.ExpectTableRowCountToBeBetween(
            min_value=1,
            description=f"{table} has at least one row (an empty table means the fetch failed silently)",
        ),
    ]


def _freshness(table, column):
    """Advisory, not a gate.

    This measures how long ago the TENANT last touched a ticket, which is only a
    proxy for "is ingestion working" while the tenant is busy. On a quiet
    weekend it fails while every part of the pipeline is healthy, and a check
    that reddens the DAG for a non-problem trains you to stop reading red DAGs.

    Whether ingestion actually ran is a question about runs, not rows, and
    ops.pipeline_runs answers it — so this stays recorded and reported, but does
    not fail the checkpoint.
    """
    hours = freshness_hours()
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT 1 FROM {{batch}} HAVING max({column}) < now() - interval '{hours} hours'"
        ),
        description=(
            f"{table}.{column} is within the last {hours}h "
            f"(advisory: a quiet tenant looks the same as a stalled pipeline here)"
        ),
        meta={"severity": "warn"},
    )


def _soft_delete_sanity(table):
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT 1 FROM {{batch}} HAVING "
            f"avg(CASE WHEN _deleted THEN 1.0 ELSE 0.0 END) > {MAX_DELETED_FRACTION}"
        ),
        description=(
            f"at most {MAX_DELETED_FRACTION:.0%} of {table} is tombstoned "
            f"(more than that means a partial fetch was soft-deleted)"
        ),
    )


def _orphans(child_column, parent_table, description):
    # {batch} expands to `(SELECT ... WHERE true) AS subselect` — GX supplies the
    # alias itself, so aliasing it again is a syntax error. Wrapping it in one
    # more subquery is what buys back a name to correlate the NOT EXISTS against,
    # without depending on GX's alias staying called `subselect`.
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT child.{child_column} FROM (SELECT {child_column} FROM {{batch}}) child "
            f"WHERE child.{child_column} IS NOT NULL AND NOT EXISTS ("
            f"  SELECT 1 FROM {RAW_SCHEMA}.{parent_table} parent WHERE parent.id = child.{child_column}"
            f")"
        ),
        description=description,
    )


def build(present=None):
    """{(schema, table): [Expectation]} for the raw layer.

    `present` is the set of tables actually in the warehouse; None means assume
    every one of them. Filtering happens here rather than at the caller because
    the cross-table checks are the awkward part: an orphan check reads its parent
    table by name, so it has to disappear along with the parent.

    Plain lists, not ExpectationSuite objects: GX only lets you add expectations
    to a suite that is already registered with a live data context, so assembling
    the suites is the context's job.
    """
    def landed(table):
        return present is None or table in present

    suites = {}

    for table in ENTITY_TABLES:
        if not landed(table):
            continue
        expectations = _identity(table)
        if table in SOFT_DELETE_TABLES:
            expectations.append(_soft_delete_sanity(table))
        suites[(RAW_SCHEMA, table)] = expectations

    # Issues carry the pipeline's freshness signal: the incremental cursor tracks
    # updated_at, so a stale max means ingestion stopped advancing.
    if landed("issues"):
        suites[(RAW_SCHEMA, "issues")] += [
            gxe.ExpectColumnValuesToNotBeNull(column="created_at"),
            _freshness("issues", "updated_at"),
        ]
        if landed("accounts"):
            suites[(RAW_SCHEMA, "issues")].append(
                _orphans("account_id", "accounts",
                         "every issue's account_id resolves to a loaded account"))

    if landed("issue_messages"):
        suites[(RAW_SCHEMA, "issue_messages")] += [
            gxe.ExpectColumnValuesToNotBeNull(column="issue_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="timestamp"),
        ]
        if landed("issues"):
            suites[(RAW_SCHEMA, "issue_messages")].append(
                _orphans("issue_id", "issues", "every message belongs to a loaded issue"))

    return suites
