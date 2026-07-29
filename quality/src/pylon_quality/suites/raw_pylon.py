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


def not_null(table, column):
    """`column` is never null, expressed as SQL rather than as GX's own
    ExpectColumnValuesToNotBeNull.

    GX's column expectations compile to `multiIf(cond, CAST(1, Numeric), ...)`,
    and SQLAlchemy renders an unparameterized Numeric as `Decimal(None, None)`,
    which ClickHouse rejects outright:

        Code: 43. DB::Exception: Decimal argument precision is invalid

    That is a clickhouse-sqlalchemy limitation, and it applies to every
    `expect_column_*`. Custom SQL is unaffected, so the whole raw layer is
    written that way. The cost is that a failure reports a count instead of the
    offending values.
    """
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=f"SELECT 1 FROM {{batch}} WHERE {column} IS NULL",
        description=f"{table}.{column} is never null",
    )


def unique(table, column):
    """`column` has no duplicates. Same reason as not_null for the SQL form."""
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT {column} FROM {{batch}} GROUP BY {column} HAVING count(*) > 1"
        ),
        description=f"{table}.{column} is unique",
    )


def _identity(table):
    return [
        not_null(table, "id"),
        unique(table, "id"),
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
            f"SELECT 1 FROM {{batch}} HAVING max({column}) < now() - INTERVAL {hours} HOUR"
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
            not_null("issues", "created_at"),
            _freshness("issues", "updated_at"),
        ]
        if landed("accounts"):
            suites[(RAW_SCHEMA, "issues")].append(
                _orphans("account_id", "accounts",
                         "every issue's account_id resolves to a loaded account"))

    if landed("issue_messages"):
        suites[(RAW_SCHEMA, "issue_messages")] += [
            not_null("issue_messages", "issue_id"),
            not_null("issue_messages", "timestamp"),
        ]
        if landed("issues"):
            suites[(RAW_SCHEMA, "issue_messages")].append(
                _orphans("issue_id", "issues", "every message belongs to a loaded issue"))

    return suites
