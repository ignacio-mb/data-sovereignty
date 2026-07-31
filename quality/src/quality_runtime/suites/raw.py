"""Expectations for a source's raw layer, generated from its spec.

Nothing here knows about any particular API. `sources/<name>.yml` declares both
what to fetch and what "arrived correctly" means for it, and this module turns the
second half of that contract into Great Expectations objects.

These check the INGEST contract, not business meaning: primary keys behave like
primary keys, declared columns are populated, the data is fresh, tombstones stay
plausible, and children point at parents that exist. What the numbers *mean* is
not this repo's question — it ingests and orchestrates, and stops there.

The spec is the single place the contract is written, which is the point: a
connector whose expectations lived in Python here would be a second file to edit,
and the two would drift. It also means the checks cannot be forgotten — every
resource gets its identity checks whether the spec author thought about them or
not.
"""

from great_expectations import expectations as gxe

from ..config import freshness_hours

# Absence of a table is normally a fact about the tenant rather than a fault: dlt
# only creates a table once a resource yields its first row, so a tenant with no
# teams simply has no teams table. `quality.required` names the exceptions — the
# tables without which the rest of the source means nothing.
DEFAULT_MAX_DELETED_FRACTION = 0.5


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


def _key_columns(resource):
    """The merge key as a tuple. A spec may declare one column or several."""
    declared = resource.primary_key
    if isinstance(declared, str):
        return (declared,)
    return tuple(declared)


def _identity(resource):
    """Every resource merges on its primary key, so a duplicate there means the
    merge key broke — the one failure that silently corrupts an incremental load."""
    table = resource.name
    expectations = []
    for column in _key_columns(resource):
        expectations.append(not_null(table, column))
    columns = _key_columns(resource)
    if len(columns) == 1:
        expectations.append(unique(table, columns[0]))
    else:
        # A composite key is unique as a tuple; each column alone is not.
        joined = ", ".join(columns)
        expectations.append(gxe.UnexpectedRowsExpectation(
            unexpected_rows_query=(
                f"SELECT {joined} FROM {{batch}} GROUP BY {joined} HAVING count(*) > 1"
            ),
            description=f"{table} ({joined}) is unique",
        ))
    expectations.append(gxe.ExpectTableRowCountToBeBetween(
        min_value=1,
        description=f"{table} has at least one row (an empty table means the fetch failed silently)",
    ))
    return expectations


def _freshness(table, column, hours, severity):
    """How long ago the SOURCE last changed a record.

    Advisory by default, and the default is the interesting part: this measures
    upstream activity, which is only a proxy for "is ingestion working" while the
    upstream is busy. On a quiet weekend it fails while every part of the pipeline
    is healthy, and a check that reddens the DAG for a non-problem trains you to
    stop reading red DAGs.

    Whether ingestion actually ran is a question about runs, not rows, and
    ops.pipeline_runs answers it. A spec whose source is genuinely expected to
    change every hour can set `severity: error` and gate on this instead.
    """
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT 1 FROM {{batch}} HAVING max({column}) < now() - INTERVAL {hours} HOUR"
        ),
        description=(
            f"{table}.{column} is within the last {hours}h"
            + (" (advisory: a quiet source looks the same as a stalled pipeline here)"
               if severity == "warn" else "")
        ),
        meta={"severity": severity},
    )


def _soft_delete_sanity(table, fraction):
    """A run that tombstones most of a table means the "absent from this load"
    predicate misfired — usually a partial fetch that was never eligible."""
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT 1 FROM {{batch}} HAVING "
            f"avg(CASE WHEN _deleted THEN 1.0 ELSE 0.0 END) > {fraction}"
        ),
        description=(
            f"at most {fraction:.0%} of {table} is tombstoned "
            f"(more than that means a partial fetch was soft-deleted)"
        ),
    )


def _orphans(database, child_column, parent_table, parent_column, description, severity):
    """Child rows whose foreign key has no parent, as a LEFT ANTI JOIN.

    Gates by default: a dangling key usually means the parent resource
    under-fetched, which is a pipeline fault. Some APIs genuinely keep children
    after deleting the parent, though — Swoogo serves line items for registrants
    it 404s on — and there the edge is a property of the source, not a fault.
    Such an edge sets `severity: warn` in the spec so the orphan count stays
    visible without reddening every run for something that will never be fixed.

    Not a correlated `NOT EXISTS`, which is what this used to be: ClickHouse
    rejects a subquery that references an outer column with

        Code: 1. Resolve identifier 'child.account_id' from parent scope only
        supported for constants and CTE  (UNSUPPORTED_METHOD)

    A small fixture can slip through — the planner does not always reach that
    path — so this only failed once there was real data in the table. LEFT ANTI
    JOIN is the idiomatic form and means the same thing: keep the left rows that
    matched nothing. Unlike a LEFT JOIN it never materializes null-filled rows,
    so ClickHouse's join_use_nulls behaviour cannot quietly turn a miss into a
    match.

    {batch} expands to `(SELECT ... WHERE true) AS subselect` — GX supplies that
    alias itself, so aliasing it again is a syntax error. Wrapping it in one more
    subquery buys back a name to join against without depending on GX's alias
    staying called `subselect`.
    """
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT child.{child_column} "
            f"FROM (SELECT {child_column} FROM {{batch}}) AS child "
            f"LEFT ANTI JOIN {database}.{parent_table} AS parent "
            f"ON parent.{parent_column} = child.{child_column} "
            f"WHERE child.{child_column} IS NOT NULL"
        ),
        description=description + (
            " (advisory: the source itself retains children whose parent is gone)"
            if severity == "warn" else ""
        ),
        meta={"severity": severity},
    )


def _split(reference):
    """"table.column" -> ("table", "column"). Spec validation guarantees the dot."""
    table, column = str(reference).split(".", 1)
    return table, column


def required_tables(spec):
    """Tables whose absence is a hard failure rather than a quiet skip."""
    return tuple(spec.quality.get("required") or ())


def entity_tables(spec):
    """Every table this source lands, whether or not it has yet."""
    return tuple(resource.name for resource in spec.resources)


def build(spec, present=None):
    """{(database, table): [Expectation]} for one source's raw layer.

    `present` is the set of tables actually in the warehouse; None means assume
    every one of them. Filtering happens here rather than at the caller because
    the cross-table checks are the awkward part: an orphan check reads its parent
    table by name, so it has to disappear along with the parent.

    Plain lists, not ExpectationSuite objects: GX only lets you add expectations
    to a suite that is already registered with a live data context, so assembling
    the suites is the context's job.
    """
    database = spec.dataset
    contract = spec.quality

    def landed(table):
        return present is None or table in present

    suites = {}

    fraction = contract.get("max_deleted_fraction", DEFAULT_MAX_DELETED_FRACTION)
    for resource in spec.resources:
        if not landed(resource.name):
            continue
        expectations = _identity(resource)
        if resource.soft_delete:
            expectations.append(_soft_delete_sanity(resource.name, fraction))
        suites[(database, resource.name)] = expectations

    for table, columns in (contract.get("not_null") or {}).items():
        if not landed(table) or (database, table) not in suites:
            continue
        for column in columns:
            suites[(database, table)].append(not_null(table, column))

    freshness = contract.get("freshness") or {}
    if freshness and landed(freshness["table"]):
        key = (database, freshness["table"])
        if key in suites:
            suites[key].append(_freshness(
                freshness["table"],
                freshness["column"],
                # The spec declares the SLO; GX_FRESHNESS_HOURS lets an operator
                # loosen it for one run without editing the contract.
                freshness_hours(default=freshness.get("hours", 24)),
                freshness.get("severity", "warn"),
            ))

    for edge in contract.get("references") or []:
        child_table, child_column = _split(edge["child"])
        parent_table, parent_column = _split(edge["parent"])
        # Both ends have to be there: the check names the parent table in SQL, so
        # keeping it without the parent trades one missing-table error for another.
        if not landed(child_table) or not landed(parent_table):
            continue
        key = (database, child_table)
        if key not in suites:
            continue
        suites[key].append(_orphans(
            database, child_column, parent_table, parent_column,
            f"every {child_table}.{child_column} resolves to a loaded {parent_table}",
            edge.get("severity", "error"),
        ))

    return suites
