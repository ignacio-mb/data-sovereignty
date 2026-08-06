"""dlt pipeline factory and warehouse-side queries."""

import logging
import os
from pathlib import Path

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation

PRODUCTION_DESTINATION = "clickhouse"

log = logging.getLogger(__name__)

# Resolved from the package, not the cwd: dlt only picks up schemas/ when it can
# find the directory, and Airflow workers run from an arbitrary cwd. Tests point
# the override at a tmp dir so they never write into the repo.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR_ENV = "DS_SCHEMA_DIR"

# dlt reads its destination credentials from the environment. The database is
# the one credential that differs per source, so it is set here at build time
# rather than baked into compose — a single global would put every source's
# tables in one database, sharing one soft-delete pass.
CLICKHOUSE_DATABASE_ENV = "DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"


def schema_dir(source=None):
    """Where dlt exports the schema YAML that makes evolution a git diff.

    Per source, so two connectors cannot overwrite each other's schema file.
    """
    override = os.environ.get(SCHEMA_DIR_ENV)
    base = Path(override) if override else PROJECT_ROOT / "schemas"
    return base / source if source else base


def build_pipeline(source, destination=PRODUCTION_DESTINATION, dataset_name=None):
    """Tables land in `raw_<source>` — a ClickHouse database, a duckdb schema.

    ClickHouse has no schemas, so dlt puts every table in the credentials
    database and prefixes it with the dataset name: dataset "raw_x" yields
    `raw_x.raw_x___things`. An EMPTY dataset name makes dlt skip the
    prefix entirely — `make_qualified_table_name_path` falls through to the bare
    table name — so the tables are plain `raw_x.things`, which is what
    Metabase shows and what every transform and expectation is written against.

    Blanking `dataset_table_separator` as well is tempting and wrong. It changes
    nothing for these tables (there is no prefix left to separate) and it does
    reach the staging dataset, whose layout is `%s_staging`, turning
    `_staging___issues` into the unreadable `_stagingissues`.

    The credentials database must therefore BE raw_<source>, which is set here
    rather than in compose so that each source gets its own.

    Schema YAML is exported (and imported, when reviewed overrides exist) from
    schemas/<source>/, making schema evolution show up as a git diff.
    """
    if dataset_name is None:
        dataset_name = f"raw_{source}"

    kwargs = {}
    base = schema_dir(source)
    export_dir = base / "export"
    import_dir = base / "import"
    if export_dir.is_dir():
        kwargs["export_schema_path"] = str(export_dir)
    if import_dir.is_dir():
        kwargs["import_schema_path"] = str(import_dir)

    # Each source gets its own pipeline, so their incremental cursors live in
    # separate <DLT_DATA_DIR>/pipelines/<name> state and cannot collide.
    #
    # Non-production destinations are namespaced again on top of that, so a
    # local duckdb smoke run can never advance a production cursor.
    pipeline_name = source if destination == PRODUCTION_DESTINATION else f"{source}_{destination}"

    if destination == "clickhouse":
        # dlt selects its database while connecting, so this must be set before
        # the pipeline is built — and it is per source, not per stack.
        os.environ[CLICKHOUSE_DATABASE_ENV] = dataset_name
        dataset_name = ""
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        **kwargs,
    )


def ensure_database(source, destination=PRODUCTION_DESTINATION):
    """Create `raw_<source>` if it is absent. Idempotent, and required.

    ClickHouse selects the database as part of connecting, so dlt fails with
    "Code: 81. Database raw_x does not exist" during its pre-run sync, before it
    could create anything. warehouse/init/ cannot cover this either: it runs
    once, on first initialisation of an empty volume, so a source added later
    would never get a database on a running stack.
    """
    if destination != "clickhouse":
        return  # duckdb creates its schema on write
    import clickhouse_connect

    from .config import clickhouse_admin_kwargs

    database = f"raw_{source}"
    client = clickhouse_connect.get_client(**clickhouse_admin_kwargs())
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    finally:
        client.close()
    log.info("warehouse database %s is present", database)


def warehouse_rows(build_query):
    """Read from the destination this run is loading into.

    For the one strategy whose worklist is a warehouse query rather than an API
    cursor: parent_watermark asks "which parents changed more recently than the
    newest child I already have", and only the warehouse can answer.

    `build_query` receives the qualifier and returns SQL, so an extension never
    has to know whether it is addressing a ClickHouse database or a duckdb
    schema — the same function works against the smoke destination.

    A missing table returns no rows rather than raising. Every connector that
    reads its own destination needs that on its first run, and hand-rolling it
    is how you end up string-matching an exception message.
    """
    import dlt

    # The pipeline is only current while a resource is being extracted, which is
    # exactly when a worklist is built. Reading it here rather than threading a
    # pipeline through every builder keeps the extension signature to the three
    # arguments the contract documents.
    pipeline = dlt.current.pipeline()
    with pipeline.sql_client() as client:
        query = build_query(client.make_qualified_table_name)
        try:
            return client.execute_sql(query)
        except DatabaseUndefinedRelation:
            return []


def table_counts(pipeline, tables):
    """{table: row count or None if the table doesn't exist}."""
    counts = {}
    with pipeline.sql_client() as client:
        for table in tables:
            qualified = client.make_qualified_table_name(table)
            try:
                [(count,)] = client.execute_sql(f"SELECT count(*) FROM {qualified}")
                counts[table] = count
            except DatabaseUndefinedRelation:
                counts[table] = None
    return counts
