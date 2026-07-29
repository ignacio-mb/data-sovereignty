"""dlt pipeline factory and warehouse-side queries."""

import logging
import os
from datetime import timedelta
from pathlib import Path

import dlt
import pendulum
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from .ingest.settings import MESSAGE_WATERMARK_FUDGE_SECONDS, PRODUCTION_DESTINATION

log = logging.getLogger(__name__)

# Resolved from the package, not the cwd: dlt only picks up schemas/ when it can
# find the directory, and Airflow workers run from an arbitrary cwd. Tests point
# the override at a tmp dir so they never write into the repo.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR_ENV = "PYLON_SCHEMA_DIR"

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


def build_pipeline(source=None, destination=PRODUCTION_DESTINATION, dataset_name=None):
    """Tables land in `raw_<source>` — a ClickHouse database, a duckdb schema.

    ClickHouse has no schemas, so dlt puts every table in the credentials
    database and prefixes it with the dataset name: dataset "raw_pylon" yields
    `raw_pylon.raw_pylon___issues`. An EMPTY dataset name makes dlt skip the
    prefix entirely — `make_qualified_table_name_path` falls through to the bare
    table name — so the tables are plain `raw_pylon.issues`, which is what
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
    source = source or "pylon"
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


def _as_utc(value):
    if isinstance(value, str):
        return pendulum.parse(value)
    return pendulum.instance(value, tz="UTC")


def pending_message_issue_ids(pipeline):
    """Issue ids whose messages are stale: latest_message_time on the issue is
    newer than the newest loaded message (+ a clock fudge). Computed in Python
    from two trivial queries rather than a LEFT JOIN, which keeps it identical
    on every destination — some engines return column defaults instead of NULLs
    for non-matching LEFT JOIN rows, which would silently mark every unmatched
    issue as up to date.

    `title != 'SCRUBBED'` skips tickets the tenant has redacted; they keep a
    latest_message_time that no message fetch can ever satisfy.
    """
    with pipeline.sql_client() as client:
        issues_table = client.make_qualified_table_name("issues")
        try:
            issue_rows = client.execute_sql(
                f"SELECT id, latest_message_time FROM {issues_table} "
                f"WHERE _deleted = false AND title != 'SCRUBBED' "
                f"AND latest_message_time IS NOT NULL "
                f"ORDER BY latest_message_time ASC"
            )
        except DatabaseUndefinedRelation:
            log.info("[issue_messages] issues table does not exist yet — empty worklist")
            return []

        messages_table = client.make_qualified_table_name("issue_messages")
        try:
            watermark_rows = client.execute_sql(
                f"SELECT issue_id, max(timestamp) FROM {messages_table} GROUP BY issue_id"
            )
        except DatabaseUndefinedRelation:
            watermark_rows = []

    watermarks = {issue_id: max_ts for issue_id, max_ts in watermark_rows}
    fudge = timedelta(seconds=MESSAGE_WATERMARK_FUDGE_SECONDS)
    pending = []
    for issue_id, latest_message_time in issue_rows:
        mark = watermarks.get(issue_id)
        if mark is None or _as_utc(latest_message_time) > _as_utc(mark) + fudge:
            pending.append(issue_id)
    return pending


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
