"""dlt pipeline factory and warehouse-side queries."""

import logging
import os
from datetime import timedelta
from pathlib import Path

import dlt
import pendulum
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from .ingest.settings import DATASET_NAME, MESSAGE_WATERMARK_FUDGE_SECONDS, PRODUCTION_DESTINATION

log = logging.getLogger(__name__)

PIPELINE_NAME = "pylon"

# Resolved from the package, not the cwd: dlt only picks up schemas/ when it can
# find the directory, and Airflow workers run from an arbitrary cwd. Tests point
# PYLON_SCHEMA_DIR at a tmp dir so they never write into the repo.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR_ENV = "PYLON_SCHEMA_DIR"


def schema_dir():
    override = os.environ.get(SCHEMA_DIR_ENV)
    return Path(override) if override else PROJECT_ROOT / "schemas"


def build_pipeline(destination=PRODUCTION_DESTINATION, dataset_name=DATASET_NAME):
    """Tables land in the `raw_pylon` schema of the warehouse database.

    Schema YAML is exported (and imported, when reviewed overrides exist) from
    schemas/, making schema evolution show up as a git diff.
    """
    kwargs = {}
    base = schema_dir()
    export_dir = base / "export"
    import_dir = base / "import"
    if export_dir.is_dir():
        kwargs["export_schema_path"] = str(export_dir)
    if import_dir.is_dir():
        kwargs["import_schema_path"] = str(import_dir)
    # Non-production destinations get their own pipeline (working dir + state),
    # so a local duckdb smoke run can't advance the production incremental
    # cursor that lives in the shared <DLT_DATA_DIR>/pipelines/<name> state.
    pipeline_name = (
        PIPELINE_NAME if destination == PRODUCTION_DESTINATION else f"{PIPELINE_NAME}_{destination}"
    )
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        **kwargs,
    )


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
