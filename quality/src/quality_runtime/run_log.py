"""Record ingest runs into ops.pipeline_runs."""

import json
import logging

from .config import OPS_SCHEMA
from .results import airflow_context

log = logging.getLogger(__name__)

COLUMNS = (
    "dag_id", "dag_run_id", "task_id", "status", "started_at", "elapsed_seconds",
    "mode", "destination", "resources", "load_ids", "rows_this_run",
    "warehouse_counts", "requests_by_family",
)


def record(summary, status="succeeded"):
    """Persist one `ingest run --summary-json` payload."""
    row = {
        **airflow_context(),
        "status": status,
        "started_at": summary.get("started_at"),
        "elapsed_seconds": summary.get("elapsed_seconds"),
        "mode": summary.get("mode"),
        "destination": summary.get("destination"),
        "resources": json.dumps(summary.get("resources") or []),
        "load_ids": json.dumps(summary.get("load_ids") or []),
        "rows_this_run": json.dumps(summary.get("rows_this_run") or {}),
        "warehouse_counts": json.dumps(summary.get("warehouse_counts") or {}, default=str),
        "requests_by_family": json.dumps(summary.get("requests_by_family") or {}),
    }
    from .context import clickhouse_client

    # started_at arrives as an ISO string; the driver wants a datetime for a
    # DateTime64 column, and a bad parse should not lose the whole run record.
    row["started_at"] = _as_datetime(row["started_at"])

    client = clickhouse_client(database=OPS_SCHEMA)
    try:
        client.insert(
            "pipeline_runs",
            [[row[column] for column in COLUMNS]],
            column_names=list(COLUMNS),
        )
    finally:
        client.close()
    log.info("recorded ingest run (%s) in %s.pipeline_runs", status, OPS_SCHEMA)


def _as_datetime(value):
    if not value or not isinstance(value, str):
        return value
    from datetime import datetime

    try:
        # Python 3.11+ parses a trailing Z natively.
        return datetime.fromisoformat(value)
    except ValueError:
        log.warning("could not parse started_at %r — recording it as null", value)
        return None
