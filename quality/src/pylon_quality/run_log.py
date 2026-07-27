"""Record ingest runs into ops.pipeline_runs."""

import json
import logging

import psycopg

from .config import OPS_SCHEMA, psycopg_dsn
from .results import airflow_context

log = logging.getLogger(__name__)

INSERT = f"""
INSERT INTO {OPS_SCHEMA}.pipeline_runs
    (dag_id, dag_run_id, task_id, status, started_at, elapsed_seconds, mode, destination,
     resources, load_ids, rows_this_run, warehouse_counts, requests_by_family)
VALUES (%(dag_id)s, %(dag_run_id)s, %(task_id)s, %(status)s, %(started_at)s, %(elapsed_seconds)s,
        %(mode)s, %(destination)s, %(resources)s, %(load_ids)s, %(rows_this_run)s,
        %(warehouse_counts)s, %(requests_by_family)s)
"""


def record(summary, status="succeeded", dsn=None):
    """Persist one `pylon ingest --summary-json` payload."""
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
    with psycopg.connect(dsn or psycopg_dsn(), autocommit=True) as conn:
        conn.execute(INSERT, row)
    log.info("recorded ingest run (%s) in %s.pipeline_runs", status, OPS_SCHEMA)
