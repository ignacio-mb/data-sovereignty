"""Persist validation results into ops.gx_results."""

import json
import logging
import os

import psycopg

from .config import OPS_SCHEMA, psycopg_dsn

log = logging.getLogger(__name__)

INSERT = f"""
INSERT INTO {OPS_SCHEMA}.gx_results
    (checkpoint, suite, asset, expectation, column_name, success,
     observed_value, details, dag_id, dag_run_id, task_id)
VALUES (%(checkpoint)s, %(suite)s, %(asset)s, %(expectation)s, %(column_name)s,
        %(success)s, %(observed_value)s, %(details)s, %(dag_id)s, %(dag_run_id)s, %(task_id)s)
"""


def airflow_context():
    """Airflow exports task context as AIRFLOW_CTX_* to every subprocess it runs,
    so a shelled-out `dq run` can stamp its rows without being passed anything."""
    return {
        "dag_id": os.environ.get("AIRFLOW_CTX_DAG_ID"),
        "dag_run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
        "task_id": os.environ.get("AIRFLOW_CTX_TASK_ID"),
    }


def _truncate(value, limit=500):
    if value is None:
        return None
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def flatten(checkpoint_name, checkpoint_result):
    """GX CheckpointResult -> one dict per expectation evaluated."""
    context = airflow_context()
    rows = []
    for validation_result in checkpoint_result.run_results.values():
        suite = validation_result.suite_name
        asset = _asset_name(validation_result)
        for result in validation_result.results:
            config = result.expectation_config
            kwargs = dict(config.kwargs or {})
            rows.append({
                "checkpoint": checkpoint_name,
                "suite": suite,
                "asset": asset,
                "expectation": config.type,
                "column_name": kwargs.get("column"),
                "success": bool(result.success),
                "observed_value": _truncate(result.result.get("observed_value")),
                "details": json.dumps({
                    "kwargs": {k: v for k, v in kwargs.items() if k != "batch_id"},
                    "result": result.result,
                }, default=str),
                **context,
            })
    return rows


def _asset_name(validation_result):
    """Best-effort schema.table for the batch that was validated."""
    batch_spec = getattr(validation_result, "meta", {}).get("batch_spec") or {}
    schema = batch_spec.get("schema_name")
    table = batch_spec.get("table_name")
    if table:
        return f"{schema}.{table}" if schema else table
    # Fall back to the suite name, which we always name after the asset.
    return validation_result.suite_name


def write(rows, dsn=None):
    if not rows:
        log.warning("no validation results to persist")
        return 0
    with psycopg.connect(dsn or psycopg_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(INSERT, rows)
    log.info("wrote %d rows to %s.gx_results", len(rows), OPS_SCHEMA)
    return len(rows)
