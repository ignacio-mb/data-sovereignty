"""Persist validation results into ops.gx_results."""

import json
import logging
import os

from .config import OPS_SCHEMA

log = logging.getLogger(__name__)

# Column order for the clickhouse-connect insert. ClickHouse takes rows as
# positional lists rather than named parameters, so this tuple is the contract
# between flatten() and write().
COLUMNS = (
    "checkpoint", "suite", "asset", "expectation", "column_name", "success",
    "severity", "description", "observed_value", "details",
    "dag_id", "dag_run_id", "task_id",
)


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


def _exception_summary(result):
    """Why an expectation raised, if it did.

    GX reports a raised expectation as success=False with an empty result, so a
    SQL error and a genuine data failure are indistinguishable in the recorded
    row unless the cause is lifted out of exception_info. Shape varies by GX
    version — sometimes the flags sit at the top level, sometimes one level down
    keyed by metric — so both are handled rather than assumed.
    """
    info = getattr(result, "exception_info", None)
    if not isinstance(info, dict) or not info:
        return None
    entries = [info] if "raised_exception" in info else [
        value for value in info.values() if isinstance(value, dict)
    ]
    for entry in entries:
        if not entry.get("raised_exception"):
            continue
        message = entry.get("exception_message")
        if not message:
            lines = [line for line in (entry.get("exception_traceback") or "").splitlines() if line.strip()]
            message = lines[-1] if lines else "expectation raised, no detail reported"
        return _truncate(message)
    return None


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
            exception = _exception_summary(result)
            meta = dict(getattr(config, "meta", None) or {})
            rows.append({
                "checkpoint": checkpoint_name,
                "suite": suite,
                "asset": asset,
                "expectation": config.type,
                "column_name": kwargs.get("column"),
                "success": bool(result.success),
                # Anything not explicitly marked advisory fails the checkpoint:
                # a suite author has to opt in to being ignorable.
                "severity": "warn" if meta.get("severity") == "warn" else "error",
                # config.description, not kwargs — GX keeps it off kwargs.
                "description": getattr(config, "description", None),
                # An expectation that raised has no observed value; surfacing the
                # error here means `select observed_value ... where not success`
                # answers "what went wrong" for both kinds of failure.
                "observed_value": _truncate(result.result.get("observed_value")) or exception,
                "details": json.dumps({
                    "kwargs": {k: v for k, v in kwargs.items() if k != "batch_id"},
                    "result": result.result,
                    "exception": exception,
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


def write(rows):
    if not rows:
        log.warning("no validation results to persist")
        return 0
    from .context import clickhouse_client

    client = clickhouse_client(database=OPS_SCHEMA)
    try:
        client.insert(
            "gx_results",
            [[row[column] for column in COLUMNS] for row in rows],
            column_names=list(COLUMNS),
        )
    finally:
        client.close()
    log.info("wrote %d rows to %s.gx_results", len(rows), OPS_SCHEMA)
    return len(rows)
