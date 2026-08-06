"""The ops database: what the pipeline knows about itself.

Every quality verdict and every ingest run lands here, in the warehouse rather
than in a log the pipeline throws away — so the history is queryable from
Metabase alongside the data it describes, or from `make ch`.

DDL lives in code (not in warehouse/init/) so it can evolve without recreating
the warehouse volume. Every statement is IF NOT EXISTS; `dq ops-init` is safe to
run on every DAG run.

── ClickHouse shapes these tables differently from Postgres ──

* **No sequences.** There is no bigserial and no identity column. The tables are
  append-only logs and nothing joins on a surrogate id, so they simply do not
  have one; the natural key is in ORDER BY instead.
* **No partial indexes.** ClickHouse's sort key is the index. Ordering
  gx_results by (validated_at, asset) serves both "the last run" and "failures
  for this table", which is what the two Postgres indexes did.
* **JSON is stored as String.** ClickHouse's JSON type is still moving; a String
  column holding JSON text is portable, and Metabase reads it either way.
* **MergeTree, not ReplacingMergeTree.** Both tables are append-only logs with
  nothing to collapse on: a second row for the same key is a second event, not a
  restatement of the first.
"""

import logging

from .config import OPS_SCHEMA
from .context import clickhouse_client

log = logging.getLogger(__name__)

STATEMENTS = [
    f"CREATE DATABASE IF NOT EXISTS {OPS_SCHEMA}",

    # One row per expectation evaluated. Fine-grained on purpose: "which check on
    # which column started failing, and when" is the question worth answering.
    f"""
    CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.gx_results (
        validated_at    DateTime64(6, 'UTC') DEFAULT now64(6),
        checkpoint      String,
        suite           String,
        asset           String,
        expectation     String,
        column_name     Nullable(String),
        success         Bool,
        -- 'error' fails the checkpoint; 'warn' is recorded and reported but does
        -- not. Freshness is the motivating case: on a quiet tenant "no new
        -- tickets for a day" is a fact about the business, not a broken
        -- pipeline, and a check that reddens every run teaches you to ignore
        -- red runs.
        severity        String DEFAULT 'error',
        -- The human sentence the suite author wrote. Without it a failure reads
        -- "unexpected_rows_expectation observed=1", which identifies nothing.
        description     Nullable(String),
        observed_value  Nullable(String),
        details         Nullable(String),
        dag_id          Nullable(String),
        dag_run_id      Nullable(String),
        task_id         Nullable(String)
    ) ENGINE = MergeTree
    ORDER BY (validated_at, asset, expectation)
    """,
    # Added after the table shipped; no-ops on a fresh install.
    f"ALTER TABLE {OPS_SCHEMA}.gx_results ADD COLUMN IF NOT EXISTS severity String DEFAULT 'error'",
    f"ALTER TABLE {OPS_SCHEMA}.gx_results ADD COLUMN IF NOT EXISTS description Nullable(String)",

    # One row per ingest run, written from the CLI's --summary-json.
    f"""
    CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.pipeline_runs (
        recorded_at         DateTime64(6, 'UTC') DEFAULT now64(6),
        source              Nullable(String),
        dag_id              Nullable(String),
        dag_run_id          Nullable(String),
        task_id             Nullable(String),
        status              String,
        started_at          Nullable(DateTime64(6, 'UTC')),
        elapsed_seconds     Nullable(Float64),
        mode                Nullable(String),
        destination         Nullable(String),
        resources           Nullable(String),
        load_ids            Nullable(String),
        rows_this_run       Nullable(String),
        warehouse_counts    Nullable(String),
        requests_by_family  Nullable(String)
    ) ENGINE = MergeTree
    ORDER BY recorded_at
    """,
    # Added after the table shipped; no-ops on a fresh install. Rows written
    # before this stay null, which is honest — the source was never recorded.
    (f"ALTER TABLE {OPS_SCHEMA}.pipeline_runs ADD COLUMN IF NOT EXISTS "
     f"source Nullable(String) AFTER recorded_at"),
]


def init():
    """Create the ops database and its tables. Idempotent."""
    client = clickhouse_client()
    try:
        for statement in STATEMENTS:
            client.command(statement)
    finally:
        client.close()
    return [
        f"{OPS_SCHEMA}.gx_results",
        f"{OPS_SCHEMA}.pipeline_runs",
    ]
