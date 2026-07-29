"""The ops database: what the pipeline knows about itself.

Everything in here is read by the Metabase "Pipeline Health" dashboard, which is
the point — pipeline observability belongs in the BI tool, next to the data it
describes, not in a separate console.

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
* **MergeTree, not ReplacingMergeTree**, except for mb_transform_runs, which is
  re-synced from Metabase and would otherwise accumulate a row per sync per run.
  ReplacingMergeTree collapses on the sort key during merges; queries that must
  not see duplicates before a merge use FINAL.
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

    # Mirror of Metabase's own transform run history, pulled through `mb`. Kept
    # here so one dashboard can join ingest, quality and modeling on a timeline
    # without querying the Metabase application database.
    f"""
    CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.mb_transform_runs (
        run_id          Int64,
        transform_id    Nullable(Int64),
        transform_name  Nullable(String),
        status          Nullable(String),
        started_at      Nullable(DateTime64(6, 'UTC')),
        ended_at        Nullable(DateTime64(6, 'UTC')),
        message         Nullable(String),
        synced_at       DateTime64(6, 'UTC') DEFAULT now64(6)
    ) ENGINE = ReplacingMergeTree(synced_at)
    ORDER BY run_id
    """,
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
        f"{OPS_SCHEMA}.mb_transform_runs",
    ]
