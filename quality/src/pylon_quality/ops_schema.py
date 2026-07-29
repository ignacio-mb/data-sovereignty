"""The ops schema: what the pipeline knows about itself.

Everything in here is read by the Metabase "Pipeline Health" dashboard, which is
the point — pipeline observability belongs in the BI tool, next to the data it
describes, not in a separate console.

DDL lives in code (not in warehouse/init/) so it can evolve without recreating
the warehouse volume. Every statement is IF NOT EXISTS; `dq ops-init` is safe to
run on every DAG run.
"""

import psycopg

from .config import OPS_SCHEMA, psycopg_dsn

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA};

-- One row per expectation evaluated. Fine-grained on purpose: "which check on
-- which column started failing, and when" is the question worth answering.
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.gx_results (
    id              bigserial PRIMARY KEY,
    validated_at    timestamptz NOT NULL DEFAULT now(),
    checkpoint      text        NOT NULL,
    suite           text        NOT NULL,
    asset           text        NOT NULL,
    expectation     text        NOT NULL,
    column_name     text,
    success         boolean     NOT NULL,
    -- 'error' fails the checkpoint; 'warn' is recorded and reported but does
    -- not. Freshness is the motivating case: on a quiet tenant "no new tickets
    -- for a day" is a fact about the business, not a broken pipeline, and a
    -- check that reddens every run teaches you to ignore red runs.
    severity        text        NOT NULL DEFAULT 'error',
    -- The human sentence the suite author wrote. Without it a failure reads
    -- "unexpected_rows_expectation observed=1", which identifies nothing.
    description     text,
    observed_value  text,
    details         jsonb,
    dag_id          text,
    dag_run_id      text,
    task_id         text
);

-- Added after the table shipped; both are no-ops on a fresh install.
ALTER TABLE {OPS_SCHEMA}.gx_results
    ADD COLUMN IF NOT EXISTS severity text NOT NULL DEFAULT 'error';
ALTER TABLE {OPS_SCHEMA}.gx_results
    ADD COLUMN IF NOT EXISTS description text;
CREATE INDEX IF NOT EXISTS gx_results_validated_at_idx
    ON {OPS_SCHEMA}.gx_results (validated_at DESC);
CREATE INDEX IF NOT EXISTS gx_results_failures_idx
    ON {OPS_SCHEMA}.gx_results (asset, validated_at DESC) WHERE NOT success;

-- One row per ingest run, written from the CLI's --summary-json.
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.pipeline_runs (
    id                  bigserial PRIMARY KEY,
    recorded_at         timestamptz NOT NULL DEFAULT now(),
    dag_id              text,
    dag_run_id          text,
    task_id             text,
    status              text        NOT NULL,
    started_at          timestamptz,
    elapsed_seconds     double precision,
    mode                text,
    destination         text,
    resources           jsonb,
    load_ids            jsonb,
    rows_this_run       jsonb,
    warehouse_counts    jsonb,
    requests_by_family  jsonb
);
CREATE INDEX IF NOT EXISTS pipeline_runs_recorded_at_idx
    ON {OPS_SCHEMA}.pipeline_runs (recorded_at DESC);

-- Mirror of Metabase's own transform run history, pulled through `mb`. Kept
-- here so one dashboard can join ingest, quality and modeling on a timeline
-- without querying the Metabase application database.
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.mb_transform_runs (
    run_id          bigint PRIMARY KEY,
    transform_id    bigint,
    transform_name  text,
    status          text,
    started_at      timestamptz,
    ended_at        timestamptz,
    message         text,
    synced_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mb_transform_runs_started_at_idx
    ON {OPS_SCHEMA}.mb_transform_runs (started_at DESC);
"""


def init(dsn=None):
    """Create the ops schema and its tables. Idempotent."""
    with psycopg.connect(dsn or psycopg_dsn(), autocommit=True) as conn:
        conn.execute(DDL)
    return [
        f"{OPS_SCHEMA}.gx_results",
        f"{OPS_SCHEMA}.pipeline_runs",
        f"{OPS_SCHEMA}.mb_transform_runs",
    ]
