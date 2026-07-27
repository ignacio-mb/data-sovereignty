"""Shared pieces of the Pylon DAGs.

The DAGs shell out to `pylon`, `dq` and `mbx` rather than importing them. Those
tools live in their own virtualenv at /opt/data-venv with dlt and Great
Expectations behind them; importing that tree into Airflow's own environment
would mean reconciling two large, tightly-pinned dependency graphs forever.
"""

from __future__ import annotations

import pendulum

# Serializes every dlt run. Two concurrent ingests share one pipeline working
# directory and one incremental cursor, and would interleave into nonsense.
INGEST_POOL = "pylon_pipeline"

# Written by the ingest task, read by the ops task. Per-run so a backfill and an
# hourly run can never read each other's summary.
SUMMARY_PATH = "/tmp/pylon-summary-{{ run_id | replace('/', '_') | replace(':', '-') }}.json"

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "depends_on_past": False,
}


def ingest_command(extra_args=""):
    """`pylon ingest`, always writing a summary for the ops task to record.

    `set -o pipefail` matters: without it the exit status would come from tee.
    """
    return (
        "set -euo pipefail\n"
        f"pylon ingest --destination postgres --summary-json '{SUMMARY_PATH}' {extra_args}\n"
    )


def record_ops_command():
    """Record the run even when an upstream task failed — a failed run is exactly
    the one worth having in the history. Missing summary means ingest never got
    far enough to write one, which is itself the finding."""
    return (
        "set -uo pipefail\n"
        f"if [ -f '{SUMMARY_PATH}' ]; then\n"
        f"  dq record-run '{SUMMARY_PATH}' --status \"${{INGEST_STATUS:-succeeded}}\"\n"
        f"  rm -f '{SUMMARY_PATH}'\n"
        "else\n"
        "  echo 'no ingest summary — the ingest task did not complete'\n"
        "fi\n"
        "dq ops-sync || echo 'transform run sync failed (Metabase may be down); continuing'\n"
    )
