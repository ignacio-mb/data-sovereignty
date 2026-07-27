"""Prove the plumbing before trusting the pipeline.

Every tool the real DAGs shell out to is reachable, and every service they talk
to answers. When an ingest fails at 03:00, this is what tells you whether the
problem is the pipeline or the stack underneath it.

Deliberately touches no data: safe to run at any time, on any environment.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DEFAULT_ARGS = {
    "retries": 0,
    "execution_timeout": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="stack_smoke",
    description="Verify the CLIs, the warehouse and Metabase are all reachable.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ops"],
) as dag:

    cli_tools = BashOperator(
        task_id="cli_tools",
        bash_command=(
            "set -euo pipefail\n"
            "pylon --help > /dev/null && echo 'pylon ok'\n"
            "dq --help    > /dev/null && echo 'dq ok'\n"
            "mbx --help   > /dev/null && echo 'mbx ok'\n"
            "mb --version                && echo 'mb ok'\n"
        ),
    )

    warehouse = BashOperator(
        task_id="warehouse",
        # Via dq rather than psql: this exercises the same credential resolution
        # the real quality tasks use, so a misconfigured env var fails here.
        bash_command=(
            "set -euo pipefail\n"
            "dq ops-init\n"
        ),
    )

    metabase = BashOperator(
        task_id="metabase",
        # `mb auth status` is the cheapest call that proves both the URL and the
        # API key are right; a missing key exits non-zero rather than hanging.
        bash_command=(
            "set -euo pipefail\n"
            'test -n "${MB_API_KEY:-}" || { echo "MB_API_KEY is empty — has bootstrap run?"; exit 1; }\n'
            "mb auth status --json --max-bytes 0\n"
        ),
    )

    dlt_state = BashOperator(
        task_id="dlt_state",
        # The incremental cursor lives here. If it is not writable, every run
        # silently starts from scratch.
        bash_command=(
            "set -euo pipefail\n"
            'test -w "${DLT_DATA_DIR}" || { echo "${DLT_DATA_DIR} is not writable"; exit 1; }\n'
            'echo "dlt state dir ok: ${DLT_DATA_DIR}"\n'
        ),
    )

    cli_tools >> [warehouse, metabase, dlt_state]
