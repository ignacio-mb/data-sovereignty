"""One set of DAGs per connected source, built from the specs in sources/.

There are deliberately no per-source DAG files. A connector is a spec; its
schedule, timeouts and pool live there, so a hand-written DAG per source would be
a second place to change when one of those moves — and the two would drift.

With no specs, this module contributes no DAGs. That is the intended empty state:
a fresh checkout has nothing to ingest, and nothing pretending to.

Each DAG is the same three steps: land the rows, verify the contract they were
supposed to arrive under, record what happened. What the rows go on to *mean* is
not modelled here — this repo ingests and orchestrates, and stops there.

DAGs shell out to `ingest` and `dq` rather than importing them. The runtime lives
in its own virtualenv precisely so dlt and Great Expectations can never drag
Airflow's dependency tree into a conflict.
"""

from __future__ import annotations

import logging
import os
import sys

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule
from common import ingest_command, record_ops_command, run_verdict

log = logging.getLogger(__name__)

# The specs are the contract, and the DAG processor needs to read them without
# importing the ingest package (which lives in the other virtualenv). Reading
# YAML is the whole dependency.
SOURCES_DIR = os.environ.get("DS_SOURCES_DIR", "/opt/project/sources")


def _specs():
    """Every source spec, or an empty list. Never raises.

    A malformed spec must not take down DAG parsing for the sources that are
    fine — a DAG that fails to import does not fail loudly, it just stops being
    scheduled, and nobody notices until the data is days old.
    """
    import pathlib

    import yaml

    directory = pathlib.Path(SOURCES_DIR)
    if not directory.is_dir():
        return []
    found = []
    for path in sorted(directory.glob("*.yml")):
        try:
            document = yaml.safe_load(path.read_text()) or {}
            if not document.get("name"):
                raise ValueError("no `name`")
            found.append(document)
        except Exception as error:  # noqa: BLE001 — one bad spec must not hide the rest
            log.error("skipping %s: %s", path.name, error)
    return found


def _orchestration(document):
    orchestration = document.get("orchestration") or {}
    timeouts = orchestration.get("timeouts_minutes") or {}
    return {
        "schedule": orchestration.get("schedule"),
        "reconcile": orchestration.get("reconcile"),
        "pool": orchestration.get("pool") or f"{document['name']}_pipeline",
        "backfill_start": orchestration.get("backfill_start"),
        "ingest_timeout": int(timeouts.get("ingest", 55)),
        "backfill_timeout": int(timeouts.get("backfill", 720)),
        "reconcile_timeout": int(timeouts.get("reconcile", 1200)),
        "retries": int(orchestration.get("retries", 1)),
    }


def _verify_command(name, fail_on_error=True):
    """`dq run` against the source's raw database.

    Addressed by source rather than by a checkpoint name: the expectations are
    generated from the same spec this DAG was, so there is no second name to keep
    in step with the first.
    """
    flag = "" if fail_on_error else " --no-fail-on-error"
    return f"set -euo pipefail\ndq run --source {name}{flag}\n"


def _record_task():
    return BashOperator(
        task_id="record_ops",
        bash_command=record_ops_command(),
        # Runs whatever happened upstream: a failed run is the one most worth
        # having in ops.pipeline_runs.
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=pendulum.duration(minutes=5),
        retries=0,
    )


def _ingest_dag(document, conf):
    name = document["name"]
    with DAG(
        dag_id=f"{name}_ingest",
        description=f"{name} -> raw_{name} -> quality.",
        schedule=conf["schedule"],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": conf["retries"],
                      "retry_delay": pendulum.duration(minutes=5),
                      "depends_on_past": False},
        tags=[name, "ingest"],
    ) as dag:
        ingest = BashOperator(
            task_id="ingest",
            bash_command=ingest_command(name),
            # A pool of one per source: concurrent runs would race that source's
            # incremental cursor. Sources do not wait for each other.
            pool=conf["pool"],
            execution_timeout=pendulum.duration(minutes=conf["ingest_timeout"]),
        )
        verify_raw = BashOperator(
            task_id="verify_raw",
            bash_command=_verify_command(name),
            execution_timeout=pendulum.duration(minutes=10),
        )
        record = _record_task()

        ingest >> verify_raw >> record
        [verify_raw, record] >> run_verdict()
    return dag


def _backfill_dag(document, conf):
    from airflow.sdk import Param

    name = document["name"]
    with DAG(
        dag_id=f"{name}_backfill",
        description=f"Load a date range of {name} on demand.",
        schedule=None,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        params={
            "start": Param(conf["backfill_start"] or "2020-01-01", type="string"),
            "end": Param("", type="string"),
            "resources": Param("all", type="string"),
        },
        default_args={"retries": conf["retries"],
                      "retry_delay": pendulum.duration(minutes=5),
                      "depends_on_past": False},
        tags=[name, "backfill"],
    ) as dag:
        backfill = BashOperator(
            task_id="backfill",
            bash_command=ingest_command(
                name,
                "--start '{{ params.start }}' "
                "{% if params.end %}--end '{{ params.end }}' {% endif %}"
                "--resources '{{ params.resources }}'",
            ),
            pool=conf["pool"],
            execution_timeout=pendulum.duration(minutes=conf["backfill_timeout"]),
        )
        verify = BashOperator(
            task_id="verify_raw",
            # A partial backfill is expected to fail freshness; the point of the
            # run is the rows, not the verdict.
            bash_command=_verify_command(name, fail_on_error=False),
            execution_timeout=pendulum.duration(minutes=15),
        )
        record = _record_task()

        backfill >> verify >> record
    return dag


def _reconcile_dag(document, conf):
    """Full-history re-fetch that tombstones rows deleted upstream.

    Only built when the spec declares a `reconcile` schedule AND a
    `backfill_start`. The tombstone pass is only safe on a run that covered all
    of history — the floor has to be declared for the guard to compare against.
    """
    name = document["name"]
    with DAG(
        dag_id=f"{name}_reconcile",
        description=f"Full-history re-fetch that tombstones rows deleted in {name}.",
        schedule=conf["reconcile"],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        # No retry: a partial retry after a tombstone pass is the dangerous case.
        default_args={"retries": 0, "depends_on_past": False},
        tags=[name, "reconcile"],
    ) as dag:
        reconcile = BashOperator(
            task_id="reconcile",
            bash_command=ingest_command(
                name, f"--start {conf['backfill_start']} --mark-deleted"
            ),
            pool=conf["pool"],
            execution_timeout=pendulum.duration(minutes=conf["reconcile_timeout"]),
        )
        verify = BashOperator(
            task_id="verify_raw",
            bash_command=_verify_command(name),
            execution_timeout=pendulum.duration(minutes=15),
        )
        record = _record_task()

        reconcile >> verify >> record
    return dag


# Airflow discovers DAGs as module globals, so they are registered by assignment.
for _document in _specs():
    _conf = _orchestration(_document)
    _name = _document["name"]
    globals()[f"{_name}_ingest"] = _ingest_dag(_document, _conf)
    globals()[f"{_name}_backfill"] = _backfill_dag(_document, _conf)
    if _conf["reconcile"] and _conf["backfill_start"]:
        globals()[f"{_name}_reconcile"] = _reconcile_dag(_document, _conf)

if not _specs():
    log.info("no source specs in %s — no ingest DAGs registered", SOURCES_DIR)

sys.modules[__name__].__dict__.pop("_document", None)
