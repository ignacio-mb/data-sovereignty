"""CLI: `pylon ingest` — Pylon API -> Postgres (dlt)."""

import json
import logging
import os
import time
from collections import Counter
from pathlib import Path

import click
import pendulum
from dotenv import load_dotenv

from .ingest.settings import (
    BACKFILL_START,
    MESSAGES_BUDGET_MINUTES_DEFAULT,
    PRODUCTION_DESTINATION,
    SOFT_DELETE_DIRECTORY_TABLES,
)
from .observability import attach_samplers, print_run_summary, report_schema_changes, setup_logging

log = logging.getLogger(__name__)

# Applied as env-var defaults so runs behave the same from any cwd (the values
# mirror .dlt/config.toml, which dlt only reads when run from the project root).
RUNTIME_DEFAULTS = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    "RUNTIME__LOG_LEVEL": "WARNING",
    "RUNTIME__REQUEST_TIMEOUT": "30",
    "RUNTIME__REQUEST_MAX_ATTEMPTS": "10",
    "RUNTIME__REQUEST_BACKOFF_FACTOR": "2",
    "RUNTIME__REQUEST_MAX_RETRY_DELAY": "300",
}


@click.group()
def cli():
    """Pylon pipeline: `ingest` (Pylon -> Postgres)."""
    load_dotenv()
    for key, value in RUNTIME_DEFAULTS.items():
        os.environ.setdefault(key, value)


@cli.command()
@click.option("--api-key", envvar="PYLON_API_KEY", required=True,
              help="Pylon API key (env: PYLON_API_KEY).")
@click.option("--start", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]), default=None,
              help="Window mode: fetch issues CREATED at/after this UTC time.")
@click.option("--end", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]), default=None,
              help="Window mode: fetch issues created before this UTC time (default: now).")
@click.option("--resources", "resources_csv", default="all", show_default=True,
              help="Comma-separated subset of: issues,issue_messages,accounts,users,teams,contacts — or 'all'.")
@click.option("--mode", type=click.Choice(["window", "incremental"]), default=None,
              help="Default: window when --start is given, else incremental (updated_at cursor).")
@click.option("--mark-deleted", "mark_deleted_flag", is_flag=True,
              help="After load, flag rows absent from this run's complete fetches as _deleted.")
@click.option("--sample", "sample_n", type=int, default=0,
              help="Pretty-print the first N records per resource as they will land in the warehouse.")
@click.option("--budget-minutes", type=int, default=None,
              help=f"Wall-clock budget for the per-issue messages fetch "
                   f"(default: {MESSAGES_BUDGET_MINUTES_DEFAULT} in incremental mode, unlimited in window mode).")
@click.option("--destination", default=PRODUCTION_DESTINATION,
              type=click.Choice([PRODUCTION_DESTINATION, "duckdb"]),
              help="duckdb is a local smoke-test destination; postgres is the real one.")
@click.option("--summary-json", type=click.Path(dir_okay=False, writable=True), default=None,
              help="Write the run summary as JSON to this path (consumed by the Airflow ops task).")
@click.option("--verbose", is_flag=True, help="Debug logging (per-message-fetch detail).")
def ingest(api_key, start, end, resources_csv, mode, mark_deleted_flag, sample_n,
           budget_minutes, destination, summary_json, verbose):
    """Ingest Pylon data into the warehouse (schema raw_pylon)."""
    from .ingest.client import PylonClient
    from .ingest.soft_delete import mark_deleted
    from .ingest.source import ALL_RESOURCES, pylon_source
    from .warehouse import build_pipeline, pending_message_issue_ids, table_counts

    setup_logging(verbose)

    if resources_csv.strip() == "all":
        selected = list(ALL_RESOURCES)
    else:
        selected = [item.strip() for item in resources_csv.split(",") if item.strip()]
        unknown = set(selected) - set(ALL_RESOURCES)
        if unknown:
            raise click.BadParameter(f"unknown resources: {', '.join(sorted(unknown))} "
                                     f"(valid: {', '.join(ALL_RESOURCES)})")

    run_started_at = pendulum.now("UTC")
    mode = mode or ("window" if start else "incremental")
    if mode == "window":
        if start is None:
            raise click.BadParameter("--mode window requires --start")
        start = pendulum.instance(start, tz="UTC")
        end = pendulum.instance(end, tz="UTC") if end else run_started_at
        if end <= start:
            raise click.BadParameter(
                f"--end ({end.isoformat()}) must be after --start ({start.isoformat()})")
        # Whether the window reaches (roughly) up to now — evaluated here, not
        # after the multi-hour load, so a long full-history reconcile still
        # qualifies for the issues soft-delete pass.
        end_is_current = end >= run_started_at.subtract(hours=1)
        log.info("ingest: WINDOW mode %s → %s (issues filtered on created_at — API constraint)",
                 start.isoformat(), end.isoformat())
    else:
        if start or end:
            raise click.BadParameter("--start/--end only apply to window mode")
        end_is_current = False
        log.info("ingest: INCREMENTAL mode (issues updated since the stored cursor)")

    log.info("resources: %s · destination: %s", ", ".join(selected), destination)

    pipeline = build_pipeline(destination=destination)
    client = PylonClient(api_key)

    # A previous run may have crashed after normalize but before load, leaving a
    # pending load package. dlt's run(source) would load THAT package and exit
    # without extracting our source — silently skipping this run's fetch and,
    # with --mark-deleted, tombstoning everything not in the stale package.
    # Drain it first; its load id must NOT count toward this run's soft-delete.
    if pipeline.has_pending_data:
        log.warning("[recovery] loading pending package(s) from a previous crashed run before extracting")
        recovery = pipeline.run()
        if recovery is not None:
            recovery.raise_on_failed_jobs()

    started = time.monotonic()
    load_infos = []
    rows_this_run = Counter()

    def run_phase(source):
        attach_samplers(source, sample_n)
        info = pipeline.run(source)
        info.raise_on_failed_jobs()
        load_infos.append(info)
        norm_info = pipeline.last_trace.last_normalize_info
        if norm_info is not None:
            for table, count in (norm_info.row_counts or {}).items():
                rows_this_run[table] += count

    # Phase 1: issues + directory. Phase 2: messages — separate so the messages
    # worklist (computed from the warehouse at extract time) sees the issues
    # loaded moments ago instead of lagging one run behind.
    phase1 = [name for name in selected if name != "issue_messages"]
    if phase1:
        run_phase(pylon_source(client=client, selected=phase1, mode=mode, start=start, end=end))

    if "issue_messages" in selected:
        effective_budget = budget_minutes
        if effective_budget is None and mode == "incremental":
            effective_budget = MESSAGES_BUDGET_MINUTES_DEFAULT
        run_phase(pylon_source(
            client=client,
            selected=("issue_messages",),
            pending_message_ids=lambda: pending_message_issue_ids(pipeline),
            budget_minutes=effective_budget,
        ))

    if mark_deleted_flag:
        eligible = [name for name in phase1 if name in SOFT_DELETE_DIRECTORY_TABLES]
        # issues only when this run observed the FULL history — otherwise
        # "absent from this load" just means "not re-fetched", not "deleted".
        covered_full_history = (
            mode == "window"
            and "issues" in phase1
            and start <= pendulum.parse(BACKFILL_START)
            and end_is_current
        )
        if covered_full_history:
            eligible.append("issues")
        elif "issues" in phase1:
            log.info("[soft-delete] issues skipped: run did not cover the full history "
                     "(need --start <= %s and --end ~now)", BACKFILL_START)
        if eligible:
            load_ids = [load_id for info in load_infos for load_id in info.loads_ids]
            mark_deleted(pipeline, eligible, load_ids)

    report_schema_changes(load_infos)

    summary = {
        "rows_this_run": dict(rows_this_run),
        "warehouse_counts": table_counts(pipeline, selected),
        "load_ids": [load_id for info in load_infos for load_id in info.loads_ids],
        "requests_by_family": dict(client.pacer.requests_made),
        "elapsed_seconds": time.monotonic() - started,
    }
    print_run_summary(**summary)

    if summary_json:
        summary_path = Path(summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps({
            **summary,
            "mode": mode,
            "destination": destination,
            "resources": selected,
            "started_at": run_started_at.isoformat(),
        }, indent=2, default=str))
        log.info("run summary written to %s", summary_path)
