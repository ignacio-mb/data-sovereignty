"""CLI: `ingest run --source <name>` — a source spec into the warehouse, via dlt.

Nothing here knows about any particular API. The spec says what to fetch and how;
this file owns what is the same for every source: draining a crashed run before
extracting, ordering the fetch so a warehouse-derived worklist sees fresh parents,
deciding when absence is allowed to mean deletion, and recording what happened.
"""

import json
import logging
import os
import time
from collections import Counter
from pathlib import Path

import click
import pendulum
from dotenv import load_dotenv

from .observability import attach_samplers, print_run_summary, report_schema_changes, setup_logging
from .spec import SpecError, available, load

log = logging.getLogger(__name__)

PRODUCTION_DESTINATION = "clickhouse"

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
    """Ingestion runtime. `run` loads a source; `sources` lists what is connected."""
    load_dotenv()
    for key, value in RUNTIME_DEFAULTS.items():
        os.environ.setdefault(key, value)


@cli.command("sources")
def list_sources():
    """List the connected sources."""
    names = available()
    if not names:
        click.echo("no sources connected — add one with the add-source skill, "
                   "which writes sources/<name>.yml")
        return
    for name in names:
        spec = load(name)
        schedule = spec.orchestration.get("schedule") or "manual only"
        click.echo(f"{name:16} {len(spec.resources):2} resources   schedule: {schedule}")


@cli.command("run")
@click.option("--source", "source_name", required=True,
              help="Which source to load. `ingest sources` lists them.")
@click.option("--start", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]), default=None,
              help="Window mode: fetch records from this UTC time. Implies --mode window.")
@click.option("--end", type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]), default=None,
              help="Window mode: fetch up to this UTC time (default: now).")
@click.option("--resources", "resources_csv", default="all", show_default=True,
              help="Comma-separated subset of the spec's resources, or 'all'.")
@click.option("--mode", type=click.Choice(["window", "incremental"]), default=None,
              help="Default: window when --start is given, else incremental.")
@click.option("--mark-deleted", "mark_deleted_flag", is_flag=True,
              help="After load, flag rows absent from this run's complete fetches as _deleted.")
@click.option("--sample", "sample_n", type=int, default=0,
              help="Pretty-print the first N records per resource as they will land.")
@click.option("--destination", default=PRODUCTION_DESTINATION,
              type=click.Choice([PRODUCTION_DESTINATION, "duckdb"]),
              help="duckdb is a local smoke destination and cannot touch production state.")
@click.option("--summary-json", type=click.Path(dir_okay=False, writable=True), default=None,
              help="Write the run summary as JSON here (consumed by the Airflow ops task).")
@click.option("--verbose", is_flag=True, help="Debug logging.")
def run(source_name, start, end, resources_csv, mode, mark_deleted_flag, sample_n,
        destination, summary_json, verbose):
    """Ingest a source into the warehouse (database raw_<source>)."""
    from .ingest.soft_delete import mark_deleted
    from .runtime import build_source, extensions, pacer
    from .warehouse import build_pipeline, ensure_database, table_counts

    setup_logging(verbose)

    try:
        spec = load(source_name)
    except SpecError as error:
        raise click.ClickException(str(error)) from error

    if resources_csv.strip() == "all":
        selected = list(spec.resource_names)
    else:
        selected = [item.strip() for item in resources_csv.split(",") if item.strip()]
        unknown = sorted(set(selected) - set(spec.resource_names))
        if unknown:
            raise click.BadParameter(
                f"{spec.name} has no resource(s) {', '.join(unknown)} "
                f"(declared: {', '.join(spec.resource_names)})")

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
        # Whether the window reaches (roughly) now — evaluated BEFORE the load, so
        # a multi-hour reconcile still qualifies for the tombstone pass.
        end_is_current = end >= run_started_at.subtract(hours=1)
    else:
        if start or end:
            raise click.BadParameter("--start/--end only apply to window mode")
        end_is_current = False

    log.info("%s: %s mode · resources: %s · destination: %s",
             spec.name, mode, ", ".join(selected), destination)

    # The database must exist before dlt connects: ClickHouse selects it while
    # connecting, so an absent one fails the pre-run sync rather than being
    # created on demand.
    ensure_database(spec.name, destination)
    pipeline = build_pipeline(spec.name, destination=destination)
    paced = pacer(spec)

    # A previous run may have crashed after normalize but before load, leaving a
    # pending package. dlt's run(source) would load THAT package and return
    # without extracting — silently skipping this run's fetch and, with
    # --mark-deleted, tombstoning everything absent from the stale package.
    # Drain it first; its load id must NOT count toward this run's soft-delete.
    if pipeline.has_pending_data:
        log.warning("[recovery] loading pending package(s) from a previous crashed run "
                    "before extracting")
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
        norm = pipeline.last_trace.last_normalize_info
        if norm is not None:
            for table, count in (norm.row_counts or {}).items():
                rows_this_run[table] += count

    # Run in sequence rather than as one merged source: a resource whose worklist
    # is a warehouse query must extract AFTER its parents are loaded, or it lags a
    # run behind. build_source returns the declarative resources first for exactly
    # that reason.
    for source in build_source(spec, selected=selected, extension=extensions(spec)):
        run_phase(source)

    if mark_deleted_flag:
        eligible = [name for name in selected if name in spec.soft_delete_tables]
        # A `full_history` resource qualifies only when this run observed all of
        # history. Otherwise "absent from this load" means "not re-fetched", not
        # "deleted upstream", and tombstoning on that basis wipes the warehouse.
        floor = spec.backfill_start
        covered_full_history = (
            mode == "window" and floor is not None
            and start <= pendulum.parse(floor) and end_is_current
        )
        for name in spec.full_history_soft_delete_tables:
            if name not in selected:
                continue
            if covered_full_history:
                eligible.append(name)
            else:
                log.info("[soft-delete] %s skipped: run did not cover the full history "
                         "(need --start <= %s and --end ~now)", name, floor)
        if eligible:
            load_ids = [load_id for info in load_infos for load_id in info.loads_ids]
            mark_deleted(pipeline, eligible, load_ids)

    report_schema_changes(load_infos)

    summary = {
        "rows_this_run": dict(rows_this_run),
        "warehouse_counts": table_counts(pipeline, selected),
        "load_ids": [load_id for info in load_infos for load_id in info.loads_ids],
        "requests_by_family": dict(paced.requests_made),
        "elapsed_seconds": time.monotonic() - started,
    }
    print_run_summary(**summary)

    if summary_json:
        payload = {
            "source": spec.name,
            "status": "succeeded",
            "started_at": run_started_at.isoformat(),
            "mode": mode,
            "destination": destination,
            "resources": selected,
            **summary,
        }
        Path(summary_json).write_text(json.dumps(payload, default=str, indent=2))
        log.info("wrote run summary to %s", summary_json)
