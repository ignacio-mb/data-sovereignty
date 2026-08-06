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
    """List the connectors on disk and what each one schedules."""
    names = available()
    if not names:
        click.echo("no sources here — add one with the add-source skill, which "
                   "writes sources/<name>/source.yml")
        return
    for name in names:
        spec = load(name)
        schedule = spec.schedule or "manual only"
        extension = " +extension" if spec.uses_extension else ""
        click.echo(f"{name:16} {spec.status:10} {len(spec.resources):2} resources   "
                   f"schedule: {schedule}{extension}")


@cli.command("validate")
@click.option("--source", "source_name", default=None,
              help="Validate one connector instead of all of them.")
@click.option("--check-manifest/--no-check-manifest", default=True, show_default=True,
              help="Also assert sources/manifest.json is current.")
@click.option("--strict", is_flag=True, help="Treat warnings as failures.")
def validate_command(source_name, check_manifest, strict):
    """Check every spec without running it: shape, identity, completeness, hygiene.

    This is what makes a connector reviewable. Before it, the load-bearing facts
    — does `fields` reach the wire, is the composite key really unique, does the
    extension supply what the spec delegated — were provable only by running the
    thing against a live API with a real credential.
    """
    from .validate import ERROR, WARN, validate_all
    from .validate import check_manifest as manifest_findings

    names = [source_name] if source_name else None
    findings = validate_all(names=names)
    if check_manifest and not source_name:
        findings.extend(manifest_findings())

    for finding in findings:
        click.echo(str(finding), err=finding.level == ERROR)

    errors = [f for f in findings if f.level == ERROR]
    warnings = [f for f in findings if f.level == WARN]
    checked = ", ".join(names) if names else f"{len(available())} connector(s)"
    if not findings:
        click.echo(f"{checked}: clean")
    if errors or (strict and warnings):
        raise SystemExit(1)


@cli.command("manifest")
@click.option("--check", is_flag=True,
              help="Fail if the committed manifest is stale instead of rewriting it.")
def manifest_command(check):
    """Regenerate sources/manifest.json — the enumeration shell and terraform read."""
    from .manifest import build, load_manifest, manifest_path, write

    if check:
        if load_manifest() == build():
            click.echo(f"{manifest_path().name} is current")
            return
        raise SystemExit(
            f"{manifest_path()} is stale. Run `ingest manifest` and commit the result."
        )
    path, changed = write()
    click.echo(f"{'wrote' if changed else 'unchanged'} {path}")


@cli.command("inventory")
@click.option("--write/--stdout", "write_file", default=True, show_default=True,
              help="Write docs/sources.md, or print the table.")
def inventory_command(write_file):
    """Regenerate the connector inventory in docs/sources.md.

    The facts about which connectors exist used to be restated in six prose
    files — CLAUDE.md twice, two READMEs, the Makefile and a skill — and two of
    them were already stale at two connectors. Prose keeps the mechanism;
    generation keeps the facts.
    """
    from .inventory import render
    from .inventory import write as write_inventory

    if not write_file:
        click.echo(render())
        return
    path, changed = write_inventory()
    click.echo(f"{'wrote' if changed else 'unchanged'} {path}")


@cli.command("scaffold")
@click.argument("name")
def scaffold_command(name):
    """Create sources/<name>/ from a template, as `status: reference`.

    Reference, never connected: a new connector should not begin its life
    scheduling an unpaused DAG that demands a credential nobody has pushed. The
    flip to `connected` — plus the line in sources/CONNECTED — is the deliberate
    last step, made once the thing has been proven to load.
    """
    from .scaffold import create

    created = create(name)
    click.echo(f"created {created['dir']}")
    for path in created["files"]:
        click.echo(f"  {path}")
    click.echo(
        "\nNext: fill in the endpoints, then\n"
        f"  ingest validate --source {name}\n"
        f"  ingest run --source {name} --destination duckdb --sample 3\n"
        "and only then set `status: connected` and add the name to sources/CONNECTED."
    )


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
    from .runtime import _DECLARATIVE_STRATEGIES, build_source, extensions, pacer
    from .warehouse import build_pipeline, ensure_database, table_counts

    setup_logging(verbose)

    if destination == PRODUCTION_DESTINATION:
        from .locality import RemoteWarehouseRefused, refuse_loopback_warehouse
        try:
            refuse_loopback_warehouse(
                "a production-destination ingest run",
                "DS_ALLOW_HOST_INGEST",
                "  Through Airflow, which serialises on the source's pool:\n"
                "      make ingest SOURCE=<name>\n"
                "  Or inside the stack, where the address is unambiguous:\n"
                "      docker compose --profile cli run --rm airflow-cli ingest run --source <name>\n"
                "  To rehearse safely on this machine:\n"
                "      ingest run --source <name> --destination duckdb --sample 3\n",
            )
        except RemoteWarehouseRefused as error:
            raise click.ClickException(str(error)) from error

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

    if mode == "window":
        # Say it once, up front. `make backfill` on a delegated resource is not a
        # backfill: build_source has no window parameter, so the extension sees
        # only its own cursor. Silence here is what makes a green DAG look like
        # loaded history.
        unbounded = [name for name in selected
                     if spec.resource(name).strategy not in _DECLARATIVE_STRATEGIES]
        if unbounded:
            log.warning("[window] --start/--end do not reach these resources, which are "
                        "fetched by the extension and bound themselves by their own "
                        "cursor: %s. They will fetch what an incremental run fetches, "
                        "not the requested range.", ", ".join(unbounded))

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
    for source in build_source(spec, selected=selected, extension=extensions(spec), paced=paced):
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
            # `covered_full_history` is computed from the FLAGS, not from what was
            # actually fetched — and --start/--end never reach a delegated
            # resource. build_source takes no window, so an extension bounds
            # itself solely by its own persisted cursor: a "backfill" of such a
            # resource fetches the same slice an ordinary incremental run does.
            # Trusting the flags there would tombstone every row the cursor
            # happened not to re-fetch, and max_deleted_fraction only notices
            # after the rows have already been marked.
            if spec.resource(name).strategy not in _DECLARATIVE_STRATEGIES:
                log.warning(
                    "[soft-delete] %s skipped: strategy %r is fetched by the extension, "
                    "which the --start/--end window does not reach, so this run cannot "
                    "have covered full history whatever the flags say. Tombstoning it "
                    "would delete rows that were simply not re-fetched.",
                    name, spec.resource(name).strategy)
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
