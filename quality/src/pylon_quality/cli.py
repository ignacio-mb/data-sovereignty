"""CLI: `dq` — data quality for the Pylon warehouse."""

import json
import logging
import sys

import click
from dotenv import load_dotenv

log = logging.getLogger(__name__)

CHECKPOINTS = {
    "raw_pylon": "suites.raw_pylon",
    "marts": "suites.marts",
}


def _setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.option("--verbose", is_flag=True, help="Debug logging.")
def cli(verbose):
    """Data quality: validate the warehouse, record the verdict in ops."""
    load_dotenv()
    _setup_logging(verbose)


@cli.command("ops-init")
def ops_init():
    """Create the ops schema and its tables. Idempotent."""
    from .ops_schema import init

    for table in init():
        click.echo(f"ready: {table}")


@cli.command("ops-sync")
@click.option("--limit", default=200, show_default=True, help="How many recent runs to pull.")
def ops_sync_command(limit):
    """Mirror Metabase transform run history into ops.mb_transform_runs."""
    from .ops_sync import MbError, sync

    try:
        count = sync(limit=limit)
    except MbError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"synced {count} transform runs")


@cli.command("record-run")
@click.argument("summary_json", type=click.Path(exists=True, dir_okay=False))
@click.option("--status", default="succeeded", show_default=True,
              help="Outcome of the ingest task this summary came from.")
def record_run(summary_json, status):
    """Record a `pylon ingest --summary-json` file into ops.pipeline_runs."""
    from .run_log import record

    with open(summary_json) as handle:
        summary = json.load(handle)
    record(summary, status=status)
    click.echo(f"recorded ingest run ({status})")


@cli.command("docs-build")
def docs_build():
    """Rebuild the data docs from the most recent validation results."""
    from .config import docs_dir

    target = docs_dir()
    if not (target / "index.html").exists():
        raise click.ClickException(
            f"no data docs at {target} — run `dq run` first, docs are written as a checkpoint action"
        )
    click.echo(f"data docs: {target / 'index.html'}")


@cli.command()
@click.option("--checkpoint", type=click.Choice(sorted(CHECKPOINTS)), required=True,
              help="Which layer to validate.")
@click.option("--fail-on-error/--no-fail-on-error", default=True, show_default=True,
              help="Exit non-zero when an expectation fails.")
def run(checkpoint, fail_on_error):
    """Validate a layer of the warehouse and record the results in ops.gx_results."""
    from . import results as results_module
    from .context import build_checkpoint, build_context

    if checkpoint == "raw_pylon":
        from .suites.raw_pylon import build as build_suites
    else:
        from .suites.marts import build as build_suites

    suites = build_suites()
    if not suites:
        click.echo(f"{checkpoint}: nothing to validate yet")
        return

    context = build_context()
    gx_checkpoint = build_checkpoint(context, checkpoint, suites)
    log.info("running checkpoint %s over %d asset(s)", checkpoint, len(suites))
    result = gx_checkpoint.run()

    rows = results_module.flatten(checkpoint, result)
    try:
        results_module.write(rows)
    except Exception as exc:  # noqa: BLE001 - a reporting failure must not mask the verdict
        log.error("could not persist validation results: %s", exc)

    _report(checkpoint, rows)

    if not result.success and fail_on_error:
        sys.exit(1)


def _report(checkpoint, rows):
    failures = [row for row in rows if not row["success"]]
    click.echo(f"\n{checkpoint}: {len(rows) - len(failures)}/{len(rows)} expectations passed")
    if not failures:
        return
    click.echo("\nfailed:")
    for row in failures:
        column = f" [{row['column_name']}]" if row["column_name"] else ""
        observed = f" observed={row['observed_value']}" if row["observed_value"] else ""
        click.echo(f"  {row['asset']}{column} {row['expectation']}{observed}")
