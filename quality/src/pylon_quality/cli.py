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
        suites = _raw_suites()
    else:
        from .suites.marts import build as build_suites

        suites = build_suites()

    if not suites:
        click.echo(f"{checkpoint}: nothing to validate yet")
        return

    from great_expectations.datasource.fluent.interfaces import TestConnectionError

    context = build_context()
    try:
        gx_checkpoint = build_checkpoint(context, checkpoint, suites)
    except TestConnectionError as exc:
        # Overwhelmingly this is "you have not ingested anything yet", which is
        # a reasonable state to be in and does not deserve a stack trace.
        raise click.ClickException(
            f"{checkpoint}: the tables to validate are not in the warehouse yet.\n"
            f"  {exc}\n"
            + ("Run `make ingest` first."
               if checkpoint == "raw_pylon"
               else "Run `make mb-transforms` first.")
        ) from exc
    log.info("running checkpoint %s over %d asset(s)", checkpoint, len(suites))
    result = gx_checkpoint.run()

    rows = results_module.flatten(checkpoint, result)
    try:
        results_module.write(rows)
    except Exception as exc:  # noqa: BLE001 - a reporting failure must not mask the verdict
        log.error("could not persist validation results: %s", exc)

    _report(checkpoint, rows)

    # GX's own result.success counts every failure equally. The verdict that
    # reaches Airflow must ignore advisory ones, or marking a check advisory
    # would change the report and nothing else.
    fatal = [row for row in rows if not row["success"] and row.get("severity", "error") == "error"]
    if fatal and fail_on_error:
        sys.exit(1)


def _raw_suites():
    """The raw suites, narrowed to the tables that actually landed.

    A resource that has never yielded a row has no table at all, and validating
    the five that exist is worth more than failing all six. The skip is printed
    rather than swallowed: "not checked" and "checked and passed" are different
    answers, and the DAG log should show which one it got.
    """
    from .config import RAW_SCHEMA
    from .context import present_tables
    from .suites.raw_pylon import ENTITY_TABLES, REQUIRED_TABLES, build

    landed = present_tables(RAW_SCHEMA)

    absent_required = [table for table in REQUIRED_TABLES if table not in landed]
    if absent_required:
        raise click.ClickException(
            f"raw_pylon: {', '.join(f'{RAW_SCHEMA}.{t}' for t in absent_required)} "
            f"{'is' if len(absent_required) == 1 else 'are'} not in the warehouse.\n"
            "Run `make ingest` first."
        )

    for table in ENTITY_TABLES:
        if table not in landed:
            click.echo(f"raw_pylon: skipping {RAW_SCHEMA}.{table} — no rows have ever "
                       f"been ingested for it, so dlt never created the table")

    return build(present=landed)


def _report(checkpoint, rows):
    failures = [row for row in rows if not row["success"]]
    click.echo(f"\n{checkpoint}: {len(rows) - len(failures)}/{len(rows)} expectations passed")
    if not failures:
        return

    def line(row):
        # Lead with the author's sentence. "unexpected_rows_expectation
        # observed=1" names the machinery, not the problem.
        column = f" [{row['column_name']}]" if row["column_name"] else ""
        observed = f" observed={row['observed_value']}" if row["observed_value"] else ""
        what = row.get("description") or row["expectation"]
        return f"  {row['asset']}{column}: {what}{observed}"

    for label, severity in (("failed", "error"), ("warnings (not fatal)", "warn")):
        subset = [row for row in failures if row.get("severity", "error") == severity]
        if subset:
            click.echo(f"\n{label}:")
            for row in subset:
                click.echo(line(row))
