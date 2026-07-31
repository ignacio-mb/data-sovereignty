"""CLI: `dq` — does what landed in the warehouse match the contract it landed under?

Addressed by source, not by layer. Each connected source declares its own
expectations in `sources/<name>.yml`, and `dq run --source <name>` validates the
`raw_<name>` database against them. There is no enumeration of checkpoints here to
keep in step with the specs, and nothing downstream of raw: what the rows go on to
mean is another project's to validate.
"""

import json
import logging
import sys

import click
from dotenv import load_dotenv

log = logging.getLogger(__name__)


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


# The twin of the ingest CLI's guard, and the reason it is shared rather than
# copied: `dq` reads the same DESTINATION__CLICKHOUSE__CREDENTIALS__HOST, has the
# same `localhost` default, and unlike `ingest` it runs CREATE DATABASE/TABLE and
# INSERTs into ops. For a while `ingest run` refused a loopback warehouse and
# `dq run` next to it did not, which is the worst possible split: the operator
# concludes host-side commands are guarded.
def _refuse_if_loopback(action):
    from ingest_runtime.locality import RemoteWarehouseRefused, refuse_loopback_warehouse

    try:
        refuse_loopback_warehouse(
            action,
            "DS_ALLOW_HOST_DQ",
            "  Inside the stack, where the address is unambiguous:\n"
            "      make quality SOURCE=<name>\n"
            "      docker compose --profile cli run --rm airflow-cli dq <command>\n",
        )
    except RemoteWarehouseRefused as error:
        raise click.ClickException(str(error)) from error


@cli.command("ops-init")
def ops_init():
    """Create the ops schema and its tables. Idempotent."""
    _refuse_if_loopback("a dq ops-init against the production warehouse")
    from .ops_schema import init

    for table in init():
        click.echo(f"ready: {table}")


@cli.command("record-run")
@click.argument("summary_json", type=click.Path(exists=True, dir_okay=False))
@click.option("--status", default="succeeded", show_default=True,
              help="Outcome of the ingest task this summary came from.")
def record_run(summary_json, status):
    """Record an `ingest run --summary-json` file into ops.pipeline_runs."""
    _refuse_if_loopback("a dq record-run against the production warehouse")
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


@cli.command("sources")
def list_sources():
    """List the sources whose contracts can be validated."""
    from ingest_runtime.spec import available, load

    names = available()
    if not names:
        click.echo("no sources connected — add one with the add-source skill, "
                   "which writes sources/<name>.yml")
        return
    for name in names:
        spec = load(name)
        checks = sum(len(expectations) for expectations in _spec_suites(spec).values())
        click.echo(f"{name:16} {len(spec.resources):2} resources   {checks:3} expectations")


@cli.command()
@click.option("--source", "source_name", required=True,
              help="Which source's raw contract to validate. `dq sources` lists them.")
@click.option("--fail-on-error/--no-fail-on-error", default=True, show_default=True,
              help="Exit non-zero when an expectation fails.")
def run(source_name, fail_on_error):
    """Validate a source's raw tables and record the results in ops.gx_results."""
    _refuse_if_loopback("a dq run against the production warehouse")

    from ingest_runtime.spec import SpecError, load

    from . import results as results_module
    from .context import build_checkpoint, build_context

    try:
        spec = load(source_name)
    except SpecError as error:
        raise click.ClickException(str(error)) from error

    # The checkpoint keeps the name the ops tables have always recorded, so a
    # source's history stays one series across this change.
    checkpoint = spec.dataset
    suites = _raw_suites(spec)

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
            f"Run `ingest run --source {spec.name}` first."
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


def _spec_suites(spec):
    """Every expectation the spec declares, assuming all its tables exist.

    Used for reporting the size of a contract. Validation goes through
    _raw_suites, which narrows it to what is actually in the warehouse.
    """
    from .suites.raw import build

    return build(spec)


def _raw_suites(spec):
    """The source's suites, narrowed to the tables that actually landed.

    A resource that has never yielded a row has no table at all, and validating
    the five that exist is worth more than failing all six. The skip is printed
    rather than swallowed: "not checked" and "checked and passed" are different
    answers, and the DAG log should show which one it got.
    """
    from .context import present_tables
    from .suites.raw import build, entity_tables, required_tables

    database = spec.dataset
    landed = present_tables(database)

    absent_required = [table for table in required_tables(spec) if table not in landed]
    if absent_required:
        raise click.ClickException(
            f"{database}: {', '.join(f'{database}.{t}' for t in absent_required)} "
            f"{'is' if len(absent_required) == 1 else 'are'} not in the warehouse.\n"
            f"Run `ingest run --source {spec.name}` first."
        )

    for table in entity_tables(spec):
        if table not in landed:
            click.echo(f"{database}: skipping {database}.{table} — no rows have ever "
                       f"been ingested for it, so dlt never created the table")

    return build(spec, present=landed)


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
