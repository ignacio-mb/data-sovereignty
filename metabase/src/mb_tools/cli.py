"""CLI: `mbx` — build and verify Metabase content from this repo.

Every subcommand is idempotent and takes the repo as the source of truth. Content
is never authored in the Metabase UI: the manifest and SQL files are the record,
and a UI edit is something the next `mbx transforms` will overwrite.
"""

import logging

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
@click.option("--verbose", is_flag=True, help="Debug logging, including every mb invocation.")
def cli(verbose):
    """Metabase modeling and semantic layer, driven from this repo."""
    load_dotenv()
    _setup_logging(verbose)


@cli.command()
@click.option("--strict/--no-strict", default=True, show_default=True,
              help="Exit non-zero when the instance cannot support the build.")
def audit(strict):
    """Verify version and license features, then write docs/10_instance_capabilities.md."""
    from .audit import CapabilityError, write_report
    from .audit import audit as run_audit

    try:
        findings = run_audit(strict=strict)
    except CapabilityError as exc:
        raise click.ClickException(str(exc)) from exc

    path = write_report(findings)
    if findings["missing_features"]:
        click.echo(f"Metabase {findings['version']} — license is MISSING: "
                   f"{', '.join(findings['missing_features'])}")
    else:
        click.echo(f"Metabase {findings['version']} — all required features present")
    for problem in findings["problems"]:
        click.echo(f"  ! {problem}")
    click.echo(f"wrote {path.relative_to(path.parents[1])}")


@cli.command()
@click.option("--only", default=None,
              help="Comma-separated transform names to build instead of the whole manifest.")
@click.option("--dry-run", is_flag=True, help="Report what would change without touching Metabase.")
def transforms(only, dry_run):
    """Create or update every transform in the manifest, run it, and assert its grain."""
    from .mb import MbError
    from .run_transforms import TransformError, build_all

    names = [name.strip() for name in only.split(",")] if only else None
    try:
        build_all(only=names, dry_run=dry_run)
    except (TransformError, MbError) as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command()
def semantics():
    """Create or update the metrics and segments that make up the semantic layer."""
    raise click.ClickException("not implemented yet — see milestone M10")


@cli.command()
def metadata():
    """Apply display names, semantic types and foreign-key wiring to the modeled tables."""
    raise click.ClickException("not implemented yet — see milestone M10")


@cli.command()
def dashboards():
    """Build the Success Engineering and Pipeline Health dashboards."""
    raise click.ClickException("not implemented yet — see milestone M10")


@cli.command()
@click.option("--export/--no-export", "export_now", default=True, show_default=True,
              help="Configure and report only, without pushing content.")
def gitsync(export_now):
    """Export Metabase content to the configured git remote (no-op if unconfigured)."""
    from .gitsync import GitSyncError
    from .gitsync import run as run_gitsync
    from .mb import MbError

    try:
        click.echo(run_gitsync(export_now=export_now))
    except (GitSyncError, MbError) as exc:
        raise click.ClickException(str(exc)) from exc
