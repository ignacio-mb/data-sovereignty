"""Great Expectations context wiring.

The context is ephemeral: suites, validation definitions and checkpoints are all
declared in code and rebuilt on every run. There is no gx/ store to drift out of
sync with the repo, and nothing to migrate when GX upgrades — the durable record
of what happened lives in ops.gx_results and the rendered data docs.
"""

import logging

import great_expectations as gx
import psycopg

from .config import connection_string, docs_dir, psycopg_dsn

log = logging.getLogger(__name__)

DATASOURCE = "warehouse"


def build_context():
    return gx.get_context(mode="ephemeral")


def present_tables(schema, dsn=None):
    """The table names that actually exist in `schema`.

    Asked before the checkpoint is assembled, because GX test-connects every
    asset while building it: one absent table raises TestConnectionError and
    takes the whole checkpoint down with it, including the tables that were fine.
    """
    with psycopg.connect(dsn or psycopg_dsn()) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (schema,),
        ).fetchall()
    return {row[0] for row in rows}


def table_batch_definition(context, schema, table):
    """A whole-table batch definition for schema.table, creating the datasource
    and asset on first use."""
    try:
        datasource = context.data_sources.get(DATASOURCE)
    except (KeyError, ValueError):
        datasource = context.data_sources.add_postgres(
            DATASOURCE, connection_string=connection_string()
        )

    asset_name = f"{schema}.{table}"
    try:
        asset = datasource.get_asset(asset_name)
    except (KeyError, LookupError):
        asset = datasource.add_table_asset(
            name=asset_name, table_name=table, schema_name=schema
        )

    batch_name = "whole_table"
    try:
        return asset.get_batch_definition(batch_name)
    except (KeyError, LookupError):
        return asset.add_batch_definition_whole_table(batch_name)


def build_checkpoint(context, name, expectations_by_asset):
    """expectations_by_asset: {(schema, table): [Expectation]} -> a runnable Checkpoint.

    Suites are registered with the context before their expectations are added:
    GX resolves each expectation against the context's expectation store, and
    an unregistered suite raises DataContextRequiredError.
    """
    validation_definitions = []
    for (schema, table), expectations in expectations_by_asset.items():
        registered = context.suites.add(gx.ExpectationSuite(name=f"{schema}.{table}"))
        for expectation in expectations:
            registered.add_expectation(expectation)
        batch_definition = table_batch_definition(context, schema, table)
        validation_definitions.append(
            context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"{name}::{schema}.{table}",
                    data=batch_definition,
                    suite=registered,
                )
            )
        )

    target = docs_dir()
    target.mkdir(parents=True, exist_ok=True)
    context.add_data_docs_site(
        site_name=name,
        site_config={
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(target),
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    )

    return context.checkpoints.add(
        gx.Checkpoint(
            name=name,
            validation_definitions=validation_definitions,
            actions=[gx.checkpoint.UpdateDataDocsAction(name=f"{name}-docs", site_names=[name])],
            result_format="SUMMARY",
        )
    )
