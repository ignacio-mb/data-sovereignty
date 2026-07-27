"""Soft-delete reconciliation: rows absent from the current (complete) fetch
get _deleted = true.

Only valid after a COMPLETE fetch of an entity: the predicate is "row's
_dlt_load_id is not among this run's load ids", so running it after a partial
fetch (an incremental issues run, a partial timeframe window) would tombstone
everything that simply wasn't re-fetched. Callers are responsible for that
guard; see cli.ingest.

On ClickHouse this is an ALTER TABLE ... UPDATE mutation — count-gated so
quiet runs issue no mutation at all, and mutations_sync=1 so failures are loud
rather than stuck asynchronously in system.mutations.
"""

import logging

from dlt.destinations.exceptions import DatabaseUndefinedRelation

log = logging.getLogger(__name__)


def mark_deleted(pipeline, tables, load_ids):
    """Returns {table: rows_marked}. load_ids are this run's dlt load ids."""
    if not load_ids:
        return {}
    in_list = ", ".join(f"'{load_id}'" for load_id in load_ids)  # dlt-generated ids, not user input
    is_clickhouse = pipeline.destination.destination_name == "clickhouse"
    marked = {}
    with pipeline.sql_client() as client:
        for table in tables:
            qualified = client.make_qualified_table_name(table)
            predicate = f"_dlt_load_id NOT IN ({in_list}) AND _deleted = false"
            try:
                [(stale,)] = client.execute_sql(f"SELECT count(*) FROM {qualified} WHERE {predicate}")
            except DatabaseUndefinedRelation:
                # Resource yielded zero rows, so dlt never created the table. Nothing to reconcile.
                log.info("[soft-delete] %s: table does not exist yet — skipped", table)
                marked[table] = 0
                continue
            if stale:
                if is_clickhouse:
                    client.execute_sql(
                        f"ALTER TABLE {qualified} UPDATE _deleted = true WHERE {predicate} "
                        f"SETTINGS mutations_sync = 1"
                    )
                else:
                    client.execute_sql(f"UPDATE {qualified} SET _deleted = true WHERE {predicate}")
                log.info("[soft-delete] %s: marked %d rows deleted", table, stale)
            marked[table] = stale
    return marked
