"""Console visibility: logging setup, sample records, schema-change report,
end-of-run summary."""

import json
import logging

log = logging.getLogger(__name__)


def setup_logging(verbose=False):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        for noisy in ("urllib3", "charset_normalizer"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def make_sampler(resource_name, n):
    """A dlt add_map transform that pretty-prints the first n records exactly
    as they will land in the warehouse (post-flatten), then passes them through."""
    state = {"printed": 0}

    def sample(record):
        if state["printed"] < n:
            state["printed"] += 1
            print(f"\n── sample [{resource_name}] #{state['printed']} " + "─" * 48)
            print(json.dumps(record, indent=2, default=str))
        return record

    return sample


def attach_samplers(source, n):
    if n:
        for name, resource in source.resources.items():
            resource.add_map(make_sampler(name, n))


def schema_change_lines(load_info):
    """One line per table whose schema was created or evolved in this load."""
    lines = []
    for package in load_info.load_packages:
        for table_name, table in (package.schema_update or {}).items():
            columns = table.get("columns", {})
            described = ", ".join(
                f"{name} {spec.get('data_type', '?')}" for name, spec in columns.items()
            )
            lines.append(f"{table_name}: {described}")
    return lines


def report_schema_changes(load_infos):
    lines = [line for info in load_infos for line in schema_change_lines(info)]
    if lines:
        log.warning("schema changes in this run (new tables or columns):")
        for line in lines:
            log.warning("  [schema-change] %s", line)
    else:
        log.info("no schema changes in this run")


def print_run_summary(rows_this_run, warehouse_counts, load_ids, requests_by_family, elapsed_seconds):
    """rows_this_run: {table: rows normalized this run};
    warehouse_counts: {table: total rows in destination (or None)}."""
    tables = sorted(set(rows_this_run) | set(warehouse_counts))
    tables = [t for t in tables if not t.startswith("_dlt")]

    print("\n" + "=" * 64)
    print("RUN SUMMARY")
    print("=" * 64)
    print(f"{'table':<20} {'rows this run':>15} {'total in destination':>22}")
    for table in tables:
        this_run = rows_this_run.get(table, 0)
        total = warehouse_counts.get(table)
        total_text = f"{total:,}" if total is not None else "(missing)"
        print(f"{table:<20} {this_run:>15,} {total_text:>22}")
    print("-" * 64)
    requests_text = ", ".join(f"{family}: {count}" for family, count in sorted(requests_by_family.items()))
    print(f"api requests      {requests_text or 'none'}")
    print(f"load ids          {', '.join(load_ids) or 'none'}")
    print(f"duration          {elapsed_seconds:.1f}s")
    print("=" * 64)
