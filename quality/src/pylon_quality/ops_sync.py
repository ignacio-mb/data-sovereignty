"""Mirror Metabase's transform run history into ops.mb_transform_runs.

Read through the `mb` CLI rather than the Metabase application database: the app
db schema is internal and changes between releases, while the CLI's output is a
declared, versioned contract.
"""

import json
import logging
import subprocess

from .config import OPS_SCHEMA

log = logging.getLogger(__name__)

# ClickHouse has no ON CONFLICT. mb_transform_runs is a ReplacingMergeTree keyed
# on run_id with synced_at as the version, so re-inserting a run that already
# exists is the upsert: the newest row wins once the parts merge, and readers
# that cannot wait for a merge use FINAL.
COLUMNS = (
    "run_id", "transform_id", "transform_name", "status",
    "started_at", "ended_at", "message",
)


class MbError(RuntimeError):
    pass


def fetch_runs(limit=200):
    """`mb transform runs` as a list of dicts."""
    command = ["mb", "transform", "runs", "--json", "--max-bytes", "0", "--limit", str(limit)]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        raise MbError(
            f"`{' '.join(command)}` exited {completed.returncode}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    payload = json.loads(completed.stdout or "{}")
    # mb list commands answer with {returned, total, limit, truncated, data}.
    return payload.get("data", payload if isinstance(payload, list) else [])


def _row(run):
    def pick(*names):
        for name in names:
            if run.get(name) is not None:
                return run[name]
        return None

    transform = run.get("transform") or {}
    return {
        "run_id": pick("id", "run_id"),
        "transform_id": pick("transform_id") or transform.get("id"),
        "transform_name": transform.get("name") or pick("transform_name"),
        "status": pick("status"),
        "started_at": pick("start_time", "started_at"),
        "ended_at": pick("end_time", "ended_at"),
        "message": pick("message"),
    }


def sync(limit=200):
    rows = [_row(run) for run in fetch_runs(limit)]
    rows = [row for row in rows if row["run_id"] is not None]
    if not rows:
        log.info("no transform runs reported by Metabase")
        return 0

    from .context import clickhouse_client
    from .run_log import _as_datetime

    for row in rows:
        row["started_at"] = _as_datetime(row["started_at"])
        row["ended_at"] = _as_datetime(row["ended_at"])

    client = clickhouse_client(database=OPS_SCHEMA)
    try:
        client.insert(
            "mb_transform_runs",
            [[row[column] for column in COLUMNS] for row in rows],
            column_names=list(COLUMNS),
        )
    finally:
        client.close()
    log.info("synced %d transform runs into %s.mb_transform_runs", len(rows), OPS_SCHEMA)
    return len(rows)
