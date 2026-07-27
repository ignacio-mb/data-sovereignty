"""Build the transform layer from metabase/transforms/manifest.yml.

Ported from the mkxform.py driver used in the business-model build. The shape
that matters: a local name -> id registry makes the whole run repeatable, and a
grain assertion after every run catches a fan-out join the moment it appears
rather than three layers downstream.

Update in place rather than delete-and-recreate. Recreating mints a new
entity_id, appends a `_2` suffix to the exported YAML filename when a same-named
representation already exists on the remote-sync repo, and leaves noisy history.
"""

import json
import logging

import yaml

from . import mb
from .audit import ensure_transforms_collection, find_database
from .config import (
    ANALYTICS_SCHEMA,
    MANIFEST_PATH,
    SQL_DIR,
    TRANSFORM_TAG,
    state_path,
)

log = logging.getLogger(__name__)

REGISTRY = "transform_registry.json"

# The nine keys `transform update` accepts. Sending anything else (for instance
# by pasting a `transform get` response straight back) leaks server-side fields
# into the update and produces a raw database error.
WRITABLE_KEYS = {
    "name", "description", "source", "target",
    "run_trigger", "tag_ids", "collection_id", "owner_user_id", "owner_email",
}


class TransformError(RuntimeError):
    pass


class NothingToBuild(TransformError):
    """The manifest parses but declares no transforms.

    Its own type because it is the one "failure" that is an expected state
    rather than a fault: modeling is stop-gated until someone works through the
    docs/ deliverables, and until then the hourly DAG has nothing to build.
    """


# Airflow's BashOperator turns this exit code into a skipped task
# (`skip_on_exit_code`), which is what keeps the stop gate from marking every
# scheduled run failed. Still non-zero, so an interactive `make mb-transforms`
# is not mistaken for a successful build.
NOTHING_TO_BUILD_EXIT = 99


def load_manifest(path=None):
    path = path or MANIFEST_PATH
    if not path.exists():
        raise TransformError(
            f"no transform manifest at {path} — nothing to build. "
            f"See docs/ for the modeling stop gates."
        )
    manifest = yaml.safe_load(path.read_text()) or {}
    manifest.setdefault("schema", ANALYTICS_SCHEMA)
    transforms = manifest.get("transforms") or []
    if not transforms:
        raise NothingToBuild(
            f"{path} declares no transforms yet.\n"
            "Modeling is stop-gated on real data: ingest first, then work through "
            "docs/00_source_inventory.md, 01_gap_report.md and 02_assumptions.md "
            "before filling in the manifest. See the model-data skill."
        )

    seen = set()
    for transform in transforms:
        name = transform.get("name")
        if not name:
            raise TransformError(f"a transform in {path} has no name")
        if name in seen:
            raise TransformError(f"duplicate transform name {name!r} in {path}")
        seen.add(name)
        sql_file = SQL_DIR / transform["sql"] if "sql" in transform else None
        if sql_file is None or not sql_file.exists():
            raise TransformError(f"transform {name!r} points at a missing SQL file: {sql_file}")
    return manifest


def read_registry():
    path = state_path(REGISTRY)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def write_registry(registry):
    state_path(REGISTRY).write_text(json.dumps(registry, indent=2, sort_keys=True))


def refresh_registry():
    """Rebuild the name -> id map from the instance. The local file is a cache;
    a recreated Metabase invalidates every id in it."""
    registry = {
        transform["name"]: transform["id"]
        for transform in mb.items(mb.run(["transform", "list"]))
        if transform.get("name") and transform.get("id")
    }
    write_registry(registry)
    return registry


def ensure_tag():
    for tag in mb.items(mb.run(["transform-tag", "list"])):
        if tag.get("name") == TRANSFORM_TAG:
            return tag["id"]
    created = mb.run(["transform-tag", "create"], body={"name": TRANSFORM_TAG})
    return created["id"]


def build_body(transform, manifest, database_id, collection_id, tag_id):
    sql = (SQL_DIR / transform["sql"]).read_text().strip()
    return {
        "name": transform["name"],
        "description": transform.get("description", ""),
        "source": {
            "type": "query",
            "query": {
                "database": database_id,
                "type": "native",
                "native": {"query": sql},
            },
        },
        "target": {
            "type": "table",
            "database": database_id,
            "schema": manifest["schema"],
            "name": transform["name"],
        },
        "collection_id": collection_id,
        "tag_ids": [tag_id],
    }


def _writable(body):
    return {key: value for key, value in body.items() if key in WRITABLE_KEYS}


def create_or_update(name, body, registry):
    transform_id = registry.get(name)
    if transform_id is not None:
        log.info("updating transform %s (id %s)", name, transform_id)
        mb.run(["transform", "update", str(transform_id)], body=_writable(body))
        return transform_id
    log.info("creating transform %s", name)
    created = mb.run(["transform", "create"], body=body)
    transform_id = created["id"]
    registry[name] = transform_id
    return transform_id


def run_transform(transform_id, name):
    result = mb.run(["transform", "run", str(transform_id), "--wait", "--sync"])
    final = (result or {}).get("final") or {}
    status = final.get("status")
    if status and status != "succeeded":
        raise TransformError(f"transform {name} finished {status}: {final.get('message')}")
    return result


def assert_grain(database_id, schema, table, grain):
    """count(*) vs count(distinct grain). Cheap, and it catches fan-out joins
    the moment they appear instead of three layers downstream."""
    if not grain:
        log.warning("%s declares no grain — skipping the uniqueness assertion", table)
        return None

    columns = ", ".join(grain)
    sql = (
        f'SELECT count(*) AS total, count(DISTINCT ({columns})) AS distinct_grain '
        f'FROM "{schema}"."{table}"'
    )
    payload = mb.run(["query"], body={
        "database": database_id,
        "type": "native",
        "native": {"query": sql},
    })
    rows = _query_rows(payload)
    if not rows:
        raise TransformError(f"grain check for {table} returned no rows")
    total, distinct_grain = rows[0][0], rows[0][1]
    if total != distinct_grain:
        raise TransformError(
            f"{table} violates its declared grain ({columns}): "
            f"{total} rows but only {distinct_grain} distinct — a join is fanning out"
        )
    log.info("  grain ok: %s rows, unique on (%s)", total, columns)
    return total


def _query_rows(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    data = payload.get("data")
    if isinstance(data, dict):
        return data.get("rows", [])
    if isinstance(data, list):
        return data
    return payload.get("rows", [])


def build_all(only=None, dry_run=False):
    manifest = load_manifest()
    transforms = manifest["transforms"]
    if only:
        wanted = set(only)
        unknown = wanted - {t["name"] for t in transforms}
        if unknown:
            raise TransformError(f"unknown transform(s): {', '.join(sorted(unknown))}")
        transforms = [t for t in transforms if t["name"] in wanted]

    if dry_run:
        for transform in transforms:
            print(f"would build {manifest['schema']}.{transform['name']} "
                  f"from {transform['sql']} (grain: {', '.join(transform.get('grain') or []) or 'none'})")
        return

    database = find_database()
    if database is None:
        raise TransformError("the warehouse is not connected in Metabase — run `mbx audit`")
    database_id = database["id"]
    collection_id = ensure_transforms_collection()["id"]
    tag_id = ensure_tag()

    # Reconcile the cache with reality first: a rebuilt Metabase invalidates
    # every id, and a stale registry would create duplicates instead of updating.
    registry = refresh_registry()

    built = []
    try:
        # Manifest order is dependency order: base_ before dim_ before fact_.
        for transform in transforms:
            name = transform["name"]
            log.info("── %s", name)
            body = build_body(transform, manifest, database_id, collection_id, tag_id)
            transform_id = create_or_update(name, body, registry)
            run_transform(transform_id, name)
            assert_grain(database_id, manifest["schema"], name, transform.get("grain"))
            built.append(name)
    finally:
        write_registry(registry)

    # Metabase does not always notice new columns on its own; the metadata and
    # semantic steps need the field catalog to be current.
    mb.run(["db", "sync-schema", str(database_id), "--wait"], parse=False, check=False)
    log.info("built %d transform(s): %s", len(built), ", ".join(built))
    return built
