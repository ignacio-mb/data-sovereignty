"""Verify the Metabase instance can actually do what we are about to ask of it.

Runs before any modeling. Designing a transform layer for an instance whose
license does not include transforms is a slow, confusing way to discover the
license — this makes it a fast, obvious one.
"""

import logging
import os
from datetime import UTC, datetime

from . import mb
from .config import (
    MIN_METABASE_VERSION,
    REQUIRED_TOKEN_FEATURES,
    TRANSFORMS_COLLECTION,
    WAREHOUSE_DB_NAME,
    docs_path,
)

log = logging.getLogger(__name__)


class CapabilityError(RuntimeError):
    pass


def _major_version(version_string):
    """Metabase Enterprise reports v1.63.1.6; the product major is the 63."""
    parts = str(version_string).lstrip("v").split(".")
    try:
        first, second = int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        return None
    # EE builds are 1.<major>.x, OSS builds are 0.<major>.x.
    return second if first in (0, 1) else first


def instance_status():
    """Version and license features, read from the live instance.

    Deliberately not `mb auth status`: that reports what was cached on a stored
    profile at login time, and returns nulls when credentials come from MB_URL /
    MB_API_KEY — which is how every container and CI run authenticates.
    `setting get` always asks the server.
    """
    version_setting = mb.run(["setting", "get", "version"]) or {}
    value = version_setting.get("value") or {}
    version = value.get("tag") if isinstance(value, dict) else value

    features_setting = mb.run(["setting", "get", "token-features"]) or {}
    features = features_setting.get("value") or {}
    enabled = (
        {name for name, on in features.items() if on}
        if isinstance(features, dict)
        else set(features)
    )
    return {"version": version, "major": _major_version(version), "features": enabled}


def find_database(name=None):
    """The warehouse connection, matched on physical identity first.

    Enterprise can relabel an attached database in the UI and a user can rename
    it, so host+dbname is the reliable key; the display name is only a fallback
    for a connection someone added by hand. --full is required — the compact
    list projection omits `details` entirely.
    """
    name = name or WAREHOUSE_DB_NAME
    host = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS__HOST", "").strip()
    dbname = os.environ.get("DESTINATION__POSTGRES__CREDENTIALS__DATABASE", "").strip()

    databases = mb.items(mb.run(["db", "list"], full=True))
    if host and dbname:
        for database in databases:
            details = database.get("details") or {}
            if details.get("host") == host and details.get("dbname") == dbname:
                return database
    for database in databases:
        if database.get("name") == name:
            return database
    return None


def ensure_transforms_collection():
    """Transforms only accept collections in the :transforms namespace; a normal
    analytics collection id is rejected outright."""
    existing = mb.items(mb.run(["collection", "list", "--namespace", "transforms"]))
    for collection in existing:
        if collection.get("name") == TRANSFORMS_COLLECTION:
            return collection
    created = mb.run(
        ["collection", "create", "--namespace", "transforms"],
        body={"name": TRANSFORMS_COLLECTION,
              "description": "Transforms built from metabase/transforms/manifest.yml."},
    )
    log.info("created transforms collection %r (id %s)", TRANSFORMS_COLLECTION, created.get("id"))
    return created


def audit(strict=True):
    """Returns a findings dict; raises CapabilityError when strict and unusable."""
    status = instance_status()
    problems = []

    if status["major"] is None:
        problems.append(f"could not parse the Metabase version from {status['version']!r}")
    elif status["major"] < MIN_METABASE_VERSION:
        problems.append(
            f"Metabase {status['version']} is older than the required v{MIN_METABASE_VERSION}"
        )

    missing = sorted(REQUIRED_TOKEN_FEATURES - status["features"])
    if missing:
        problems.append(
            "license is missing required token features: " + ", ".join(missing)
            + " (an Enterprise instance without a token behaves like OSS)"
        )

    database = find_database()
    if database is None:
        problems.append(
            f"no database named {WAREHOUSE_DB_NAME!r} is connected "
            f"(scripts/bootstrap_metabase.sh connects it)"
        )

    collection = None
    if not problems:
        collection = ensure_transforms_collection()

    findings = {
        "checked_at": datetime.now(UTC).isoformat(),
        "version": status["version"],
        "major": status["major"],
        "features": sorted(status["features"]),
        "missing_features": missing,
        "database": database,
        "transforms_collection": collection,
        "problems": problems,
    }

    if problems and strict:
        raise CapabilityError("; ".join(problems))
    return findings


def render(findings):
    """The audit as a reviewable markdown deliverable (docs/10_...)."""
    lines = [
        "# 10 — Instance capabilities",
        "",
        "Generated by `mbx audit`. Regenerate rather than hand-editing.",
        "",
        f"- Checked at: `{findings['checked_at']}`",
        f"- Metabase version: `{findings['version']}` (major {findings['major']})",
        "",
        "## Required license features",
        "",
        "| Feature | Present |",
        "|---|---|",
    ]
    for feature in sorted(REQUIRED_TOKEN_FEATURES):
        present = "yes" if feature not in findings["missing_features"] else "**NO**"
        lines.append(f"| `{feature}` | {present} |")

    database = findings.get("database") or {}
    collection = findings.get("transforms_collection") or {}
    lines += [
        "",
        "## Warehouse connection",
        "",
        f"- Name: `{database.get('name', '—')}`",
        f"- Database id: `{database.get('id', '—')}`",
        f"- Engine: `{database.get('engine', '—')}`",
        "",
        "## Transforms collection",
        "",
        f"- Name: `{collection.get('name', '—')}`",
        f"- Collection id: `{collection.get('id', '—')}` (`:transforms` namespace)",
        "",
        "## All enabled token features",
        "",
        "".join(f"- `{feature}`\n" for feature in findings["features"]) or "- (none reported)\n",
    ]

    if findings["problems"]:
        lines += ["## Problems", ""] + [f"- {problem}" for problem in findings["problems"]] + [""]

    return "\n".join(lines)


def write_report(findings):
    path = docs_path("10_instance_capabilities.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(findings))
    return path
