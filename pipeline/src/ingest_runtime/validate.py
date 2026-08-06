"""Checks that answer "is this spec correct?" without running it.

A spec used to be provable only against the live API: you wrote it, ran it, and
found out. That is a slow loop for a human and a useless one for a reviewer, who
has neither the credential nor the appetite to load a warehouse to approve a
pull request.

Two layers do the work. `sources/source.schema.json` covers shape — which keys,
of what type, in what combination — and rejects unknown keys everywhere, which
is what makes a typo an error instead of a silently ignored line. This module
covers everything a schema cannot say, because it needs more than one file to
know it:

  * identity      the directory, the `name`, and the derived database, pool and
                  DAG ids all agree, and no two connectors collide on any of them
  * completeness  a spec declaring an extension has one, and every delegated
                  resource has a builder in it
  * coherence     rate-limit families are routed, freshness names a real table,
                  a connected source is listed in CONNECTED
  * hygiene       schedule minutes are spread out, append is justified, a
                  connected connector ships fixtures

Warnings do not fail. Errors do, and CI runs this before terraform ever reads a
spec — a bad connector should fail in a pull request, not in a plan.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from .spec import CONNECTED, SPEC_FILENAME, SpecError, available, load, sources_dir

CONNECTED_FILE = "CONNECTED"

ERROR = "error"
WARN = "warn"


class Finding:
    __slots__ = ("level", "message", "source")

    def __init__(self, level, source, message):
        self.level = level
        self.source = source
        self.message = message

    def __repr__(self):
        return f"<{self.level} {self.source}: {self.message}>"

    def __str__(self):
        where = f"{self.source}: " if self.source else ""
        return f"{self.level.upper():5} {where}{self.message}"


def connected_file(directory=None):
    base = Path(directory) if directory else sources_dir()
    return base / CONNECTED_FILE


def declared_connected(directory=None):
    """The names in sources/CONNECTED, which is the second, deliberate place.

    One name per line so that two connector pull requests append different lines
    and merge cleanly. The list used to be a Python literal inside a test, where
    every connector rewrote the same line and every pair of them conflicted.
    """
    path = connected_file(directory)
    if not path.is_file():
        return None
    names = []
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            names.append(line)
    return names


def validate_all(directory=None, names=None):
    """Every check, over every spec (or the ones named). Returns [Finding]."""
    base = Path(directory) if directory else sources_dir()
    wanted = list(names or available(base))
    findings = []

    specs = []
    for name in wanted:
        try:
            specs.append(load(name, directory=base))
        except SpecError as error:
            findings.append(Finding(ERROR, name, str(error)))
    if not specs:
        return findings

    for spec in specs:
        findings.extend(_check_one(spec))
    findings.extend(_check_across(specs, base, partial=bool(names)))
    return findings


def _check_one(spec):
    findings = []
    name = spec.name

    # ── the extension is where the spec says it is, and answers for everything
    # it was asked to ────────────────────────────────────────────────────────
    extension_exists = spec.extension_path.is_file()
    if spec.uses_extension and not extension_exists:
        findings.append(Finding(
            ERROR, name,
            f"declares `extensions: true` but {spec.extension_path.name} is missing"))
    if extension_exists and not spec.uses_extension:
        findings.append(Finding(
            ERROR, name,
            f"ships {spec.extension_path.name} but the spec does not declare "
            f"`extensions: true`, so nothing will ever load it"))

    delegated = spec.delegated_resources
    if delegated and not spec.uses_extension:
        names = ", ".join(r.name for r in delegated)
        findings.append(Finding(
            ERROR, name,
            f"{names} use a strategy the declarative layer cannot build, but the "
            f"spec declares no extension"))
    elif delegated and extension_exists:
        findings.extend(_check_builders(spec, delegated))

    # ── the vocabulary is being used, not just declared ──────────────────────
    routed = {family for resource in spec.resources for family in resource.families}
    for family in sorted(set(spec.rate_limits) - routed):
        findings.append(Finding(
            WARN, name,
            f"rate_limits declares family {family!r}, which no resource routes to "
            f"— that budget is never applied"))

    for resource in spec.resources:
        if resource.write_disposition == "append":
            findings.append(Finding(
                WARN, f"{name}.{resource.name}",
                "write_disposition: append cannot dedupe, so a run killed mid-load "
                "duplicates rows permanently. Use merge unless the source has no "
                "stable key — and say why in a comment if so"))
        if resource.strategy == "cursor":
            cursor_field = resource.incremental.get("cursor_field")
            if cursor_field and cursor_field not in resource.hint_columns:
                findings.append(Finding(
                    WARN, f"{name}.{resource.name}",
                    f"cursor {cursor_field!r} is not in hint_columns, so dlt infers "
                    f"its type — a cursor typed as text compares lexicographically"))

    # ── a connected connector is provable offline ────────────────────────────
    if spec.is_connected:
        fixtures = spec.dir / "fixtures"
        required = set(spec.quality.get("required") or ())
        missing = sorted(
            table for table in (required or set(spec.resource_names[:1]))
            if not (fixtures / f"{table}.json").is_file()
        )
        if missing:
            findings.append(Finding(
                WARN, name,
                f"no fixtures for {', '.join(missing)} — the contract suite cannot "
                f"prove this connector offline without them"))

    return findings


def _check_builders(spec, delegated):
    """Every delegated resource has a function that will be found at run time.

    Importing the extension is the only way to know. That is a real import of
    real code, which is why this is a lint rather than part of loading a spec:
    `ingest sources` should not execute a connector's Python.
    """
    from .extensions import builder_for
    from .extensions import load as load_extension

    try:
        extension = load_extension(spec)
    except Exception as error:  # noqa: BLE001 - report, do not propagate
        return [Finding(ERROR, spec.name,
                        f"{spec.extension_path.name} does not import: {error}")]

    findings = []
    for resource in delegated:
        if builder_for(spec, resource, extension) is None:
            findings.append(Finding(
                ERROR, f"{spec.name}.{resource.name}",
                f"strategy {resource.strategy!r} is delegated, but "
                f"{spec.extension_path.name} defines neither build_{resource.name}() "
                f"nor build_resource()"))
    if spec.api["auth"]["type"] == "extension" and not hasattr(extension, "build_auth"):
        findings.append(Finding(
            ERROR, spec.name,
            f"auth type 'extension' needs build_auth(spec) in {spec.extension_path.name}"))
    return findings


def _check_across(specs, directory, partial=False):
    """Collisions and the connected list — the checks that need every spec."""
    findings = []

    for label, key in (("database", lambda s: s.dataset),
                       ("pool", lambda s: s.pool)):
        owners = defaultdict(list)
        for spec in specs:
            owners[key(spec)].append(spec.name)
        for value, names in sorted(owners.items()):
            if len(names) > 1:
                findings.append(Finding(
                    ERROR, ", ".join(sorted(names)),
                    f"share one {label} ({value}) — they would write over each "
                    f"other and share a single soft-delete pass"))

    dag_owners = defaultdict(list)
    for spec in specs:
        for dag_id in spec.dag_ids:
            dag_owners[dag_id].append(spec.name)
    for dag_id, names in sorted(dag_owners.items()):
        if len(names) > 1:
            findings.append(Finding(
                ERROR, ", ".join(sorted(names)), f"generate the same DAG id {dag_id!r}"))

    findings.extend(_check_schedule_spread(specs))

    # The CONNECTED file is only meaningful against the whole directory: a
    # single-source run knows nothing about the sources it did not load.
    if not partial:
        findings.extend(_check_connected_list(specs, directory))
    return findings


def _check_schedule_spread(specs):
    """Two connectors starting on the same minute contend for everything.

    A warning, not an error: at four sources it is noise, and at thirty it is
    the difference between a smooth hour and a thundering herd. The minutes used
    to be coordinated in YAML comments (":23 — clear of :00 and of Pylon's :17"),
    which is a convention nothing enforced.
    """
    minutes = defaultdict(list)
    for spec in specs:
        schedule = spec.schedule
        # `@hourly` and friends have no minute to compare, and a `*` minute means
        # "every minute", which collides with nothing in particular.
        if not schedule or schedule.strip().startswith("@"):
            continue
        parts = schedule.split()
        if len(parts) < 5:
            continue
        minute = parts[0]
        if minute.isdigit():
            minutes[minute].append(spec.name)
    findings = []
    for minute, names in sorted(minutes.items(), key=lambda item: int(item[0])):
        if len(names) > 1:
            findings.append(Finding(
                WARN, ", ".join(sorted(names)),
                f"all start at minute :{minute} — stagger them so one API's rate "
                f"budget and one warehouse do not absorb every connector at once"))
    return findings


def _check_connected_list(specs, directory):
    """`status: connected` and sources/CONNECTED must agree.

    The tripwire, in its scalable form. Its purpose is unchanged: nothing
    schedules by accident, and the set of connectors this checkout runs is
    acknowledged deliberately in a second place. What changed is the shape —
    a sorted file of one name per line instead of a list literal inside a test.
    """
    declared = declared_connected(directory)
    actual = sorted(spec.name for spec in specs if spec.is_connected)
    if declared is None:
        if actual:
            return [Finding(
                ERROR, "", f"sources/{CONNECTED_FILE} is missing, but {', '.join(actual)} "
                f"are marked `status: connected`")]
        return []

    findings = []
    for name in sorted(set(actual) - set(declared)):
        findings.append(Finding(
            ERROR, name,
            f"is `status: connected` but absent from sources/{CONNECTED_FILE}. Every "
            f"connected spec schedules an unpaused DAG and demands its token on every "
            f"clone — add the line if that is intended"))
    for name in sorted(set(declared) - set(actual)):
        findings.append(Finding(
            ERROR, name,
            f"is listed in sources/{CONNECTED_FILE} but is not a connected spec "
            f"(no such directory, or its status is not `connected`)"))
    return findings


def check_manifest(directory=None):
    """Whether sources/manifest.json still matches the specs."""
    from .manifest import build, load_manifest

    base = Path(directory) if directory else sources_dir()
    stored = load_manifest(base)
    if stored is None:
        return [Finding(ERROR, "", "sources/manifest.json is missing — run `ingest manifest`")]
    if stored != build(base):
        return [Finding(
            ERROR, "",
            "sources/manifest.json is stale — run `ingest manifest`. Shell, compose "
            "and terraform read it instead of parsing YAML, so a stale one means a "
            "pool that is never created or a task definition with no credential")]
    return []


def worst_level(findings):
    return ERROR if any(f.level == ERROR for f in findings) else (
        WARN if findings else None)


def spec_paths(directory=None):
    """Every spec file on disk, for tooling that wants paths rather than names."""
    base = Path(directory) if directory else sources_dir()
    return [base / name / SPEC_FILENAME for name in available(base)]


__all__ = [
    "CONNECTED",
    "CONNECTED_FILE",
    "ERROR",
    "WARN",
    "Finding",
    "check_manifest",
    "declared_connected",
    "spec_paths",
    "validate_all",
    "worst_level",
]
