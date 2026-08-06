"""The source contract: `sources/<name>/source.yml` loaded and validated.

One directory defines a connector. `source.yml` is the whole contract — what to
call, how to page it, what is incremental, when it runs, and what "correct" means
for the tables it lands. Beside it sit the things that contract cannot hold: the
`extension.py` for fetch behaviour no vocabulary can express, the fixtures that
prove it offline, the research that explains why it is shaped the way it is.
Nothing about any particular API is compiled into the runtime.

Validation is strict about unknown keys on purpose. A misspelled
`time_stamp_columns` that is silently ignored produces a connector that runs,
looks healthy, and lands untyped strings — the class of failure this stack exists
to catch. An unknown key is a typo until proven otherwise, so it raises.

The shape itself lives in `sources/source.schema.json` rather than in this file:
one description of a spec, read by this loader, by `ingest validate`, by an
editor's YAML language server, and by an agent writing its first connector.
"""

from __future__ import annotations

import functools
import json
import os
from pathlib import Path

import yaml

# sources/ sits beside pipeline/, at the repo root: this file is
# pipeline/src/ingest_runtime/spec.py, so three parents up is `pipeline/`.
REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCES_DIR_ENV = "DS_SOURCES_DIR"

SPEC_FILENAME = "source.yml"
EXTENSION_FILENAME = "extension.py"
SCHEMA_FILENAME = "source.schema.json"

# Which statuses schedule anything. `reference` is the status that lets a worked
# example live beside real connectors without becoming one: it is validated and
# built by the test harness, and skipped by the DAG generator, the pool loop, the
# secrets push and terraform alike.
CONNECTED = "connected"
REFERENCE = "reference"
PAUSED = "paused"
SCHEDULED_STATUSES = {CONNECTED, PAUSED}

# When absence from a run's loads is allowed to mean "deleted upstream".
#
#   always        the resource is fully re-fetched every run, so anything not
#                 seen really is gone.
#   full_history  absence only means deleted on a run that covered all of
#                 history. On an incremental run everything outside the window
#                 is absent and innocent, and tombstoning it would wipe the
#                 warehouse.
_SOFT_DELETE_MODES = {"always", "full_history"}

# Strategies built without any Python. Everything else names an algorithm the
# connector's own extension.py must supply, and build_source raises when it does
# not.
DECLARATIVE_STRATEGIES = {"full_refresh", "cursor"}

DEFAULT_RUNTIME_TIER = "standard"


class SpecError(ValueError):
    """A source spec that cannot be trusted to describe a connector."""


def sources_dir():
    override = os.environ.get(SOURCES_DIR_ENV, "").strip()
    return Path(override) if override else REPO_ROOT / "sources"


@functools.lru_cache(maxsize=4)
def _read_schema(path_text):
    return json.loads(Path(path_text).read_text())


def schema(directory=None):
    """The JSON Schema every spec is validated against, or None.

    Read from disk rather than embedded so there is exactly one description of a
    spec's shape, and so the file an editor's `$schema` header points at is the
    same one the loader enforces.

    Absent is tolerated: semantic validation below does not depend on it, and
    `DS_SOURCES_DIR` legitimately points at a bare tmp directory in tests.
    """
    base = Path(directory) if directory else sources_dir()
    path = base / SCHEMA_FILENAME
    if not path.is_file():
        return None
    return _read_schema(str(path))


def available(directory=None):
    """Every source with a spec on disk, sorted. All statuses."""
    base = Path(directory) if directory else sources_dir()
    if not base.is_dir():
        return []
    return sorted(
        entry.name for entry in base.iterdir()
        if entry.is_dir() and (entry / SPEC_FILENAME).is_file()
    )


def load_all(directory=None):
    """Every spec, parsed. Raises on the first one that will not load."""
    return [load(name, directory=directory) for name in available(directory)]


def connected(directory=None):
    """Sources that schedule and demand a credential on every clone."""
    return [spec for spec in load_all(directory) if spec.is_connected]


def load(name, directory=None):
    """Parse and validate `sources/<name>/source.yml` into a Spec."""
    base = Path(directory) if directory else sources_dir()
    path = base / name / SPEC_FILENAME
    if not path.is_file():
        legacy = base / f"{name}.yml"
        if legacy.is_file():
            raise SpecError(
                f"{legacy} is the old flat layout. A connector is a directory now: "
                f"move it to {path} so its extension, fixtures, research and "
                f"reviewed schemas can live beside the contract they belong to."
            )
        known = ", ".join(available(directory)) or "none"
        raise SpecError(f"no source spec at {path} (known sources: {known})")
    try:
        document = yaml.safe_load(path.read_text())
    except yaml.YAMLError as error:
        raise SpecError(f"{path} is not valid YAML: {error}") from error
    if not isinstance(document, dict):
        raise SpecError(f"{path} must be a mapping, got {type(document).__name__}")
    spec = Spec(document, path, document_schema=schema(base))
    if spec.name != name:
        raise SpecError(
            f"{path}: directory is {name!r} but `name:` is {spec.name!r}. They must "
            f"match — the directory is the identity everything else is derived from, "
            f"and a mismatch produces DAG ids nothing else predicts."
        )
    return spec


def _validate_against_schema(document, document_schema, where):
    """Shape validation, delegated to the JSON Schema.

    Everything expressible as "which keys, of what type, in what combination"
    lives there. What stays in Python is what a schema cannot say: that a
    reference names a resource this spec declares, that the directory matches the
    `name` field, that a delegated resource has a builder.
    """
    if document_schema is None:
        return
    import jsonschema

    validator = jsonschema.Draft202012Validator(document_schema)
    errors = sorted(validator.iter_errors(document), key=lambda e: list(e.absolute_path))
    if not errors:
        return
    lines = []
    for error in errors[:10]:
        location = ".".join(str(part) for part in error.absolute_path) or "(root)"
        lines.append(f"  {location}: {error.message}")
    more = f"\n  ... and {len(errors) - 10} more" if len(errors) > 10 else ""
    raise SpecError(f"{where} does not match the source schema:\n" + "\n".join(lines) + more)


def _require(where, mapping, *keys):
    missing = [key for key in keys if not mapping.get(key)]
    if missing:
        raise SpecError(f"{where}: missing required key(s) {', '.join(missing)}")


class Resource:
    """One endpoint's worth of the contract."""

    def __init__(self, entry, source_name):
        where = f"{source_name}.resources[{entry.get('name', '?')}]"
        if not isinstance(entry, dict):
            raise SpecError(f"{source_name}: each resource must be a mapping")
        _require(where, entry, "name", "primary_key")
        self._entry = entry
        self._where = where
        self.strategy = self.incremental.get("strategy", "full_refresh")

    name = property(lambda self: self._entry["name"])
    primary_key = property(lambda self: self._entry["primary_key"])

    @property
    def endpoint(self):
        """The resource's endpoint, wherever the strategy happened to declare it.

        A strategy that fetches from one place may put it under `incremental`
        rather than at the top level. Falling back matters beyond tidiness:
        rate-limit families and request routing are both derived from this, so
        a resource whose endpoint the fallback missed would quietly get its own
        budget instead of sharing the one the API actually grants.

        Strategies with more than one endpoint (search_window has two) are left
        alone — there is no single answer, and guessing one would route half the
        requests wrongly.
        """
        return self._entry.get("endpoint") or self.incremental.get("endpoint") or {}

    incremental = property(lambda self: self._entry.get("incremental") or {})
    promote = property(lambda self: self._entry.get("promote") or {})
    html_text = property(lambda self: self._entry.get("html_text") or {})

    @property
    def soft_delete(self):
        """'always', 'full_history', or None. Never a bare bool.

        The two modes are not a nuance: applying `always` to a resource that is
        only ever fetched incrementally tombstones every row outside the current
        window on the first run.
        """
        value = self._entry.get("soft_delete")
        if value in (None, False):
            return None
        if value is True or value not in _SOFT_DELETE_MODES:
            raise SpecError(
                f"{self._where}: soft_delete must be one of "
                f"{', '.join(sorted(_SOFT_DELETE_MODES))}, got {value!r}"
            )
        return value

    @property
    def hint_columns(self):
        """Columns dlt must type deterministically because something compares them.

        Defaults to every timestamp column, which is right for a simple source.
        A resource that parses more timestamps than it compares narrows it, so a
        column nothing depends on stays inferred rather than becoming a schema
        commitment.
        """
        declared = self._entry.get("hint_columns")
        return tuple(declared) if declared is not None else self.timestamp_columns

    @property
    def write_disposition(self):
        return self._entry.get("write_disposition", "merge")

    @property
    def timestamp_columns(self):
        return tuple(self._entry.get("timestamp_columns") or ())

    @property
    def family(self):
        """Rate-limit family. Defaults to the resource's own name, so a source
        that does not group its endpoints gets one budget each."""
        return self.endpoint.get("family") or self.name

    @property
    def all_endpoints(self):
        """Every endpoint this resource may call, each with its own family.

        A strategy can have more than one: search_window calls a windowed list
        endpoint and a search endpoint, which are separately rate-limited in
        every API that offers both. Reading only the primary endpoint meant the
        others were invisible to the pacer — Pylon declared budgets for
        issues_list, issues_search and messages, and all three were routed to a
        family nothing billed, so the budgets were published and never applied.
        """
        found = []
        incremental = self.incremental
        candidates = [
            self._entry.get("endpoint"),
            incremental.get("endpoint"),
            (incremental.get("window") or {}).get("endpoint"),
            (incremental.get("search") or {}).get("endpoint"),
        ]
        for endpoint in candidates:
            if endpoint:
                found.append((endpoint, endpoint.get("family") or self.name))
        if not found:
            # No endpoint declared anywhere: the declarative path defaults the
            # path to /<name>, so the route table needs the same assumption.
            found.append(({"path": f"/{self.name}"}, self.name))
        return tuple(found)

    @property
    def families(self):
        return tuple(family for _, family in self.all_endpoints)

    def data_selector(self, spec):
        """Where this endpoint's records live in its response envelope.

        Per resource, falling back to the source-wide default. Source-wide used
        to be the only option, which cost Customer.io its declaration entirely:
        one API can wrap `/customers` and `/segments` differently, and a single
        selector is then wrong for one of them.
        """
        return self.endpoint.get("data_selector") or spec.pagination.get("data_selector")

    @property
    def is_declarative(self):
        """Whether the declarative layer can build this resource with no Python."""
        return self.strategy in DECLARATIVE_STRATEGIES

    def __repr__(self):
        return f"<Resource {self.name} strategy={self.strategy}>"


class Spec:
    """A validated source definition."""

    def __init__(self, document, path, document_schema=None):
        self.path = Path(path)
        self.dir = self.path.parent
        _validate_against_schema(document, document_schema, str(path))
        # Re-checked in Python because the schema is optional (a tmp sources dir
        # in a test has none) and these four are what everything else
        # dereferences immediately.
        _require(str(path), document, "name", "status", "api", "resources")
        self._doc = document

        api = document["api"]
        _require(f"{self.name}.api", api, "base_url", "auth")
        _require(f"{self.name}.api.auth", api["auth"], "type", "token_env")

        if not isinstance(document["resources"], list) or not document["resources"]:
            raise SpecError(f"{self.name}: `resources` must be a non-empty list")
        self.resources = [Resource(entry, self.name) for entry in document["resources"]]

        names = [resource.name for resource in self.resources]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise SpecError(f"{self.name}: duplicate resource name(s) {', '.join(duplicates)}")

        self._validate_references()
        self._validate_freshness()

    name = property(lambda self: self._doc["name"])
    status = property(lambda self: self._doc["status"])
    display_name = property(lambda self: self._doc.get("display_name") or self._doc["name"])
    owner = property(lambda self: self._doc.get("owner"))
    docs_url = property(lambda self: self._doc.get("docs_url"))
    api = property(lambda self: self._doc["api"])
    base_url = property(lambda self: self._doc["api"]["base_url"])
    rate_limits = property(lambda self: dict(self._doc.get("rate_limits") or {}))
    orchestration = property(lambda self: dict(self._doc.get("orchestration") or {}))
    pagination = property(lambda self: dict(self._doc.get("pagination") or {}))
    quality = property(lambda self: dict(self._doc.get("quality") or {}))

    @property
    def is_connected(self):
        """Scheduled, unpaused, and demanding its credential on every clone."""
        return self.status == CONNECTED

    @property
    def schedules_dags(self):
        """Whether the DAG generator should build anything for this spec.

        A `reference` spec is the worked example: it parses, it validates, the
        contract suite builds a source from it — and nothing schedules it. That
        is what lets the example live beside real connectors instead of being
        exiled somewhere no test could reach it.
        """
        return self.status in SCHEDULED_STATUSES

    @property
    def uses_extension(self):
        """Whether this connector ships an extension.py beside its spec.

        Accepts the legacy string spelling (`extensions: swoogo`), which named a
        module inside the runtime package. The name was always redundant with the
        file's location and could drift from it, so `true` is the spelling now.
        """
        return bool(self._doc.get("extensions"))

    @property
    def extension_path(self):
        return self.dir / EXTENSION_FILENAME

    @property
    def token_env(self):
        return self._doc["api"]["auth"]["token_env"]

    @property
    def dataset(self):
        """The warehouse database these tables land in.

        Derived, never configurable: `raw_<source>` keeps every source's blast
        radius its own, and a spec that could name it something else invites two
        sources sharing one database and one soft-delete pass.
        """
        return f"raw_{self.name}"

    @property
    def pool(self):
        return self.orchestration.get("pool") or f"{self.name}_pipeline"

    @property
    def backfill_start(self):
        return self.orchestration.get("backfill_start")

    @property
    def schedule(self):
        return self.orchestration.get("schedule")

    @property
    def reconcile_schedule(self):
        return self.orchestration.get("reconcile")

    def runtime_tier(self, task="ingest"):
        """The task-size tier for one of the three DAGs.

        A bare string sizes every task; a mapping sizes them separately, which is
        what a source whose hourly run is trivial and whose backfill is not
        actually needs.
        """
        declared = self.orchestration.get("runtime") or DEFAULT_RUNTIME_TIER
        if isinstance(declared, str):
            return declared
        return declared.get(task) or DEFAULT_RUNTIME_TIER

    @property
    def dag_ids(self):
        """Exactly the DAG ids this spec generates, in registration order.

        One derivation, so the generator, the deploy verification and the
        manifest cannot disagree about whether a reconcile DAG exists — which
        they did, in three places, each re-deciding it from the same two keys.

        A paused connector keeps its ingest and backfill DAGs and loses its
        schedules, but gets no reconcile DAG at all: the generator drops the
        reconcile cron along with the ingest one, and what would be left is a
        tombstone pass sitting there triggerable on a connector nobody is
        loading. `TestTheTwoDerivationsAgree` is what holds the two in step.
        """
        if not self.schedules_dags:
            return ()
        ids = [f"{self.name}_ingest", f"{self.name}_backfill"]
        if self.status != PAUSED and self.reconcile_schedule and self.backfill_start:
            ids.append(f"{self.name}_reconcile")
        return tuple(ids)

    def resource(self, name):
        for resource in self.resources:
            if resource.name == name:
                return resource
        raise SpecError(f"{self.name}: no resource named {name!r}")

    @property
    def resource_names(self):
        return tuple(resource.name for resource in self.resources)

    @property
    def delegated_resources(self):
        """Resources whose strategy the declarative layer cannot build."""
        return tuple(r for r in self.resources if not r.is_declarative)

    @property
    def soft_delete_tables(self):
        """Resources whose absence always means deletion — eligible every run."""
        return tuple(r.name for r in self.resources if r.soft_delete == "always")

    @property
    def full_history_soft_delete_tables(self):
        """Resources tombstoned only by a run that covered all of history."""
        return tuple(r.name for r in self.resources if r.soft_delete == "full_history")

    @property
    def tombstoned_tables(self):
        """Every resource carrying a _deleted column, under either mode."""
        return tuple(r.name for r in self.resources if r.soft_delete)

    @property
    def freshness_checks(self):
        """Freshness contracts as a list, however many the spec declared.

        One mapping or several: a source with two clocks — an append-only event
        table and a slowly-changing entity table — has two answers to "is this
        stale", and allowing only one meant declaring the less useful of them.
        """
        declared = self.quality.get("freshness")
        if not declared:
            return ()
        if isinstance(declared, dict):
            return (declared,)
        return tuple(declared)

    def timeout_minutes(self, task, default):
        return int((self.orchestration.get("timeouts_minutes") or {}).get(task, default))

    def _validate_references(self):
        """Referential edges must name resources this spec actually declares.

        Checked here rather than at query time: a typo'd parent surfaces as a
        LEFT ANTI JOIN against a table that does not exist, which reads as a
        broken warehouse rather than a broken spec.
        """
        known = set(self.resource_names)
        for edge in self.quality.get("references") or []:
            for side in ("child", "parent"):
                value = edge.get(side)
                if not value or "." not in str(value):
                    raise SpecError(
                        f"{self.name}.quality.references: {side} must be 'table.column', got {value!r}"
                    )
                table = str(value).split(".", 1)[0]
                if table not in known:
                    raise SpecError(
                        f"{self.name}.quality.references: {side} names table {table!r}, "
                        f"which is not a resource in this spec ({', '.join(sorted(known))})"
                    )

    def _validate_freshness(self):
        """A freshness contract must name a table this source lands.

        Same reasoning as references: the check becomes SQL against that table,
        so a typo surfaces as a missing-table error in the warehouse rather than
        as a spec error here.
        """
        known = set(self.resource_names)
        for entry in self.freshness_checks:
            table = entry.get("table")
            if table not in known:
                raise SpecError(
                    f"{self.name}.quality.freshness: table {table!r} is not a resource "
                    f"in this spec ({', '.join(sorted(known))})"
                )

    def __repr__(self):
        return f"<Spec {self.name} status={self.status} resources={len(self.resources)}>"
