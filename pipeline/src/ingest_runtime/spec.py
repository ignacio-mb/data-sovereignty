"""The source contract: `sources/<name>.yml` loaded and validated.

One file defines a connector — what to call, how to page it, what is
incremental, when it runs, and what "correct" means for the tables it lands.
Nothing about any particular API is compiled into the runtime.

Validation is strict about unknown keys on purpose. A misspelled `time_stamp_columns`
that is silently ignored produces a connector that runs, looks healthy, and
lands untyped strings — the class of failure this stack exists to catch. An
unknown key is a typo until proven otherwise, so it raises.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml

# sources/ sits beside pipeline/, at the repo root: this file is
# pipeline/src/ingest_runtime/spec.py, so three parents up is `pipeline/`.
REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCES_DIR_ENV = "DS_SOURCES_DIR"

_TOP_LEVEL = {
    "name", "display_name", "docs_url", "api", "rate_limits", "orchestration",
    "resources", "pagination", "quality", "extensions",
}
_RESOURCE_KEYS = {
    "name", "primary_key", "write_disposition", "soft_delete", "endpoint",
    "incremental", "timestamp_columns", "hint_columns", "promote", "html_text",
    "exclude_columns",
}
_QUALITY_KEYS = {
    "required", "freshness", "max_deleted_fraction", "references", "not_null",
}

# Strategies the runtime recognises. Adding one is a code change, which is the
# point: a strategy is a fetch algorithm, not a setting.
#
# Recognised is not the same as built. Only full_refresh is built declaratively
# (runtime._DECLARATIVE_STRATEGIES); the other two must be supplied by the spec's
# `extensions` module, and build_source raises when they are not.
_STRATEGIES = {"search_window", "parent_watermark", "parent_fanout", "full_refresh"}

# When absence from a run's loads is allowed to mean "deleted upstream".
#
#   always        the resource is fully re-fetched every run, so anything not
#                 seen really is gone.
#   full_history  absence only means deleted on a run that covered all of
#                 history. On an incremental run everything outside the window
#                 is absent and innocent, and tombstoning it would wipe the
#                 warehouse.
_SOFT_DELETE_MODES = {"always", "full_history"}


class SpecError(ValueError):
    """A source spec that cannot be trusted to describe a connector."""


def sources_dir():
    override = os.environ.get(SOURCES_DIR_ENV, "").strip()
    return Path(override) if override else REPO_ROOT / "sources"


def available():
    """Source names with a spec on disk, sorted."""
    directory = sources_dir()
    if not directory.is_dir():
        return []
    return sorted(path.stem for path in directory.glob("*.yml"))


def load(name, directory=None):
    """Parse and validate `sources/<name>.yml` into a Spec."""
    path = (Path(directory) if directory else sources_dir()) / f"{name}.yml"
    if not path.is_file():
        known = ", ".join(available()) or "none"
        raise SpecError(f"no source spec at {path} (known sources: {known})")
    try:
        document = yaml.safe_load(path.read_text())
    except yaml.YAMLError as error:
        raise SpecError(f"{path} is not valid YAML: {error}") from error
    if not isinstance(document, dict):
        raise SpecError(f"{path} must be a mapping, got {type(document).__name__}")
    return Spec(document, path)


def _reject_unknown(where, got, allowed):
    unknown = sorted(set(got) - allowed)
    if unknown:
        raise SpecError(
            f"{where}: unknown key(s) {', '.join(unknown)}. "
            f"Allowed: {', '.join(sorted(allowed))}"
        )


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
        _reject_unknown(where, entry, _RESOURCE_KEYS)
        _require(where, entry, "name", "primary_key")
        self._entry = entry
        self._where = where

        strategy = self.incremental.get("strategy", "full_refresh")
        if strategy not in _STRATEGIES:
            raise SpecError(
                f"{where}: unknown incremental strategy {strategy!r}. "
                f"Known: {', '.join(sorted(_STRATEGIES))}"
            )
        self.strategy = strategy

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
    def exclude_columns(self):
        """Top-level fields to drop entirely rather than land.

        `promote` has no mirror for "never mind this one" — every other field
        the source sends lands, JSON-stringified if nested. That is usually
        right (a connector should not decide what is interesting), but it is
        wrong for a field whose whole content is sensitive rather than
        structural: Lever's `notes.fields`/`offers.fields` carry freeform
        candidate-assessment text and compensation/PII, not something this
        landing layer should retain just because the API happens to send it.
        Dropped before the row is built, not after — it never reaches
        `flatten_record`, so there is no JSON-stringify cost paid on data that
        is about to be discarded.
        """
        return tuple(self._entry.get("exclude_columns") or ())
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

    def __repr__(self):
        return f"<Resource {self.name} strategy={self.strategy}>"


class Spec:
    """A validated source definition."""

    def __init__(self, document, path):
        self.path = path
        _reject_unknown(str(path), document, _TOP_LEVEL)
        _require(str(path), document, "name", "api", "resources")
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

        _reject_unknown(f"{self.name}.quality", self.quality, _QUALITY_KEYS)
        self._validate_references()

    name = property(lambda self: self._doc["name"])
    display_name = property(lambda self: self._doc.get("display_name") or self._doc["name"])
    api = property(lambda self: self._doc["api"])
    base_url = property(lambda self: self._doc["api"]["base_url"])
    rate_limits = property(lambda self: dict(self._doc.get("rate_limits") or {}))
    orchestration = property(lambda self: dict(self._doc.get("orchestration") or {}))
    pagination = property(lambda self: dict(self._doc.get("pagination") or {}))
    quality = property(lambda self: dict(self._doc.get("quality") or {}))
    extensions = property(lambda self: self._doc.get("extensions"))

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

    def resource(self, name):
        for resource in self.resources:
            if resource.name == name:
                return resource
        raise SpecError(f"{self.name}: no resource named {name!r}")

    @property
    def resource_names(self):
        return tuple(resource.name for resource in self.resources)

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

    def __repr__(self):
        return f"<Spec {self.name} resources={len(self.resources)}>"
