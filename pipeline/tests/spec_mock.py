"""A mock API driven by the spec, so a connector can be proved offline.

A connector's fixtures are JSON arrays of records — one file per resource — and
this turns them into an API that behaves the way the spec SAYS the real one
does: the declared envelope, the declared paginator, the declared page-size
parameter. That is what makes the contract suite a test of the spec rather than
of a hand-written stub agreeing with itself. Change `data_selector` and the
fixtures stop being found; change the paginator and the pages stop being walked.

Two rules are deliberate.

Pages are always SMALLER than the fixture, whatever page size the spec asked
for. An API is allowed to serve fewer records than requested, and a test whose
first response happens to be the whole dataset proves nothing about paging — a
paginator that stops after one page passes it. Four records are served two at a
time; the page-size parameter is asserted separately, on the wire.

Only DECLARATIVE resources are served from configuration. Anything a
connector's extension fetches is, by definition, behaviour no spec key
describes, so `fixtures/server.py` registers those endpoints itself and this
module stays free of any particular API.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from math import ceil
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import requests
import requests_mock

from ingest_runtime import paginators

FIXTURES_DIRNAME = "fixtures"
SERVER_FILENAME = "server.py"

# Synthetic package for the per-connector servers, mirroring how the runtime
# loads an extension: by path, under a name that will not collide with a real
# module and that makes a traceback say which connector it came from.
SERVER_PACKAGE = "ds_fixture_server"


class Unsupported(RuntimeError):
    """A paginator shape this server has no envelope for.

    Raised rather than guessed: a mock that invented a shape would let a spec
    declare paging nothing here actually walks, which is the failure the whole
    suite exists to catch. The contract suite turns this into a skip that names
    the paginator.
    """


def fixtures_dir(spec):
    return spec.dir / FIXTURES_DIRNAME


def fixture_rows(spec, resource_name):
    """The records for one resource, or None if the connector ships none."""
    path = fixtures_dir(spec) / f"{resource_name}.json"
    if not path.is_file():
        return None
    rows = json.loads(path.read_text())
    if not isinstance(rows, list):
        raise TypeError(f"{path} must be a JSON array of records")
    return rows


def fixture_backed(spec):
    """Resource names this connector can be proved against, in spec order."""
    return tuple(r.name for r in spec.resources
                 if (fixtures_dir(spec) / f"{r.name}.json").is_file())


def query(request):
    """The request's query string with its values' case intact.

    `request.qs` lowercases the whole URL, which is right for matching and wrong
    for reading a value back: an ISO timestamp comes out as `2026-01-01t00:00:00`
    and an id as `iss-1`. Anything asserting on what was sent has to go through
    here.
    """
    return parse_qs(urlparse(request.url).query)


def page_size_for(rows):
    """Half the fixture, so every collection needs at least two pages."""
    return max(1, len(rows) // 2)


def path_matches(spec, declared, path):
    """Whether a request path is the endpoint the spec declared.

    Two things a naive comparison gets wrong. An endpoint path is relative to
    `base_url`, which for Swoogo carries `/api/v1` of its own, so the wire path
    is never the declared one. And matching by substring — the obvious thing —
    hides bugs: `/issues` is a prefix of `/issues/{id}/messages`, so a check on
    the list endpoint's parameters silently graded the messages requests too.
    So: anchored, base included, and `{issue_id}` matching one segment.
    """
    prefix = urlparse(spec.base_url).path.rstrip("/")
    pattern = "".join(
        "[^/]+" if part.startswith("{") and part.endswith("}") else re.escape(part)
        for part in re.split(r"(\{[^}]*\})", prefix + declared)
    )
    return re.fullmatch(pattern, path) is not None


def _set_path(body, dotted, value):
    node = body
    parts = dotted.split(".")
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


def _paginator_config(spec, resource):
    try:
        return paginators.build(spec, resource)
    except RuntimeError as error:
        raise Unsupported(str(error)) from error


def _envelope(selector, resource_name, window):
    """The records, under whatever key the spec says they arrive at.

    A spec with no `data_selector` is one whose envelope key differs per
    endpoint (Customer.io wraps `/v1/transactional` in `messages`), so dlt is
    left to detect the list. Wrapping under the resource name here keeps that
    detection genuinely exercised rather than handing dlt a bare array.
    """
    body = {}
    _set_path(body, selector or resource_name, window)
    return body


def _responder(spec, resource, rows):
    """A requests_mock callback serving `rows` the way the spec declares."""
    config = _paginator_config(spec, resource)
    selector = resource.data_selector(spec)
    size = page_size_for(rows)
    pages = max(1, ceil(len(rows) / size))

    if isinstance(config, dict):
        kind = config.get("type")
    elif config in ("auto", "single_page"):
        # Nothing declared, so there is no paging behaviour to exercise: dlt
        # detects what it can and a single page is the honest answer.
        kind = "single_page"
    else:
        kind = config

    if kind == "cursor":
        cursor_path = config["cursor_path"] if isinstance(config, dict) else \
            spec.pagination.get("cursor_path", "meta.next_cursor")
        cursor_param = config["cursor_param"] if isinstance(config, dict) else \
            spec.pagination.get("cursor_param", "cursor")

        def respond(request, context):
            # The cursor is opaque to the client, so the offset itself serves.
            offset = int(query(request).get(cursor_param, ["0"])[0])
            window = rows[offset:offset + size]
            body = _envelope(selector, resource.name, window)
            if offset + size < len(rows):
                _set_path(body, cursor_path, str(offset + size))
            return body

        return respond

    if kind == "page_number":
        page_param = config.get("page_param", "page")
        base_page = int(config.get("base_page", 1))
        total_path = config.get("total_path", "total")

        def respond(request, context):
            page = int(query(request).get(page_param, [str(base_page)])[0]) - base_page
            window = rows[page * size:(page + 1) * size]
            body = _envelope(selector, resource.name, window)
            _set_path(body, total_path, pages)
            return body

        return respond

    if kind == "offset":
        offset_param = config.get("offset_param", "offset")
        total_path = config.get("total_path", "total")

        def respond(request, context):
            offset = int(query(request).get(offset_param, ["0"])[0])
            body = _envelope(selector, resource.name, rows[offset:offset + size])
            _set_path(body, total_path, len(rows))
            return body

        return respond

    if kind == "single_page":
        def respond(request, context):
            return _envelope(selector, resource.name, rows)

        return respond

    raise Unsupported(
        f"{spec.name}.{resource.name} declares paginator {kind!r}, which this "
        f"mock has no envelope for. Add one here, or serve the endpoint from "
        f"{spec.name}/fixtures/{SERVER_FILENAME}."
    )


def _load_server(spec):
    """The connector's own fixture server, loaded by path, or None."""
    path = fixtures_dir(spec) / SERVER_FILENAME
    if not path.is_file():
        return None
    module_name = f"{SERVER_PACKAGE}.{spec.name}"
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    if not hasattr(module, "register"):
        raise RuntimeError(
            f"{path} must define register(mock, spec, fixtures) — that is the "
            f"whole seam for API behaviour no spec key describes."
        )
    return module


class SpecServer:
    """The connector's API, for the length of a `with` block.

    Records every request and every timeout, because both are assertions the
    contract suite makes: a request with no timeout can hang a pooled task
    forever, and a page-size parameter that never reached the wire is a spec
    that only claims to have been applied.
    """

    def __init__(self, spec):
        self.spec = spec
        self.fixtures = {name: fixture_rows(spec, name) for name in fixture_backed(spec)}
        self.timeouts = []
        self._mock = requests_mock.Mocker()
        self._patch = None

    def __enter__(self):
        self._mock.start()
        try:
            for resource in self.spec.resources:
                rows = self.fixtures.get(resource.name)
                if rows is None or not resource.is_declarative:
                    continue
                endpoint = resource.endpoint
                url = self.spec.base_url.rstrip("/") + endpoint.get("path", f"/{resource.name}")
                self._mock.register_uri(
                    endpoint.get("method", "GET"), url,
                    json=_responder(self.spec, resource, rows))

            # After the declarative registrations, so a connector can override
            # one: requests_mock matches the most recent first, and Swoogo's
            # `/events` needs its sparse projection on top of the shape the
            # spec describes.
            server = _load_server(self.spec)
            if server is not None:
                server.register(self._mock, self.spec, self.fixtures)

            # requests_mock replaces Session.send, so this wraps the mock rather
            # than the network — which is the only place a timeout is visible at
            # all. `requests` defaults to waiting forever.
            self._patch = patch.object(requests.Session, "send", self._recording(
                requests.Session.send))
            self._patch.start()
        except Exception:
            self._mock.stop()
            raise
        return self

    def __exit__(self, *exc):
        if self._patch is not None:
            self._patch.stop()
            self._patch = None
        self._mock.stop()
        return False

    def _recording(self, send):
        timeouts = self.timeouts

        def recording_send(session, request, **kwargs):
            timeouts.append(kwargs.get("timeout"))
            return send(session, request, **kwargs)

        return recording_send

    @property
    def requests(self):
        return list(self._mock.request_history)

    def calls(self, declared_path, method=None):
        """Every request to one declared endpoint, template placeholders and all."""
        return [r for r in self.requests
                if (method is None or r.method == method)
                and path_matches(self.spec, declared_path, urlparse(r.url).path)]


def load_server_module(spec):
    """The connector's fixture server, for a test that wants to configure it."""
    return _load_server(spec)


__all__ = [
    "SERVER_FILENAME",
    "SpecServer",
    "Unsupported",
    "fixture_backed",
    "fixture_rows",
    "fixtures_dir",
    "load_server_module",
    "page_size_for",
    "path_matches",
    "query",
]
