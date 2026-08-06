"""How a spec says an endpoint pages, in increasing order of escape.

Three ways, and the ordering is the design: a registered shorthand for what is
common, a dlt paginator name passed straight through so everything dlt already
knows stays available without this file growing an entry, and a raw config
mapping for the rest. `extension` is the fourth, for APIs whose paging is a
behaviour rather than a shape.

What is asserted here is mostly that the pass-through really is a pass-through.
A shorthand table that had to grow an entry per API would be exactly the
compiled-in per-API knowledge the whole contract is written to avoid.
"""

from __future__ import annotations

import pytest

from ingest_runtime import paginators, spec

SPEC = """
name: probe
status: reference
api:
  base_url: https://probe.test
  auth: {type: bearer, token_env: PROBE_TOKEN}
pagination:
  kind: cursor
  cursor_path: meta.next
  cursor_param: after
resources:
  - {name: widgets, primary_key: id}
  - name: gadgets
    primary_key: id
    endpoint:
      path: /gadgets
      paginator: {type: page_number, base_page: 1, page_param: page, total_path: _meta.pages}
  - name: sprockets
    primary_key: id
    endpoint: {path: /sprockets, paginator: single_page}
  - name: cogs
    primary_key: id
    endpoint: {path: /cogs, cursor_path: paging.token, cursor_param: token, paginator: cursor}
"""

BARE = """
name: bare
status: reference
api:
  base_url: https://bare.test
  auth: {type: bearer, token_env: BARE_TOKEN}
resources:
  - {name: things, primary_key: id}
"""


def write(tmp_path, text, name="probe"):
    (tmp_path / name).mkdir(exist_ok=True)
    (tmp_path / name / "source.yml").write_text(text)
    return spec.load(name, directory=tmp_path)


@pytest.fixture
def probe(tmp_path):
    return write(tmp_path, SPEC)


def test_the_registry_holds_the_shorthands_not_the_apis():
    """Every entry here must be a shape more than one API has. A name only one
    source uses belongs in that source's endpoint config."""
    assert paginators.registered() == ("cursor",)


class TestTheShorthand:
    def test_cursor_expands_from_the_source_wide_defaults(self, probe):
        assert paginators.build(probe, probe.resource("widgets")) == {
            "type": "cursor", "cursor_path": "meta.next", "cursor_param": "after"}

    def test_a_resource_can_override_both_halves(self, probe):
        """One API can page two endpoints differently, and the override has to
        be per resource or the second one is silently walked wrongly."""
        assert paginators.build(probe, probe.resource("cogs")) == {
            "type": "cursor", "cursor_path": "paging.token", "cursor_param": "token"}


class TestThePassThrough:
    def test_a_raw_config_mapping_is_handed_to_dlt_untouched(self, probe):
        """This is the real escape hatch, and it already earns its keep:
        Swoogo's page-number envelope arrived through it without a line of code
        in the runtime."""
        assert paginators.build(probe, probe.resource("gadgets")) == {
            "type": "page_number", "base_page": 1, "page_param": "page",
            "total_path": "_meta.pages"}

    def test_a_dlt_paginator_name_is_not_validated_here(self, probe):
        """dlt raises a clear error naming an unknown paginator, and
        duplicating its list would go stale the first time dlt adds one."""
        assert paginators.build(probe, probe.resource("sprockets")) == "single_page"

    def test_a_spec_declaring_nothing_lets_dlt_detect(self, tmp_path):
        bare = write(tmp_path, BARE, name="bare")
        assert paginators.build(bare, bare.resource("things")) == "auto"


class TestTheExtensionEscapeHatch:
    """For paging that is a behaviour rather than a shape — Pylon's, which
    claims another page while serving nothing."""

    @pytest.fixture
    def delegating(self, tmp_path):
        return write(tmp_path, SPEC.replace(
            "endpoint: {path: /sprockets, paginator: single_page}",
            "endpoint: {path: /sprockets, paginator: extension}"))

    def test_it_asks_the_connector_to_build_one(self, delegating):
        built = object()
        extension = type("Extension", (), {
            "build_paginator": staticmethod(lambda s, r: built)})
        assert paginators.build(
            delegating, delegating.resource("sprockets"), extension) is built

    def test_a_connector_that_did_not_write_it_is_told_which_file(self, delegating):
        with pytest.raises(RuntimeError, match="extension.py"):
            paginators.build(delegating, delegating.resource("sprockets"), object())
