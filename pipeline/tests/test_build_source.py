"""A spec with no Python must actually load data.

This is the test that licenses the escape hatch being an escape hatch. If the
declarative path cannot fetch, every connector needs a module and `extensions:`
stops being the exception it is documented as. So this drives a spec that
exists nowhere in sources/ end to end into duckdb through dlt's REST source,
with no extension module involved.

The connectors that DO ship here are covered by test_connector_contract.py
against their own fixtures. What is exercised here is the runtime's own
machinery: the two declarative strategies, the pacer's seam, and the guard that
refuses to write to production from a laptop.
"""

from __future__ import annotations

import duckdb
import pytest
import requests_mock as rm_module
from harness import load_into_duckdb

from ingest_runtime import locality, runtime, spec

SPEC = """
name: probe
status: reference
api:
  base_url: https://probe.test
  auth: {type: bearer, token_env: PROBE_TOKEN}
rate_limits:
  widgets: 120
pagination:
  kind: cursor
  cursor_path: pagination.next
  cursor_param: cursor
  data_selector: data
resources:
  - name: widgets
    primary_key: id
    write_disposition: merge
    soft_delete: always
    endpoint: {path: /widgets, page_size: 2}
    timestamp_columns: [created_at]
    promote: {owner.id: owner_id}
"""

CURSOR_SPEC = """
name: ticker
status: reference
api:
  base_url: https://ticker.test
  auth: {type: bearer, token_env: TICKER_TOKEN}
pagination:
  kind: cursor
  cursor_path: pagination.next
  cursor_param: cursor
  data_selector: data
resources:
  - name: events
    primary_key: id
    endpoint: {path: /events, page_size: 2}
    incremental:
      strategy: cursor
      cursor_field: updated_at
      cursor_param: updated_since
      lookback_seconds: 3600
    timestamp_columns: [updated_at]
"""


def write(tmp_path, text, name="probe"):
    (tmp_path / name).mkdir(parents=True, exist_ok=True)
    (tmp_path / name / "source.yml").write_text(text)
    return spec.load(name, directory=tmp_path)


@pytest.fixture
def probe(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBE_TOKEN", "secret")
    return write(tmp_path, SPEC)


def widget(n):
    return {"id": f"w{n}", "name": f"Widget {n}", "owner": {"id": f"o{n}"},
            "meta": {"nested": True}, "created_at": "2026-01-01T00:00:00Z"}


def two_pages(mock):
    """Two pages, so the paginator is genuinely walked rather than the first
    response happening to be the whole dataset."""
    mock.get("https://probe.test/widgets?limit=2",
             json={"data": [widget(1), widget(2)], "pagination": {"next": "c2"}})
    mock.get("https://probe.test/widgets?limit=2&cursor=c2",
             json={"data": [widget(3)], "pagination": {"next": None}})


def test_a_spec_with_no_python_loads_into_the_warehouse(probe, tmp_path):
    with rm_module.Mocker() as mock:
        two_pages(mock)
        load_into_duckdb(probe)

    rows = duckdb.connect(str(tmp_path / "probe_duckdb.duckdb")).execute(
        "SELECT id, owner_id, _deleted, typeof(meta) FROM raw_probe.widgets ORDER BY id"
    ).fetchall()

    assert [row[0] for row in rows] == ["w1", "w2", "w3"], "both pages must land"
    assert [row[1] for row in rows] == ["o1", "o2", "o3"], "promoted scalar became a column"
    assert all(row[2] is False for row in rows), "tombstone column present and false"
    assert all("VARCHAR" in row[3].upper() for row in rows), "nested object stayed JSON text"


class TestTheCursorStrategy:
    """The declarative incremental: the high-water mark pushed into the API's
    own filter, so the server sends only what changed.

    This is the shape most REST APIs actually have, and a connector using it
    needs no Python at all — which is the whole reason it is worth the runtime
    knowing about. The failure it prevents is an hourly run that reads a year.
    """

    @pytest.fixture
    def ticker(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TICKER_TOKEN", "secret")
        return write(tmp_path, CURSOR_SPEC, name="ticker")

    @staticmethod
    def serve(mock, seen):
        def respond(request, context):
            seen.append(request)
            return {"data": [{"id": "e1", "updated_at": "2026-03-01T10:00:00Z"},
                             {"id": "e2", "updated_at": "2026-03-02T10:00:00Z"}],
                    "pagination": {"next": None}}

        mock.get("https://ticker.test/events", json=respond)

    def test_the_first_run_asks_the_api_for_no_lower_bound(self, ticker):
        """There is nothing to bound by yet, and inventing one would skip
        history on the very load that is supposed to establish it."""
        seen = []
        with rm_module.Mocker() as mock:
            self.serve(mock, seen)
            load_into_duckdb(ticker)

        assert seen, "nothing was fetched"
        assert all("updated_since" not in request.qs for request in seen), \
            [request.url for request in seen]

    def test_the_next_run_sends_the_high_water_mark_as_a_query_parameter(self, ticker):
        """Server-side filtering, not client-side discarding: the difference
        between an hourly run that reads an hour and one that reads a year."""
        seen = []
        with rm_module.Mocker() as mock:
            self.serve(mock, seen)
            load_into_duckdb(ticker)
            first_run = len(seen)
            load_into_duckdb(ticker)

        resumed = seen[first_run:]
        assert resumed, "the second run fetched nothing at all"
        bounds = {request.qs["updated_since"][0] for request in resumed}
        assert bounds, [request.url for request in resumed]
        # The newest record was 2026-03-02T10:00:00Z and lookback_seconds is an
        # hour, so the bound is an hour before it: re-reading a little of what
        # was already loaded is free under merge, and it covers records written
        # while the previous run was mid-fetch.
        assert all(bound.startswith("2026-03-02 09:00:00") for bound in bounds), bounds

    def test_it_needs_no_extension(self, ticker):
        assert ticker.resource("events").is_declarative
        assert not ticker.delegated_resources
        assert not ticker.uses_extension


class TestRateLimits:
    """The spec's `rate_limits` must reach the request path.

    Constructing an EndpointPacer is not pacing. The CLI built one, passed it
    nowhere, and reported its counters in the run summary — so the stack
    claimed a published budget it never applied, and `requests_by_family` was
    always `{}` in ops.pipeline_runs. Nothing structural catches that; only
    counting real requests does.
    """

    @staticmethod
    def run(probe, paced):
        with rm_module.Mocker() as mock:
            two_pages(mock)
            load_into_duckdb(probe, paced=paced)

    def test_every_request_is_paced_and_counted(self, probe):
        slept = []
        paced = runtime.EndpointPacer(probe.rate_limits, sleeper=slept.append)

        self.run(probe, paced)

        assert paced.requests_made["widgets"] == 2, dict(paced.requests_made)
        assert "unmatched" not in paced.requests_made, \
            "every request should match a declared route"
        # 120/minute is one every 0.5s, and the two pages are back to back.
        assert slept and all(0 < seconds <= 0.5 for seconds in slept), slept

    def test_without_a_pacer_nothing_is_throttled(self, probe):
        """The default stays unpaced, so a duckdb smoke run does not crawl."""
        paced = runtime.EndpointPacer(probe.rate_limits, sleeper=lambda _: None)
        self.run(probe, None)
        assert paced.requests_made == {}

    def test_an_endpoint_with_no_declared_budget_is_counted_not_rejected(self):
        """A spec need not publish a budget for every family, and the one
        endpoint nobody wrote a limit for must not take the connector down."""
        slept = []
        paced = runtime.EndpointPacer({"widgets": 120}, sleeper=slept.append)
        paced.wait("something_else")
        assert paced.requests_made["something_else"] == 1
        assert slept == []


class TestDelegationIsExplicit:
    DELEGATED = """
name: needy
status: reference
api:
  base_url: https://needy.test
  auth: {type: bearer, token_env: NEEDY_TOKEN}
resources:
  - name: children
    primary_key: id
    incremental: {strategy: parent_watermark, parent: parents}
  - {name: parents, primary_key: id}
"""

    def test_a_strategy_needing_an_extension_refuses_without_one(self, tmp_path, monkeypatch):
        """A connector that quietly skips an endpoint looks exactly like one
        whose source has no data."""
        monkeypatch.setenv("NEEDY_TOKEN", "secret")
        needy = write(tmp_path, self.DELEGATED, name="needy")
        with pytest.raises(RuntimeError, match="declares no .extensions: true."):
            runtime.build_source(needy, selected=["children"])

    def test_an_extension_that_supplies_nothing_for_it_is_an_error_too(self, tmp_path,
                                                                      monkeypatch):
        monkeypatch.setenv("NEEDY_TOKEN", "secret")
        needy = write(tmp_path, self.DELEGATED + "extensions: true\n", name="needy")
        (tmp_path / "needy" / "extension.py").write_text("def unrelated():\n    pass\n")
        with pytest.raises(RuntimeError, match="build_children"):
            runtime.build_source(needy, selected=["children"])

    def test_the_declarative_resources_are_built_first(self, tmp_path, monkeypatch):
        """A worklist derived from the warehouse has to extract AFTER its
        parents are loaded, or it lags a run behind."""
        monkeypatch.setenv("NEEDY_TOKEN", "secret")
        needy = write(tmp_path, self.DELEGATED + "extensions: true\n", name="needy")
        (tmp_path / "needy" / "extension.py").write_text(
            "import dlt\n"
            "def build_resource(spec, resource, paced=None):\n"
            "    @dlt.source(name=spec.name)\n"
            "    def built():\n"
            "        return dlt.resource(lambda: iter(()), name=resource.name)\n"
            "    return built()\n")
        built = runtime.build_source(needy, selected=["children", "parents"])
        assert [next(iter(source.resources)) for source in built] == ["parents", "children"]


class TestHostSideProductionGuard:
    """`localhost` names a port, not a machine.

    An SSH tunnel to the instance binds loopback while Docker binds 0.0.0.0, so
    the tunnel wins and a host-side production run writes to the instance with
    nothing in the output to say so. That happened. The guard refuses the whole
    class rather than trying to tell the two apart at the address level, and it
    is shared with `dq`, which writes DDL and rows and had no guard at all.
    """

    ACTION = "a test action"
    ALTERNATIVES = "  use the container\n"

    @pytest.fixture(autouse=True)
    def no_override(self, monkeypatch):
        monkeypatch.delenv("DS_ALLOW_HOST_INGEST", raising=False)

    @pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1", "0.0.0.0", ""])
    def test_every_loopback_spelling_is_refused(self, monkeypatch, host):
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", host)
        with pytest.raises(locality.RemoteWarehouseRefused):
            locality.refuse_loopback_warehouse(
                self.ACTION, "DS_ALLOW_HOST_INGEST", self.ALTERNATIVES)

    def test_the_container_address_passes(self, monkeypatch):
        """Compose injects `warehouse-db`, so a legitimate run never trips this."""
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "warehouse-db")
        locality.refuse_loopback_warehouse(
            self.ACTION, "DS_ALLOW_HOST_INGEST", self.ALTERNATIVES)

    def test_the_override_is_explicit(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "127.0.0.1")
        monkeypatch.setenv("DS_ALLOW_HOST_INGEST", "1")
        locality.refuse_loopback_warehouse(
            self.ACTION, "DS_ALLOW_HOST_INGEST", self.ALTERNATIVES)
