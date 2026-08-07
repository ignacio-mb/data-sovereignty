"""A spec with no Python must actually load data.

This is the test that licenses emptying the repo. Until the declarative path can
fetch, deleting the hand-written Pylon client leaves a stack that cannot ingest
anything — clean and useless. So this drives a spec end to end into duckdb
through dlt's REST source, with no extension module involved.
"""

import duckdb
import pytest
import requests_mock as rm_module

from ingest_runtime import locality, runtime, spec
from ingest_runtime.warehouse import build_pipeline

SPEC = """
name: probe
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
    exclude_columns: [internal_notes]
"""


@pytest.fixture
def probe_spec(tmp_path):
    (tmp_path / "probe.yml").write_text(SPEC)
    return spec.load("probe", directory=tmp_path)


def widget(n):
    return {"id": f"w{n}", "name": f"Widget {n}", "owner": {"id": f"o{n}"},
            "meta": {"nested": True}, "created_at": "2026-01-01T00:00:00Z",
            "internal_notes": f"sensitive note for w{n}"}


def test_a_spec_with_no_python_loads_into_the_warehouse(probe_spec, monkeypatch, tmp_path):
    monkeypatch.setenv("PROBE_TOKEN", "secret")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    monkeypatch.chdir(tmp_path)

    with rm_module.Mocker() as mock:
        # Two pages, so the paginator is genuinely exercised rather than the
        # first response happening to be the whole dataset.
        mock.get("https://probe.test/widgets?limit=2",
                 json={"data": [widget(1), widget(2)], "pagination": {"next": "c2"}})
        mock.get("https://probe.test/widgets?limit=2&cursor=c2",
                 json={"data": [widget(3)], "pagination": {"next": None}})

        sources = runtime.build_source(probe_spec)
        pipeline = build_pipeline("probe", destination="duckdb")
        for source in sources:
            pipeline.run(source).raise_on_failed_jobs()

    con = duckdb.connect(str(tmp_path / "probe_duckdb.duckdb"))
    rows = con.execute(
        "SELECT id, owner_id, _deleted, typeof(meta) FROM raw_probe.widgets ORDER BY id"
    ).fetchall()

    assert [r[0] for r in rows] == ["w1", "w2", "w3"], "both pages must land"
    assert [r[1] for r in rows] == ["o1", "o2", "o3"], "promoted scalar became a column"
    assert all(r[2] is False for r in rows), "tombstone column present and false"
    assert all("VARCHAR" in r[3].upper() for r in rows), "nested object stayed JSON text"

    columns = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw_probe' AND table_name = 'widgets'"
    ).fetchall()}
    assert "internal_notes" not in columns, "exclude_columns must keep the field out entirely"


class TestRateLimits:
    """The spec's `rate_limits` must reach the request path.

    Constructing an EndpointPacer is not pacing. The CLI built one, passed it
    nowhere, and reported its counters in the run summary — so the stack claimed a
    published budget it never applied, and `requests_by_family` was always `{}` in
    ops.pipeline_runs. Nothing structural catches that; only counting real requests
    does.
    """

    @staticmethod
    def two_pages(mock):
        mock.get("https://probe.test/widgets?limit=2",
                 json={"data": [widget(1), widget(2)], "pagination": {"next": "c2"}})
        mock.get("https://probe.test/widgets?limit=2&cursor=c2",
                 json={"data": [widget(3)], "pagination": {"next": None}})

    def run(self, probe_spec, tmp_path, paced):
        with rm_module.Mocker() as mock:
            self.two_pages(mock)
            sources = runtime.build_source(probe_spec, paced=paced)
            pipeline = build_pipeline("probe", destination="duckdb")
            for source in sources:
                pipeline.run(source).raise_on_failed_jobs()

    @pytest.fixture(autouse=True)
    def isolated(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PROBE_TOKEN", "secret")
        monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
        monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
        monkeypatch.chdir(tmp_path)

    def test_every_request_is_paced_and_counted(self, probe_spec, tmp_path):
        slept = []
        paced = runtime.EndpointPacer(probe_spec.rate_limits, sleeper=slept.append)

        self.run(probe_spec, tmp_path, paced)

        # Both pages, attributed to the family the spec declared a budget for.
        assert paced.requests_made["widgets"] == 2, dict(paced.requests_made)
        assert "unmatched" not in paced.requests_made, \
            "every request should match a declared route"
        # 120/minute is one every 0.5s, and the two pages are back to back.
        assert slept and all(0 < s <= 0.5 for s in slept), slept

    def test_without_a_pacer_nothing_is_throttled(self, probe_spec, tmp_path):
        """The default stays unpaced, so a duckdb smoke run does not crawl."""
        paced = runtime.EndpointPacer(probe_spec.rate_limits, sleeper=lambda _: None)
        self.run(probe_spec, tmp_path, None)
        assert paced.requests_made == {}

    def test_an_endpoint_with_no_declared_budget_is_counted_not_rejected(self):
        """A spec need not publish a budget for every family, and the one endpoint
        nobody wrote a limit for must not take the connector down."""
        slept = []
        paced = runtime.EndpointPacer({"widgets": 120}, sleeper=slept.append)
        paced.wait("something_else")
        assert paced.requests_made["something_else"] == 1
        assert slept == []


def test_a_missing_token_says_which_variable(probe_spec, monkeypatch):
    monkeypatch.delenv("PROBE_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="PROBE_TOKEN"):
        runtime.build_source(probe_spec)


def test_a_strategy_needing_an_extension_refuses_without_one(tmp_path, monkeypatch):
    monkeypatch.setenv("T", "x")
    (tmp_path / "n.yml").write_text(
        "name: n\napi:\n  base_url: https://e.test\n  auth: {type: bearer, token_env: T}\n"
        "resources:\n  - name: a\n    primary_key: id\n"
        "    incremental: {strategy: parent_watermark, parent: b}\n"
        "  - {name: b, primary_key: id}\n"
    )
    loaded = spec.load("n", directory=tmp_path)
    with pytest.raises(RuntimeError, match="declares no `extensions`"):
        runtime.build_source(loaded, selected=["a"])


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
