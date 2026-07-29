"""A spec with no Python must actually load data.

This is the test that licenses emptying the repo. Until the declarative path can
fetch, deleting the hand-written Pylon client leaves a stack that cannot ingest
anything — clean and useless. So this drives a spec end to end into duckdb
through dlt's REST source, with no extension module involved.
"""

import duckdb
import pytest
import requests_mock as rm_module

from ingest_runtime import runtime, spec
from ingest_runtime.warehouse import build_pipeline

SPEC = """
name: probe
api:
  base_url: https://probe.test
  auth: {type: bearer, token_env: PROBE_TOKEN}
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


@pytest.fixture
def probe_spec(tmp_path):
    (tmp_path / "probe.yml").write_text(SPEC)
    return spec.load("probe", directory=tmp_path)


def widget(n):
    return {"id": f"w{n}", "name": f"Widget {n}", "owner": {"id": f"o{n}"},
            "meta": {"nested": True}, "created_at": "2026-01-01T00:00:00Z"}


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
