"""What a connector test needs that is not a fixture.

Split from conftest.py so a test module can import it: a conftest is loaded by
pytest under a name of pytest's choosing, and importing one by hand gets you a
second copy of it. Everything here is a plain function, so the fixtures in
conftest.py and the module-scoped ones in the contract suite — which cannot
take `monkeypatch` — can share one definition of what isolation means.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ingest_runtime import auth, extensions, spec

# Base64-shaped, because one registered auth type (oauth2 with
# `credentials_in: basic_header`) is handed a pre-encoded credential verbatim.
# Nothing here is a real secret and nothing reaches a real host.
DUMMY_TOKEN = "ZHVtbXktY3JlZGVudGlhbA=="

# Resolved once, at import: every connector on disk, whatever its status.
CONNECTORS = tuple(spec.available())
TOKEN_ENVS = tuple(sorted({s.token_env for s in spec.load_all()}))


def reset_run_state():
    """Drop everything cached for the length of a run.

    A minted OAuth token, a loaded extension module and whatever worklist that
    extension keeps are all process-scoped — which is one run under Airflow and
    the whole session under pytest. Leaving them in place makes a second test
    a continuation of the first.
    """
    auth.reset_token_cache()
    extensions.reset()


def isolate(monkeypatch, tmp_path):
    """Point a test's whole world at `tmp_path`.

    The dlt data directory matters most: it is where incremental cursors live,
    and a test advancing a real one would silently skip rows on the next real
    run.
    """
    for token_env in TOKEN_ENVS:
        monkeypatch.setenv(token_env, DUMMY_TOKEN)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    # dlt posts anonymous telemetry through the same transport the mock owns,
    # so leaving it on means unregistered-URL noise in every request history.
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    # duckdb writes its file relative to the cwd.
    monkeypatch.chdir(tmp_path)
    reset_run_state()


def load_into_duckdb(source_spec, resources=None, paced=None, extension=None):
    """Run a source into the local smoke destination. Returns the pipeline.

    One source at a time, in the order build_source returns them, because a
    worklist derived from the warehouse has to extract after its parents have
    landed — the same reason the CLI runs them in sequence rather than merging.
    """
    from ingest_runtime.runtime import build_source
    from ingest_runtime.warehouse import build_pipeline

    pipeline = build_pipeline(source_spec.name, destination="duckdb")
    for source in build_source(source_spec, selected=resources,
                               extension=extension, paced=paced):
        pipeline.run(source).raise_on_failed_jobs()
    return pipeline


def duckdb_path(source_spec, directory=None):
    """Where `--destination duckdb` puts this source's database.

    Namespaced by destination in build_pipeline, so a smoke run can never be
    confused with — or advance the state of — a production one.
    """
    base = Path(directory) if directory else Path.cwd()
    return base / f"{source_spec.name}_duckdb.duckdb"


def connect(source_spec, directory=None):
    return duckdb.connect(str(duckdb_path(source_spec, directory)))
