"""The fixtures every connector test shares.

Three things have to be true of any test here, and none are worth restating per
file: the run must not touch the real dlt state directory, auth must never
raise for want of a credential, and the caches that make one RUN cheap must not
leak into the next test. `harness.isolate` is all three; this makes it
automatic.

`sources/conftest.py` re-exports these, which is what lets a connector's own
tests live beside its spec and still use the same harness.
"""

from __future__ import annotations

import pytest
from harness import connect, isolate, load_into_duckdb, reset_run_state

from ingest_runtime import spec


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    isolate(monkeypatch, tmp_path)
    yield
    reset_run_state()


@pytest.fixture
def all_specs():
    """Every connector in the real sources/ directory, all statuses."""
    return spec.load_all()


@pytest.fixture
def connector_spec():
    """`connector_spec("swoogo")` — the shipped spec, never a fixture copy.

    Deliberately the real file: a test against a copy proves the copy.
    """
    return spec.load


@pytest.fixture
def landed(tmp_path):
    """`landed(spec, resources)` — load into duckdb and hand back a connection."""
    def run(source_spec, resources=None, paced=None, extension=None):
        load_into_duckdb(source_spec, resources=resources, paced=paced, extension=extension)
        return connect(source_spec, tmp_path)

    return run
