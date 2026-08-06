"""Let a connector's own tests use the shared offline harness.

A connector is a directory, and the tests for the part of it that could not be
declared belong beside the thing they test — `sources/pylon/test_pylon.py` sits
next to the extension whose semantics it pins. The harness those tests need is
not per connector, though: isolating the environment, resetting the run-scoped
caches and driving a spec-shaped mock server are the same job everywhere, so
they live once, in pipeline/tests.

This puts that directory on the import path and re-exports its fixtures. Loaded
by path under an explicit name rather than imported, because pytest has already
claimed the module name `conftest` for THIS file.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HARNESS_DIR = Path(__file__).resolve().parents[1] / "pipeline" / "tests"
if str(HARNESS_DIR) not in sys.path:
    sys.path.insert(0, str(HARNESS_DIR))

_MODULE = "ds_shared_fixtures"
if _MODULE not in sys.modules:
    _spec = importlib.util.spec_from_file_location(_MODULE, HARNESS_DIR / "conftest.py")
    _loaded = importlib.util.module_from_spec(_spec)
    sys.modules[_MODULE] = _loaded
    _spec.loader.exec_module(_loaded)
_shared = sys.modules[_MODULE]

# `isolated` is autouse where it is defined and stays autouse here — a
# connector test that forgot it would write into the real dlt state directory
# and advance a production cursor.
isolated = _shared.isolated
all_specs = _shared.all_specs
connector_spec = _shared.connector_spec
landed = _shared.landed
