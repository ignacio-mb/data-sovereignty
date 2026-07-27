"""Names and thresholds shared by the mbx subcommands."""

import os
from pathlib import Path

# metabase/src/mb_tools -> repo root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
METABASE_DIR = PROJECT_ROOT / "metabase"
MANIFEST_PATH = METABASE_DIR / "transforms" / "manifest.yml"
SQL_DIR = METABASE_DIR / "transforms" / "sql"
# Local name -> id registries. Gitignored: rebuildable from the instance by name.
STATE_DIR = METABASE_DIR / ".state"

MIN_METABASE_VERSION = 63

# Exact names as reported by `mb setting get token-features`. Note it is
# "transforms-basic", not "transforms" — "transforms-python" is a separate
# feature we do not need, and guessing "transforms" makes the audit fail on a
# perfectly good license.
#   transforms-basic — the modeling layer itself
#   remote_sync      — git version control of Metabase content
#   library          — the publish/trust boundary for the semantic layer
REQUIRED_TOKEN_FEATURES = {"transforms-basic", "remote_sync", "library"}

# How the warehouse connection is named inside Metabase. bootstrap_metabase.sh
# creates it under this name; the audit matches on it.
WAREHOUSE_DB_NAME = os.environ.get("MB_WAREHOUSE_DB_NAME", "Warehouse")

TRANSFORMS_COLLECTION = "Pylon transforms"
ANALYTICS_SCHEMA = "analytics"

# Every transform this repo owns carries this tag, so a transform-job can
# refresh exactly our set and nothing a human added by hand.
TRANSFORM_TAG = "pylon"


def docs_path(name):
    return PROJECT_ROOT / "docs" / name


def state_path(name):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / name
