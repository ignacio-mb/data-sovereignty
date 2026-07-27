"""Warehouse connection details, read from the same env vars dlt uses.

One source of truth: whatever loaded the data is what we validate against, on
the host (localhost:5434) and in a container (warehouse-db:5432) alike.
"""

import os
from pathlib import Path

RAW_SCHEMA = "raw_pylon"
ANALYTICS_SCHEMA = "analytics"
OPS_SCHEMA = "ops"

# Repo root as seen from this package: quality/src/pylon_quality -> repo root.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = PROJECT_ROOT / "metabase" / "transforms" / "manifest.yml"

_PREFIX = "DESTINATION__POSTGRES__CREDENTIALS__"
_DEFAULTS = {
    "HOST": "localhost",
    "PORT": "5434",
    "USERNAME": "warehouse",
    "PASSWORD": "warehouse",
    "DATABASE": "warehouse",
}


class ConfigError(RuntimeError):
    pass


def _credential(name):
    value = os.environ.get(f"{_PREFIX}{name}", "").strip()
    return value or _DEFAULTS[name]


def connection_string():
    """SQLAlchemy URL for the warehouse."""
    from urllib.parse import quote_plus

    user = quote_plus(_credential("USERNAME"))
    password = quote_plus(_credential("PASSWORD"))
    host = _credential("HOST")
    port = _credential("PORT")
    database = _credential("DATABASE")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


def psycopg_dsn():
    """libpq DSN for the direct psycopg writes into the ops schema."""
    return (
        f"host={_credential('HOST')} port={_credential('PORT')} "
        f"user={_credential('USERNAME')} password={_credential('PASSWORD')} "
        f"dbname={_credential('DATABASE')}"
    )


def freshness_hours():
    raw = os.environ.get("GX_FRESHNESS_HOURS", "24").strip() or "24"
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"GX_FRESHNESS_HOURS must be an integer, got {raw!r}") from exc


def docs_dir():
    override = os.environ.get("GX_DOCS_DIR", "").strip()
    if override:
        return Path(override)
    return PROJECT_ROOT / "quality" / "gx_output" / "data_docs"
