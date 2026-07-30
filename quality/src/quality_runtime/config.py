"""Warehouse connection details, read from the same env vars dlt uses.

One source of truth: whatever loaded the data is what we validate against, on
the host (localhost, published ports) and in a container (warehouse-db, internal
ports) alike.

ClickHouse has no schemas — a "schema" is a database. Each source lands in its
own `raw_<source>`, which comes from that source's spec rather than from here;
the only fixed name is ops. Everything qualifies its table names, so which
database the connection happens to select is immaterial.
"""

import os
from pathlib import Path

OPS_SCHEMA = "ops"

# Repo root as seen from this package: quality/src/quality_runtime -> repo root.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

_PREFIX = "DESTINATION__CLICKHOUSE__CREDENTIALS__"
_DEFAULTS = {
    "HOST": "localhost",
    # The native protocol port. Everything here talks HTTP instead, because
    # that is what clickhouse-connect and the SQLAlchemy dialect use.
    "PORT": "9001",
    "HTTP_PORT": "8124",
    "USERNAME": "warehouse",
    "PASSWORD": "warehouse",
    # Only ever the database the connection selects, never the one queried: every
    # table name here is qualified. `default` because it is the one database
    # ClickHouse always has — naming a source's would make the connection fail on
    # a stack where that source is not connected.
    "DATABASE": "default",
    "SECURE": "0",
}


class ConfigError(RuntimeError):
    pass


def _credential(name):
    value = os.environ.get(f"{_PREFIX}{name}", "").strip()
    return value or _DEFAULTS[name]


def connection_string():
    """SQLAlchemy URL for the warehouse.

    `clickhouse+http`, not the native protocol: Great Expectations reflects
    tables through SQLAlchemy, and the HTTP dialect is the one clickhouse-
    sqlalchemy supports best.

    Note this deliberately does NOT come from GX's `great-expectations
    [clickhouse]` extra, which cannot be installed: it pins sqlalchemy<2 while
    requiring clickhouse-sqlalchemy>=0.3, which requires sqlalchemy>=2. The
    dialect is a direct dependency instead.
    """
    from urllib.parse import quote_plus

    user = quote_plus(_credential("USERNAME"))
    password = quote_plus(_credential("PASSWORD"))
    host = _credential("HOST")
    port = _credential("HTTP_PORT")
    database = _credential("DATABASE")
    scheme = "clickhouse+https" if _credential("SECURE") == "1" else "clickhouse+http"
    return f"{scheme}://{user}:{password}@{host}:{port}/{database}"


def clickhouse_client_kwargs():
    """Arguments for clickhouse_connect.get_client(), used for the ops writes.

    The ops tables are written with plain INSERTs rather than through
    SQLAlchemy: they are append-only and the driver's own client is simpler and
    faster for that than reflecting a table it already knows the shape of.
    """
    return {
        "host": _credential("HOST"),
        "port": int(_credential("HTTP_PORT")),
        "username": _credential("USERNAME"),
        "password": _credential("PASSWORD"),
        "secure": _credential("SECURE") == "1",
    }


def freshness_hours(default=24):
    """The freshness SLO in hours.

    The source's spec declares it, which is where it belongs — how often a source
    changes is a fact about that source. GX_FRESHNESS_HOURS overrides it for
    every source at once, which is for the operator who wants one loose run
    without editing the contract, not for expressing the contract.
    """
    raw = os.environ.get("GX_FRESHNESS_HOURS", "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"GX_FRESHNESS_HOURS must be an integer, got {raw!r}") from exc


def docs_dir():
    override = os.environ.get("GX_DOCS_DIR", "").strip()
    if override:
        return Path(override)
    return PROJECT_ROOT / "quality" / "gx_output" / "data_docs"
