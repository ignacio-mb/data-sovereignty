"""Warehouse connection details, read from the same env vars dlt uses.

The ingest side normally lets dlt resolve these itself. This module exists for
the one thing dlt cannot do: create the database it is about to connect to.
"""

import os

_PREFIX = "DESTINATION__CLICKHOUSE__CREDENTIALS__"
_DEFAULTS = {
    "HOST": "localhost",
    "HTTP_PORT": "8124",
    "USERNAME": "warehouse",
    "PASSWORD": "warehouse",
    "SECURE": "0",
}


def _credential(name):
    return os.environ.get(f"{_PREFIX}{name}", "").strip() or _DEFAULTS[name]


def clickhouse_admin_kwargs():
    """Arguments for clickhouse_connect.get_client(), with NO database selected.

    Deliberately omits `database`: this client's whole job is to create one, and
    naming a database that does not exist yet is how you get
    "Code: 81. Database raw_x does not exist" from the connection itself.
    """
    return {
        "host": _credential("HOST"),
        "port": int(_credential("HTTP_PORT")),
        "username": _credential("USERNAME"),
        "password": _credential("PASSWORD"),
        "secure": _credential("SECURE") == "1",
    }
