"""`dq` writes DDL and rows, so it needs the same locality guard `ingest` has.

For a while it did not. `ingest run` refused a loopback warehouse while `dq run`
next to it — same virtualenv, same DESTINATION__CLICKHOUSE__CREDENTIALS__HOST,
same `localhost` default — would happily CREATE DATABASE and INSERT into
whatever that address reached. With a tunnel holding the port, that is the
instance. The split is the dangerous part: guarding one of two twin commands
teaches the operator that host-side commands are guarded.
"""

import pytest
from click import exceptions as click_exceptions
from quality_runtime import cli as dq_cli


@pytest.fixture(autouse=True)
def no_override(monkeypatch):
    monkeypatch.delenv("DS_ALLOW_HOST_DQ", raising=False)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1", "0.0.0.0", ""])
def test_every_loopback_spelling_is_refused(monkeypatch, host):
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", host)
    with pytest.raises(click_exceptions.ClickException, match="refusing"):
        dq_cli._refuse_if_loopback("a test action")


def test_the_container_address_passes(monkeypatch):
    """Compose injects `warehouse-db`, so `make quality` is unaffected."""
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "warehouse-db")
    dq_cli._refuse_if_loopback("a test action")


def test_the_override_is_explicit(monkeypatch):
    monkeypatch.setenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", "localhost")
    monkeypatch.setenv("DS_ALLOW_HOST_DQ", "1")
    dq_cli._refuse_if_loopback("a test action")


def test_ingest_and_dq_share_one_implementation():
    """Two copies drift, and the drift is silent until someone gets burned."""
    from ingest_runtime import locality

    assert locality.refuse_loopback_warehouse.__module__ == "ingest_runtime.locality"
