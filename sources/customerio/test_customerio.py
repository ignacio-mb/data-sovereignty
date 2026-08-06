"""What is true of Customer.io and of nothing else.

Three facts about this API decide whether the connector works, and all three
fail silently:

  the envelope key differs per endpoint  `campaigns`, `types`, and — for
      /v1/transactional — `messages`, which is also /v1/messages' key. The spec
      declares no `data_selector` at all and relies on dlt detecting the list
      per response. When that detection fails it does not raise: it lands the
      envelope itself as one junk row.

  every timestamp is a Unix INTEGER  the runtime's parser only handles strings
      and passes ints through, so the dlt column hint is the only thing turning
      these into timestamps. Drop a name from `hint_columns` and the column
      quietly lands as a BIGINT — a working pipeline with a useless column.

  `metrics` is a slug-keyed map  one key per tracked link, so its key space
      grows with every link in every email. Exploding it would mint a warehouse
      column per link.

The generic invariants — rows land, keys are unique, paging is walked — are in
pipeline/tests/test_connector_contract.py and are not repeated here.
"""

from __future__ import annotations

import datetime

import pytest
import spec_mock
from harness import connect, load_into_duckdb
from ingest_runtime import spec

RESOURCES = ["messages", "campaigns", "transactional", "object_types", "segments"]

# The epochs in the fixtures, as the integers Customer.io actually sends.
CREATED = 1785485066
OPENED = 1785485074


@pytest.fixture
def customerio():
    """The shipped spec, not a fixture copy — so drift fails this test."""
    return spec.load("customerio")


@pytest.fixture
def warehouse(customerio, tmp_path):
    with spec_mock.SpecServer(customerio) as server:
        load_into_duckdb(customerio, resources=RESOURCES)
        requested = server
    return connect(customerio, tmp_path), requested


def columns_of(connection, table):
    return {row[0] for row in connection.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw_customerio' AND table_name = ?", [table]).fetchall()}


class TestEnvelopeDetection:
    """Every endpoint's list must be found, whatever the envelope calls it."""

    def test_the_envelope_itself_never_lands_as_a_row(self, warehouse):
        """Detection failing does not raise — it yields the envelope dict as
        one record, which shows up as a column named after the envelope key."""
        connection, _ = warehouse
        for table, envelope in (("transactional", "messages"), ("object_types", "types")):
            columns = columns_of(connection, table)
            assert envelope not in columns, (
                f"raw_customerio.{table} has a column named {envelope!r}, which means "
                f"dlt landed the response envelope instead of the records inside it")
            assert "id" in columns

    @pytest.mark.parametrize("path", ["/v1/campaigns", "/v1/transactional", "/v1/object_types"])
    def test_an_endpoint_with_no_next_key_is_fetched_once(self, warehouse, path):
        """These responses carry no `next` at all. A paginator treating a
        missing cursor as "keep going" would loop on them forever."""
        _, server = warehouse
        assert len(server.calls(path, method="GET")) == 1

    def test_a_last_page_that_still_hands_back_a_cursor_is_followed(self, warehouse):
        """Customer.io returns `next` on the last page that carries rows and
        terminates with an EMPTY page instead — verified against the live API,
        and not what the docs imply. Stopping on the cursor rather than on the
        empty page would lose the final page of every paginated resource."""
        connection, server = warehouse
        landed = connection.execute("SELECT count(*) FROM raw_customerio.messages").fetchone()[0]
        calls = server.calls("/v1/messages", method="GET")
        assert landed == 3
        # Three records, two a page: two pages of rows plus the empty one that
        # is the only thing that can have stopped it.
        assert len(calls) == 3, [call.url for call in calls]


class TestEpochCoercion:
    """Customer.io sends integers; only the hint makes them timestamps."""

    def test_a_raw_epoch_column_lands_as_a_timestamp(self, warehouse):
        connection, _ = warehouse
        kind, value = connection.execute(
            "SELECT typeof(created), created FROM raw_customerio.messages "
            "WHERE id = 'm1'").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"created landed as {kind}, not a timestamp"
        assert value.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(CREATED, datetime.UTC)

    def test_a_promoted_metric_lands_as_a_timestamp(self, warehouse):
        """`opened_at` exists only because `promote` lifted `metrics.opened` and
        `hint_columns` named the result — neither alone is enough."""
        connection, _ = warehouse
        kind, value = connection.execute(
            "SELECT typeof(opened_at), opened_at FROM raw_customerio.messages "
            "WHERE id = 'm1'").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"opened_at landed as {kind}, not a timestamp"
        assert value.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(OPENED, datetime.UTC)

    def test_the_zero_sentinel_stays_an_integer(self, warehouse):
        """`scheduled_start` is 0 for "never", so it is deliberately NOT hinted:
        as a timestamp that sentinel reads as 1970-01-01 and quietly matches
        every `scheduled_start < now()` filter."""
        connection, _ = warehouse
        kind, value = connection.execute(
            "SELECT typeof(scheduled_start), scheduled_start "
            "FROM raw_customerio.campaigns WHERE id = 17").fetchone()
        assert "INT" in kind.upper(), f"scheduled_start landed as {kind}"
        assert value == 0


class TestUnboundedKeysStayContained:
    def test_the_metrics_map_stays_json_text(self, warehouse):
        """One key per tracked link, so exploding it would mint a warehouse
        column per link in every email."""
        connection, _ = warehouse
        kind, value = connection.execute(
            "SELECT typeof(metrics), metrics FROM raw_customerio.messages "
            "WHERE id = 'm1'").fetchone()
        assert "VARCHAR" in kind.upper(), f"metrics landed as {kind}, not JSON text"
        assert "link:4838" in value

    def test_no_column_is_minted_per_link(self, warehouse):
        connection, _ = warehouse
        columns = columns_of(connection, "messages")
        assert not [name for name in columns if name.startswith("link") or "link_" in name]

    def test_the_populated_identity_is_promoted(self, warehouse):
        """`customer_id` is null on real deliveries; `customer_identifiers`
        holds the identity that is actually there."""
        connection, _ = warehouse
        assert connection.execute(
            "SELECT cio_id, email FROM raw_customerio.messages "
            "WHERE id = 'm1'").fetchone() == ("cio1", "p1@example.test")


class TestSoftDeleteShape:
    """Deliveries must never be tombstoned; configuration must be able to be."""

    def test_deliveries_carry_no_tombstone_column(self, customerio, warehouse):
        connection, _ = warehouse
        assert "_deleted" not in columns_of(connection, "messages"), (
            "a run only ever sees the API's rolling 6-month window, so absence "
            "means the cap was hit, not that the delivery was deleted")
        assert "messages" not in customerio.tombstoned_tables

    def test_configuration_resources_are_tombstoneable(self, customerio, warehouse):
        connection, _ = warehouse
        deleted = connection.execute(
            "SELECT _deleted FROM raw_customerio.campaigns LIMIT 1").fetchone()[0]
        assert deleted is False, "present from the first load, so never null-typed"
        assert "campaigns" in customerio.soft_delete_tables
