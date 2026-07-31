"""Customer.io: the shipped spec, driven end to end into duckdb.

This runs the REAL `sources/customerio.yml` rather than a fixture copy, because
the two things most likely to break this connector are properties of that file
and would both fail silently:

  the envelope key differs per endpoint  Customer.io returns `{"campaigns": …}`,
      `{"types": …}`, and — for /v1/transactional — `{"messages": …}`, which is
      also /v1/messages' key. The runtime has a single source-wide
      `data_selector`, so the spec declares none and relies on dlt detecting the
      list per response. If that detection ever stops working, the resource
      lands the envelope as one junk row instead of raising.

  every timestamp is a Unix INTEGER  `_parse_ts` only parses strings and passes
      ints straight through, so the dlt column hint is the only thing turning
      these into timestamps. Drop a name from `hint_columns` and the column
      quietly lands as a BIGINT — a working pipeline with a useless column.

The mock implements Customer.io's actual paging, including the two behaviours
that decide whether the paginator terminates: a `next` token handed back on the
last page that still carries rows, and an EMPTY page to finish.
"""

import datetime

import duckdb
import pytest
import requests_mock as rm_module

from ingest_runtime import runtime, spec
from ingest_runtime.warehouse import build_pipeline

BASE = "https://api.customer.io"

# Real epochs from the live API, kept as the integers Customer.io actually sends.
CREATED = 1785485066
OPENED = 1785485074


def message(n, **over):
    """One delivery, shaped like the live payload."""
    record = {
        "id": f"m{n}",
        "deduplicate_id": f"m{n}:{CREATED}",
        "customer_id": None,                     # null on most real deliveries
        "customer_identifiers": {"cio_id": f"cio{n}", "email": f"p{n}@example.com"},
        "recipient": f"p{n}@example.com",
        "subject": f"Subject {n}",
        "type": "email",
        "created": CREATED,
        "campaign_id": 17,
        "newsletter_id": None,
        "transactional_message_id": None,
        # A slug-keyed map: the `link:` entries are one per tracked link, which is
        # why this must stay JSON text rather than being exploded into columns.
        "metrics": {"sent": CREATED, "delivered": CREATED, "opened": OPENED,
                    "link:4838": OPENED, "bot_link:4838": OPENED},
    }
    record.update(over)
    return record


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("CUSTOMERIO_API_KEY_ACTIVE_CUSTOMER", "secret")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("DS_SCHEMA_DIR", str(tmp_path / "schemas"))
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def customerio():
    return spec.load("customerio")


def serve(mock):
    """Customer.io's real paging, and its real per-endpoint envelope keys."""
    # Three pages, and page 2 is the one that matters: it carries a row AND a
    # `next`, exactly as the live API does on its last page of data. Only the
    # empty page 3 stops the paginator.
    pages = {
        None: {"messages": [message(1), message(2)], "next": "c2"},
        # Newsletter sends are the overwhelming majority of real deliveries
        # (102,590 of 104,551 in the live window), and they carry no campaign_id.
        "c2": {"messages": [message(3, campaign_id=None, newsletter_id=4)], "next": "c3"},
        "c3": {"messages": [], "next": ""},
    }

    def messages(request, context):
        return pages[request.qs.get("start", [None])[0]]

    mock.get(f"{BASE}/v1/messages", json=messages)
    # Envelope key == the resource name, and no `next` key at all: an
    # unpaginated endpoint must stop after one request rather than loop.
    mock.get(f"{BASE}/v1/campaigns", json={"campaigns": [
        {"id": 17, "name": "Let Starter customers try Pro", "state": "running",
         "created": CREATED, "updated": CREATED, "scheduled_start": 0,
         "actions": [{"id": 3, "type": "email"}], "tags": ["Sample"]}]})
    # Envelope key `messages`, not `transactional` — the collision that makes a
    # single source-wide data_selector impossible.
    mock.get(f"{BASE}/v1/transactional", json={"messages": [
        {"id": 1, "name": "All uncategorized email messages",
         "created_at": CREATED, "updated_at": CREATED}]})
    # Envelope key `types`, matching neither the path nor the resource name.
    mock.get(f"{BASE}/v1/object_types", json={"types": [
        {"id": "ot1", "name": "Company", "slug": "company", "enabled": True}]})


def load(customerio, resources):
    with rm_module.Mocker() as mock:
        serve(mock)
        pipeline = build_pipeline("customerio", destination="duckdb")
        for source in runtime.build_source(customerio, selected=resources):
            pipeline.run(source).raise_on_failed_jobs()
        # Only Customer.io traffic. dlt posts its own telemetry through the same
        # mocked adapter, so `mock.call_count` would count that too.
        return [r.url for r in mock.request_history if r.url.startswith(BASE)]


@pytest.fixture
def warehouse(customerio, tmp_path):
    requested = load(customerio, ["messages", "campaigns", "transactional", "object_types"])
    con = duckdb.connect(str(tmp_path / "customerio_duckdb.duckdb"))
    return con, requested


class TestEnvelopeDetection:
    """Every endpoint's list must be found, whatever the envelope calls it."""

    def test_each_resource_lands_its_own_records(self, warehouse):
        con, _ = warehouse
        counts = {
            table: con.execute(f"SELECT count(*) FROM raw_customerio.{table}").fetchone()[0]
            for table in ("messages", "campaigns", "transactional", "object_types")
        }
        assert counts == {"messages": 3, "campaigns": 1, "transactional": 1, "object_types": 1}

    def test_the_envelope_itself_never_lands_as_a_row(self, warehouse):
        """Detection failing does not raise — it yields the envelope dict as one
        record, which shows up as a column named after the envelope key."""
        con, _ = warehouse
        for table, envelope in (("transactional", "messages"), ("object_types", "types")):
            columns = {
                row[0] for row in con.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{table}'").fetchall()
            }
            assert envelope not in columns, (
                f"raw_customerio.{table} has a column named {envelope!r}, which means "
                f"dlt landed the response envelope instead of the records inside it")
            assert "id" in columns


class TestPagination:
    def test_every_page_lands_and_the_empty_page_stops_it(self, warehouse):
        con, requested = warehouse
        ids = [r[0] for r in con.execute(
            "SELECT id FROM raw_customerio.messages ORDER BY id").fetchall()]
        assert ids == ["m1", "m2", "m3"], "all three pages of records must land"
        # Page 2 carries both a row and a `next`, so only the empty page 3 can
        # have stopped it — which is the live API's actual shape.
        assert len([u for u in requested if "/v1/messages" in u]) == 3

    @pytest.mark.parametrize("path", ["/v1/campaigns", "/v1/transactional", "/v1/object_types"])
    def test_an_endpoint_with_no_next_key_is_fetched_once(self, warehouse, path):
        """These responses have no `next` at all. A paginator that treated a
        missing cursor as "keep going" would loop on them forever."""
        _, requested = warehouse
        assert len([u for u in requested if path in u]) == 1


class TestEpochCoercion:
    """Customer.io sends integers; only the hint makes them timestamps."""

    def test_a_raw_epoch_column_lands_as_a_timestamp(self, warehouse):
        con, _ = warehouse
        kind, value = con.execute(
            "SELECT typeof(created), created FROM raw_customerio.messages LIMIT 1").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"created landed as {kind}, not a timestamp"
        assert value.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(CREATED, datetime.UTC)

    def test_a_promoted_metric_lands_as_a_timestamp(self, warehouse):
        """`opened_at` exists only because `promote` lifted `metrics.opened` and
        `hint_columns` named the result — neither alone is enough."""
        con, _ = warehouse
        kind, value = con.execute(
            "SELECT typeof(opened_at), opened_at FROM raw_customerio.messages LIMIT 1").fetchone()
        assert "TIMESTAMP" in kind.upper(), f"opened_at landed as {kind}, not a timestamp"
        assert value.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(OPENED, datetime.UTC)

    def test_the_zero_sentinel_stays_an_integer(self, warehouse):
        """`scheduled_start` is 0 for "never", so it is deliberately NOT hinted:
        as a timestamp that sentinel would read as 1970-01-01 and quietly match
        every `scheduled_start < now()` filter."""
        con, _ = warehouse
        kind, value = con.execute(
            "SELECT typeof(scheduled_start), scheduled_start "
            "FROM raw_customerio.campaigns LIMIT 1").fetchone()
        assert "INT" in kind.upper(), f"scheduled_start landed as {kind}"
        assert value == 0


class TestUnboundedKeysStayContained:
    def test_the_metrics_map_stays_json_text(self, warehouse):
        """One key per tracked link, so exploding it would mint a warehouse
        column per link in every email."""
        con, _ = warehouse
        kind, value = con.execute(
            "SELECT typeof(metrics), metrics FROM raw_customerio.messages LIMIT 1").fetchone()
        assert "VARCHAR" in kind.upper(), f"metrics landed as {kind}, not JSON text"
        assert "link:4838" in value

    def test_no_column_is_minted_per_link(self, warehouse):
        con, _ = warehouse
        columns = {row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'messages'").fetchall()}
        assert not [c for c in columns if "link_" in c or c.startswith("link")]

    def test_the_populated_identity_is_promoted(self, warehouse):
        """`customer_id` is null on real deliveries; `customer_identifiers` holds
        the identity that is actually there."""
        con, _ = warehouse
        cio_id, email = con.execute(
            "SELECT cio_id, email FROM raw_customerio.messages ORDER BY id LIMIT 1").fetchone()
        assert (cio_id, email) == ("cio1", "p1@example.com")


class TestSoftDeleteShape:
    """Deliveries must never be tombstoned; configuration must be able to be."""

    def test_deliveries_carry_no_tombstone_column(self, customerio, warehouse):
        con, _ = warehouse
        columns = {row[0] for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'messages'").fetchall()}
        assert "_deleted" not in columns, (
            "a run only ever sees the API's rolling 6-month window, so absence "
            "means the cap was hit, not that the delivery was deleted")
        assert "messages" not in customerio.tombstoned_tables

    def test_configuration_resources_are_tombstoneable(self, customerio, warehouse):
        con, _ = warehouse
        deleted = con.execute(
            "SELECT _deleted FROM raw_customerio.campaigns LIMIT 1").fetchone()[0]
        assert deleted is False, "present from the first load, so never null-typed"
        assert "campaigns" in customerio.soft_delete_tables
