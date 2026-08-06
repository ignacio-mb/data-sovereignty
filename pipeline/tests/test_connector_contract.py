"""What every connector must do, whoever wrote it and whatever it fetches.

This is the suite that makes "a connector is a spec" mean something. It runs
over EVERY directory in `sources/` — the reference example included, because an
example nothing exercises is how Pylon came to declare an extension module that
did not exist — and asserts the invariants that are the same for all of them:
the spec validates, the source builds into the shape the CLI walks, the rows
land, the merge key is unique, the paging is walked, the timeouts are set, the
budget is applied, and a second run changes nothing.

Every one of those is a failure that would otherwise be silent. A connector
that under-fetches, or hands back a bare resource, or sends `limit` where the
API wants `per-page`, still exits zero and still turns its DAG green.

What is NOT here is anything about a particular API. A behaviour only one
source has belongs in `sources/<name>/test_<name>.py`, beside the spec it is
about; the fixtures and the connector's own `fixtures/server.py` are what let
this file stay generic.
"""

from __future__ import annotations

import pytest
import spec_mock
from harness import DUMMY_TOKEN, TOKEN_ENVS, connect, load_into_duckdb, reset_run_state

from ingest_runtime import spec
from ingest_runtime.ingest.pacing import EndpointPacer
from ingest_runtime.runtime import build_source
from ingest_runtime.validate import ERROR, validate_all
from ingest_runtime.warehouse import table_counts

CONNECTORS = spec.available()


@pytest.mark.parametrize("name", CONNECTORS)
class TestEveryConnectorIsReviewable:
    """The checks that need no credential, no network and no warehouse.

    These are what a reviewer gets: before them, the load-bearing facts about a
    connector were provable only by running it against the live API.
    """

    def test_the_spec_loads_and_validates_without_errors(self, name):
        findings = validate_all(names=[name])
        errors = [str(f) for f in findings if f.level == ERROR]
        assert not errors, "\n".join(errors)

    def test_build_source_returns_sources_never_a_bare_resource(self, name):
        """`attach_samplers` and the run summary both walk `.resources`.

        A DltResource does not have it, so an extension returning one builds
        fine, runs fine under pipeline.run(), and dies on the first `--sample`.
        That is how it was found — against the live API rather than here.
        """
        built = build_source(spec.load(name))
        assert built, f"{name}: nothing was built"
        for source in built:
            assert hasattr(source, "resources"), f"{name}: {source!r} is not a source"


class Run:
    """One end-to-end load, and everything worth asserting about it."""

    def __init__(self, requests, timeouts, paced, counts):
        self.requests = requests
        self.timeouts = timeouts
        self.paced = paced
        self.counts = counts


def _load(source_spec, selected):
    """One run of the connector against its own fixtures.

    The pacer's sleeper is a list: the budgets are real (Pylon publishes
    10/min), and honouring them for real would make this suite take minutes to
    prove something that is about arithmetic.
    """
    paced = EndpointPacer(source_spec.rate_limits, sleeper=[].append)
    with spec_mock.SpecServer(source_spec) as server:
        pipeline = load_into_duckdb(source_spec, resources=list(selected), paced=paced)
        requests, timeouts = server.requests, server.timeouts
    return Run(requests, timeouts, paced, table_counts(pipeline, list(selected)))


@pytest.fixture(scope="module", params=CONNECTORS)
def loaded(request, tmp_path_factory):
    """Two consecutive runs of one connector, module-scoped.

    Twice, because merge-idempotency is only observable across runs, and
    because the second run is the one that exercises the incremental path: a
    connector with no cursor yet takes a different route through its own code
    than one resuming.

    Module-scoped so the load happens once per connector rather than once per
    assertion. That means building the isolation by hand — `monkeypatch` and
    `tmp_path` are function-scoped — which is what `pytest.MonkeyPatch.context`
    is for.
    """
    name = request.param
    source_spec = spec.load(name)
    selected = spec_mock.fixture_backed(source_spec)
    if not selected:
        pytest.skip(
            f"{name} ships no fixtures — add sources/{name}/fixtures/<resource>.json "
            f"(captured from live responses, redacted) and this connector is proved "
            f"offline like the others")

    home = tmp_path_factory.mktemp(f"contract_{name}")
    with pytest.MonkeyPatch.context() as patched:
        for token_env in TOKEN_ENVS:
            patched.setenv(token_env, DUMMY_TOKEN)
        patched.setenv("DLT_DATA_DIR", str(home / "dlt"))
        patched.setenv("DS_SCHEMA_DIR", str(home / "schemas"))
        patched.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
        patched.chdir(home)

        reset_run_state()
        first = _load(source_spec, selected)
        # A run is a fresh process under Airflow, so the caches that make one
        # run cheap — a minted token, an extension's worklist — do not survive
        # into the next. Resetting here is what makes the second run a second
        # RUN rather than a continuation of the first.
        reset_run_state()
        second = _load(source_spec, selected)

        connection = connect(source_spec, home)
        yield source_spec, selected, first, second, connection
        connection.close()


class TestEveryConnectorLoads:
    """The end-to-end contract, against the connector's own fixtures."""

    def test_every_fixture_backed_resource_lands_rows(self, loaded):
        source_spec, selected, first, _, _ = loaded
        empty = [name for name in selected if not first.counts.get(name)]
        assert not empty, (
            f"{source_spec.name}: {', '.join(empty)} have fixtures but landed no rows. "
            f"An empty table and a broken fetch look identical downstream.")

    def test_every_fixture_row_arrives_and_not_one_more(self, loaded):
        """The count, not merely "some rows".

        This is what a paginator stopping early actually looks like: the table
        exists, the DAG is green, and the last page is missing. The mock serves
        every collection in pages smaller than the fixture precisely so that
        one page is never the whole answer.
        """
        source_spec, selected, first, _, _ = loaded
        landed = {name: first.counts.get(name) for name in selected}
        expected = {name: len(spec_mock.fixture_rows(source_spec, name)) for name in selected}
        assert landed == expected

    def test_the_merge_key_is_unique_in_what_landed(self, loaded):
        """A duplicate primary key means the merge key broke — the one failure
        that silently corrupts an incremental load rather than stopping it."""
        source_spec, selected, _, _, connection = loaded
        for name in selected:
            key = source_spec.resource(name).primary_key
            columns = ", ".join([key] if isinstance(key, str) else key)
            duplicates = connection.execute(
                f"SELECT {columns} FROM {source_spec.dataset}.{name} "
                f"GROUP BY {columns} HAVING count(*) > 1").fetchall()
            assert not duplicates, f"{source_spec.name}.{name}: duplicate key(s) {duplicates}"

    def test_every_request_carried_a_timeout(self, loaded):
        """`requests` waits forever by default, and these tasks hold a pool of one.

        A hang is not a slow run: the slot is never released, every later run of
        that source queues behind it, and the only symptom is a task that never
        ends. Observed once, for 858 seconds, before the scheduler SIGKILLed it.
        """
        source_spec, _, first, _, _ = loaded
        assert first.timeouts, f"{source_spec.name}: no requests were sent"
        missing = [t for t in first.timeouts if t is None]
        assert not missing, (
            f"{source_spec.name}: {len(missing)}/{len(first.timeouts)} requests had no timeout")

    def test_the_declared_page_size_reached_the_wire(self, loaded):
        """A page-size parameter the API does not recognise is not an error.

        It silently serves the default instead — 20 rows a request where the
        spec asked for 200 — which against a paced budget is the difference
        between a backfill of hours and one of days. Swoogo pages on `per-page`
        and ignores `limit`, which is why the spelling is a spec key at all.
        """
        source_spec, selected, first, _, _ = loaded
        checked = 0
        for name in selected:
            for endpoint, _family in source_spec.resource(name).all_endpoints:
                size = endpoint.get("page_size")
                # A POST endpoint carries its paging in the body, where the
                # query string cannot see it.
                if not size or endpoint.get("method", "GET") != "GET":
                    continue
                param = endpoint.get("page_size_param", "limit")
                calls = [r for r in first.requests
                         if r.method == "GET"
                         and spec_mock.path_matches(source_spec, endpoint["path"], r.path)]
                assert calls, f"{source_spec.name}.{name}: {endpoint['path']} was never called"
                assert all(spec_mock.query(r).get(param) == [str(size)] for r in calls), (
                    f"{source_spec.name}.{name}: {param}={size} did not reach "
                    f"{endpoint['path']}")
                checked += 1
        if not checked:
            pytest.skip(f"{source_spec.name} declares no page size on a GET endpoint")

    def test_hinted_cursor_columns_land_as_timestamps(self, loaded):
        """A cursor dlt typed as text compares lexicographically.

        Which mostly works, and then does not — and the failure is a run that
        fetches the wrong slice rather than one that stops. `hint_columns` is
        what forbids the inference; this is what proves the hint took.
        """
        source_spec, selected, _, _, connection = loaded
        checked = 0
        for name in selected:
            landed_columns = {
                row[0]: row[1] for row in connection.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ?",
                    [source_spec.dataset, name]).fetchall()
            }
            for column in source_spec.resource(name).hint_columns:
                if column not in landed_columns:
                    continue
                assert "TIMESTAMP" in landed_columns[column].upper(), (
                    f"{source_spec.name}.{name}.{column} landed as "
                    f"{landed_columns[column]}, not a timestamp")
                checked += 1
        assert checked, f"{source_spec.name}: no hinted column was actually checked"

    def test_every_request_is_billed_to_a_declared_family(self, loaded):
        """A budget only applies to requests the router can attribute.

        `unmatched` is counted rather than dropped precisely so this is
        visible: a paginator following a link nobody declared spends a budget
        the summary still reports as respected.
        """
        source_spec, _, first, _, _ = loaded
        if not source_spec.rate_limits:
            pytest.skip(f"{source_spec.name} publishes no rate limits")
        assert first.paced.requests_made, f"{source_spec.name}: nothing was paced"
        assert "unmatched" not in first.paced.requests_made, dict(first.paced.requests_made)
        declared = {family for r in source_spec.resources for family in r.families}
        assert set(first.paced.requests_made) <= declared, dict(first.paced.requests_made)

    def test_a_second_run_changes_nothing(self, loaded):
        """Merge on the primary key, so a re-fetched row updates in place.

        Every connector here re-reads some overlap on purpose — a lookback, a
        full refresh of a small collection — and the overlap is only free if
        merging really is idempotent. Appending instead shows up as a row count
        that grows every hour.
        """
        source_spec, _, first, second, _ = loaded
        assert second.counts == first.counts, (
            f"{source_spec.name}: row counts moved between two identical runs "
            f"({first.counts} -> {second.counts})")
