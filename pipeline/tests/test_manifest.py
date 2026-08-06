"""One enumeration of the connectors, for everything that is not Python.

Seven readers used to glob `sources/` independently, and three of them
re-derived pool names or orchestration defaults in their own dialect — one in
an inline `python -c`, two in `grep -oE`. They agreed by luck and by review.
Now the spec parser writes this file and shell, compose and terraform read it.

Two properties matter and both are easy to lose. It must stay a projection
rather than a second copy of the contract — a manifest mirroring the spec is a
manifest people edit. And it must be regenerable byte-for-byte, because CI
proves it is current by rebuilding and diffing.
"""

from __future__ import annotations

import json

from ingest_runtime import manifest, spec

SPEC = """
name: {name}
status: {status}
owner: data-eng
api:
  base_url: https://{name}.test
  auth: {{type: bearer, token_env: {upper}_TOKEN}}
orchestration:
  schedule: "{minute} * * * *"
  reconcile: "0 4 * * 6"
  backfill_start: "2020-01-01"
  runtime: {{ingest: light, backfill: heavy}}
resources:
  - {{name: things, primary_key: id}}
"""


def write(directory, name, status="connected", minute=7):
    (directory / name).mkdir(parents=True, exist_ok=True)
    (directory / name / "source.yml").write_text(
        SPEC.format(name=name, status=status, upper=name.upper(), minute=minute))
    return spec.load(name, directory=directory)


class TestTheEntry:
    def test_it_carries_the_join_keys_a_shell_consumer_needs(self, tmp_path):
        entry = manifest.entry(write(tmp_path, "alpha"))
        assert entry["name"] == "alpha"
        assert entry["status"] == "connected"
        assert entry["path"] == "alpha/source.yml"
        assert entry["token_env"] == "ALPHA_TOKEN"
        assert entry["pool"] == "alpha_pipeline"
        assert entry["database"] == "raw_alpha"
        assert entry["owner"] == "data-eng"
        assert entry["resources"] == ["things"]

    def test_it_is_not_a_second_copy_of_the_contract(self, tmp_path):
        """A manifest mirroring the spec would be the file people edited, and
        the two would disagree the first time somebody did."""
        entry = manifest.entry(write(tmp_path, "alpha"))
        for key in ("api", "endpoints", "quality", "rate_limits", "pagination", "base_url"):
            assert key not in entry

    def test_the_dag_ids_are_the_spec_s_own_derivation(self, tmp_path):
        """One derivation, so the generator, the deploy verification and this
        cannot disagree about whether a reconcile DAG exists — which they did,
        in three places, each re-deciding it from the same two keys."""
        alpha = write(tmp_path, "alpha")
        assert manifest.entry(alpha)["dag_ids"] == list(alpha.dag_ids)
        assert manifest.entry(alpha)["dag_ids"] == [
            "alpha_ingest", "alpha_backfill", "alpha_reconcile"]

    def test_a_reference_spec_enumerates_no_dags(self, tmp_path):
        """It is read by the add-source skill and built by the contract suite,
        and scheduled by nothing — including by whatever consumes this file."""
        reference = write(tmp_path, "alpha", status="reference")
        entry = manifest.entry(reference)
        assert entry["status"] == "reference"
        assert entry["dag_ids"] == []

    def test_the_runtime_tier_is_resolved_per_task(self, tmp_path):
        """terraform sizes the ephemeral task from this, and a source whose
        hourly run is trivial and whose backfill is not needs both answers."""
        entry = manifest.entry(write(tmp_path, "alpha"))
        assert entry["runtime"] == {"ingest": "light", "backfill": "heavy",
                                    "reconcile": "standard"}


class TestRegeneration:
    def test_sources_are_sorted_so_two_pull_requests_merge(self, tmp_path):
        write(tmp_path, "zulu")
        write(tmp_path, "alpha", minute=9)
        assert [s["name"] for s in manifest.build(tmp_path)["sources"]] == ["alpha", "zulu"]

    def test_writing_twice_changes_nothing(self, tmp_path):
        write(tmp_path, "alpha")
        path, changed = manifest.write(tmp_path)
        assert changed and path.is_file()
        assert manifest.write(tmp_path)[1] is False

    def test_a_new_connector_makes_it_stale(self, tmp_path):
        write(tmp_path, "alpha")
        manifest.write(tmp_path)
        write(tmp_path, "beta", minute=9)
        assert manifest.load_manifest(tmp_path) != manifest.build(tmp_path)
        assert manifest.write(tmp_path)[1] is True

    def test_it_ends_in_a_newline(self, tmp_path):
        """One source per block with a trailing newline, so two connector pull
        requests touch different lines."""
        write(tmp_path, "alpha")
        path, _ = manifest.write(tmp_path)
        assert path.read_text().endswith("}\n")

    def test_an_absent_manifest_reads_as_none_rather_than_raising(self, tmp_path):
        assert manifest.load_manifest(tmp_path) is None

    def test_unreadable_json_reads_as_none_too(self, tmp_path):
        (tmp_path / "manifest.json").write_text("{ not json")
        assert manifest.load_manifest(tmp_path) is None


class TestTheCommittedManifest:
    """Committed rather than built on demand: terraform's `for_each` and the
    deploy's verification both need it before anything here is installed."""

    def test_it_matches_the_specs_on_disk(self):
        assert manifest.load_manifest() == manifest.build()

    def test_it_covers_every_connector_including_the_reference_one(self):
        names = [entry["name"] for entry in manifest.load_manifest()["sources"]]
        assert names == sorted(spec.available())

    def test_the_shape_is_versioned(self):
        """A consumer reading an older manifest should fail loudly rather than
        find a key missing."""
        stored = json.loads(manifest.manifest_path().read_text())
        assert stored["version"] == manifest.VERSION

    def test_every_connected_entry_names_a_credential(self):
        for entry in manifest.load_manifest()["sources"]:
            if entry["status"] == "connected":
                assert entry["token_env"], entry["name"]
