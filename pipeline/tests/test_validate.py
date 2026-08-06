"""The lints that make a connector reviewable without running it.

Before these, the load-bearing facts about a connector were provable only
against the live API: you wrote the spec, ran it with a real credential, and
found out. That is a slow loop for the author and a useless one for the
reviewer, who has neither the credential nor the appetite to load a warehouse
to approve a pull request.

Every check here is one that needs MORE than one file to know its answer, which
is why it cannot live in source.schema.json: two connectors colliding on a
database, an extension that does not answer for what its spec delegated, a
`status: connected` that nobody acknowledged in sources/CONNECTED.

Warnings do not fail. Errors do, and CI runs this before terraform ever reads a
spec — a bad connector should fail in a pull request, not in a plan.
"""

from __future__ import annotations

import json

from ingest_runtime import validate

SPEC = """
name: {name}
status: {status}
api:
  base_url: https://{name}.test
  auth: {{type: bearer, token_env: {token}}}
orchestration:
{orchestration}
resources:
  - {{name: things, primary_key: id}}
{resources}"""


def write(directory, name, status="reference", token=None, orchestration="  schedule: null",
          resources="", extra=""):
    (directory / name).mkdir(parents=True, exist_ok=True)
    (directory / name / "source.yml").write_text(
        SPEC.format(name=name, status=status, token=token or f"{name.upper()}_TOKEN",
                    orchestration=orchestration, resources=resources) + extra)
    return directory / name


def levels(findings, level):
    return [f for f in findings if f.level == level]


def messages(findings):
    return " | ".join(str(f) for f in findings)


class TestCollisions:
    """Two connectors that would write over each other."""

    def test_one_database_between_two_sources_is_an_error(self, tmp_path):
        """The database is derived from the name, so a collision means two
        directories claiming one identity — and one soft-delete pass covering
        both."""
        write(tmp_path, "alpha")
        # A second directory whose spec calls itself alpha cannot load at all,
        # which is the collision arriving one step earlier than the lint.
        beta = write(tmp_path, "beta")
        (beta / "source.yml").write_text((beta / "source.yml").read_text()
                                         .replace("name: beta", "name: alpha"))
        findings = validate.validate_all(directory=tmp_path)
        assert levels(findings, validate.ERROR), messages(findings)
        assert "alpha" in messages(findings)

    def test_a_shared_pool_is_an_error(self, tmp_path):
        """A pool is one slot: sharing it serialises connectors that have no
        reason to wait for each other, and makes one source's backfill block
        another's hourly run."""
        write(tmp_path, "alpha", orchestration="  pool: shared_pipeline")
        write(tmp_path, "beta", orchestration="  pool: shared_pipeline")
        findings = validate.validate_all(directory=tmp_path)
        assert any("share one pool" in str(f) for f in levels(findings, validate.ERROR)), \
            messages(findings)

    def test_two_sources_generating_one_dag_id_is_an_error(self, tmp_path):
        """Airflow registers DAGs by id, so the second one silently replaces
        the first and that source simply stops running."""
        write(tmp_path, "alpha", status="connected",
              orchestration="  schedule: '5 * * * *'")
        beta = write(tmp_path, "beta", status="connected",
                     orchestration="  schedule: '9 * * * *'")
        (beta / "source.yml").write_text((beta / "source.yml").read_text()
                                         .replace("name: beta", "name: alpha"))
        findings = validate.validate_all(directory=tmp_path)
        assert levels(findings, validate.ERROR), messages(findings)


class TestTheConnectedTripwire:
    """`status: connected` and sources/CONNECTED must agree.

    Written down twice on purpose: a connected spec schedules an unpaused DAG
    and demands its token on every clone, so it is a choice that gets
    acknowledged in a second place rather than reached by editing one word.
    """

    def test_a_connected_spec_missing_from_the_file_is_an_error(self, tmp_path):
        write(tmp_path, "alpha", status="connected")
        (tmp_path / "CONNECTED").write_text("# nothing here yet\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("absent from sources/CONNECTED" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_a_listed_name_with_no_connected_spec_is_an_error(self, tmp_path):
        """The other direction matters too: a line left behind after a spec was
        paused or deleted claims a connector nobody runs."""
        write(tmp_path, "alpha", status="paused")
        (tmp_path / "CONNECTED").write_text("alpha\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("is not a connected spec" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_agreement_is_clean(self, tmp_path):
        write(tmp_path, "alpha", status="connected")
        write(tmp_path, "beta", status="reference")
        (tmp_path / "CONNECTED").write_text("# comment\n\nalpha\n")
        assert not levels(validate.validate_all(directory=tmp_path), validate.ERROR)

    def test_comments_and_blanks_are_not_names(self, tmp_path):
        (tmp_path / "CONNECTED").write_text("# a comment\n\nalpha  # trailing\n")
        assert validate.declared_connected(tmp_path) == ["alpha"]

    def test_a_missing_file_is_only_a_problem_when_something_is_connected(self, tmp_path):
        write(tmp_path, "alpha", status="reference")
        assert not levels(validate.validate_all(directory=tmp_path), validate.ERROR)
        write(tmp_path, "beta", status="connected")
        assert levels(validate.validate_all(directory=tmp_path), validate.ERROR)

    def test_a_single_source_run_says_nothing_about_the_list(self, tmp_path):
        """`ingest validate --source x` knows nothing about the sources it did
        not load, so it must not report them as missing."""
        write(tmp_path, "alpha", status="connected")
        findings = validate.validate_all(directory=tmp_path, names=["alpha"])
        assert not any("CONNECTED" in str(f) for f in findings), messages(findings)


class TestTheExtensionAnswersForWhatItWasGiven:
    DELEGATED = (
        "  - name: children\n"
        "    primary_key: id\n"
        "    incremental: {strategy: parent_watermark, parent: things}\n"
    )

    def test_declaring_an_extension_without_writing_it_is_an_error(self, tmp_path):
        write(tmp_path, "alpha", extra="extensions: true\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("extension.py is missing" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_shipping_one_the_spec_does_not_declare_is_an_error(self, tmp_path):
        """Nothing would ever load it, so the connector runs as if the file did
        not exist — which is how a spec came to name a module that was never
        written without anything noticing."""
        directory = write(tmp_path, "alpha")
        (directory / "extension.py").write_text("")
        findings = validate.validate_all(directory=tmp_path)
        assert any("does not declare" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_a_delegated_resource_with_no_extension_at_all_is_an_error(self, tmp_path):
        write(tmp_path, "alpha", resources=self.DELEGATED)
        findings = validate.validate_all(directory=tmp_path)
        assert any("declares no extension" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_a_delegated_resource_with_no_builder_is_an_error(self, tmp_path):
        """Naming the strategy is not supplying it. A connector that quietly
        skips an endpoint looks exactly like one whose source has no data."""
        directory = write(tmp_path, "alpha", resources=self.DELEGATED, extra="extensions: true\n")
        (directory / "extension.py").write_text("def build_something_else():\n    pass\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("build_children()" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)

    def test_a_generic_builder_covers_every_delegated_resource(self, tmp_path):
        directory = write(tmp_path, "alpha", resources=self.DELEGATED, extra="extensions: true\n")
        (directory / "extension.py").write_text("def build_resource(spec, resource, paced=None):\n"
                                                "    return None\n")
        assert not levels(validate.validate_all(directory=tmp_path), validate.ERROR)

    def test_an_extension_that_does_not_import_is_reported_not_raised(self, tmp_path):
        """A lint that crashes on the file it is linting tells you less than
        one that reports it."""
        directory = write(tmp_path, "alpha", resources=self.DELEGATED, extra="extensions: true\n")
        (directory / "extension.py").write_text("import nonexistent_module_xyz\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("does not import" in str(f)
                   for f in levels(findings, validate.ERROR)), messages(findings)


class TestTheWarnings:
    """Hygiene: worth saying, not worth failing a pull request over."""

    def test_a_rate_limit_family_no_resource_routes_to_is_a_warning(self, tmp_path):
        """A budget for a family nothing uses is a budget that is never
        applied, while the run summary still reports it as published."""
        write(tmp_path, "alpha", extra="rate_limits:\n  ghost: 10\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("never applied" in str(f) for f in levels(findings, validate.WARN)), \
            messages(findings)
        assert not levels(findings, validate.ERROR)

    def test_two_connectors_on_the_same_minute_is_a_warning(self, tmp_path):
        """At four sources it is noise; at thirty it is the difference between
        a smooth hour and a thundering herd. The minutes used to be coordinated
        in YAML comments, which is a convention nothing enforced."""
        write(tmp_path, "alpha", orchestration="  schedule: '17 * * * *'")
        write(tmp_path, "beta", orchestration="  schedule: '17 */6 * * *'")
        findings = validate.validate_all(directory=tmp_path)
        assert any("minute :17" in str(f) for f in levels(findings, validate.WARN)), \
            messages(findings)

    def test_staggered_schedules_are_clean(self, tmp_path):
        write(tmp_path, "alpha", orchestration="  schedule: '17 * * * *'")
        write(tmp_path, "beta", orchestration="  schedule: '23 * * * *'")
        assert not levels(validate.validate_all(directory=tmp_path), validate.WARN)

    def test_a_connected_connector_with_no_fixtures_is_a_warning(self, tmp_path):
        """Without them the contract suite cannot prove the connector offline,
        so its only proof is a live credential nobody reviewing it has."""
        write(tmp_path, "alpha", status="connected",
              extra="quality:\n  required: [things]\n")
        (tmp_path / "CONNECTED").write_text("alpha\n")
        findings = validate.validate_all(directory=tmp_path)
        assert any("no fixtures" in str(f) for f in levels(findings, validate.WARN)), \
            messages(findings)

    def test_fixtures_for_the_required_tables_clear_it(self, tmp_path):
        directory = write(tmp_path, "alpha", status="connected",
                          extra="quality:\n  required: [things]\n")
        (directory / "fixtures").mkdir()
        (directory / "fixtures" / "things.json").write_text("[]")
        (tmp_path / "CONNECTED").write_text("alpha\n")
        assert not levels(validate.validate_all(directory=tmp_path), validate.WARN)


class TestTheManifestIsCurrent:
    def test_a_missing_manifest_is_an_error(self, tmp_path):
        write(tmp_path, "alpha")
        assert levels(validate.check_manifest(tmp_path), validate.ERROR)

    def test_a_stale_manifest_says_why_it_matters(self, tmp_path):
        """Shell, compose and terraform read it instead of parsing YAML, so a
        stale one means a pool that is never created or a task definition with
        no credential."""
        from ingest_runtime.manifest import write as write_manifest

        write(tmp_path, "alpha")
        write_manifest(tmp_path)
        write(tmp_path, "beta")
        findings = validate.check_manifest(tmp_path)
        assert any("stale" in str(f) for f in levels(findings, validate.ERROR)), \
            messages(findings)

    def test_a_current_manifest_is_clean(self, tmp_path):
        from ingest_runtime.manifest import write as write_manifest

        write(tmp_path, "alpha")
        write_manifest(tmp_path)
        assert validate.check_manifest(tmp_path) == []

    def test_unreadable_json_counts_as_missing_rather_than_crashing(self, tmp_path):
        write(tmp_path, "alpha")
        (tmp_path / "manifest.json").write_text("{not json")
        assert levels(validate.check_manifest(tmp_path), validate.ERROR)


class TestTheShippedDirectory:
    """The real sources/, which is the one that actually schedules things."""

    def test_this_checkout_validates_clean(self):
        findings = validate.validate_all()
        assert not levels(findings, validate.ERROR), messages(findings)

    def test_the_committed_manifest_is_current(self):
        findings = validate.check_manifest()
        assert not findings, messages(findings)

    def test_the_connected_file_and_the_specs_agree(self):
        from ingest_runtime.spec import connected

        assert sorted(s.name for s in connected()) == sorted(validate.declared_connected())


def test_worst_level_is_the_exit_code_in_one_word():
    error = validate.Finding(validate.ERROR, "a", "boom")
    warn = validate.Finding(validate.WARN, "a", "hmm")
    assert validate.worst_level([]) is None
    assert validate.worst_level([warn]) == validate.WARN
    assert validate.worst_level([warn, error]) == validate.ERROR


def test_spec_paths_point_at_the_files_on_disk():
    for path in validate.spec_paths():
        assert path.is_file(), path
        assert path.name == "source.yml"


def test_the_manifest_shape_is_versioned(tmp_path):
    """A consumer reading an older manifest should fail loudly rather than find
    a key missing."""
    from ingest_runtime.manifest import manifest_path

    assert json.loads(manifest_path().read_text())["version"] >= 1
