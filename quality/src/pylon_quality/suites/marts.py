"""Expectations for the modeled tables, generated from the transform manifest.

The manifest is the single contract: `mbx transforms` reads it to build the
tables, and this module reads the same file to check them. Declaring a grain in
one place and testing it in another is how the two quietly stop agreeing.
"""

import logging

import yaml
from great_expectations import expectations as gxe

from ..config import ANALYTICS_SCHEMA, MANIFEST_PATH

log = logging.getLogger(__name__)


def load_manifest(path=None):
    path = path or MANIFEST_PATH
    if not path.exists():
        log.warning("no transform manifest at %s — nothing to validate yet", path)
        return {"schema": ANALYTICS_SCHEMA, "transforms": []}
    parsed = yaml.safe_load(path.read_text()) or {}
    parsed.setdefault("schema", ANALYTICS_SCHEMA)
    parsed.setdefault("transforms", [])
    return parsed


def _grain(columns):
    """A compound grain has to be checked as a whole; a single column can use
    the native uniqueness expectation, which reports the offending values."""
    if len(columns) == 1:
        return gxe.ExpectColumnValuesToBeUnique(column=columns[0])
    joined = ", ".join(columns)
    return gxe.UnexpectedRowsExpectation(
        unexpected_rows_query=(
            f"SELECT {joined} FROM {{batch}} GROUP BY {joined} HAVING count(*) > 1"
        ),
        description=f"grain ({joined}) is unique",
    )


def build(manifest=None):
    """{(schema, table): [Expectation]} for every transform in the manifest."""
    manifest = manifest or load_manifest()
    schema = manifest["schema"]
    suites = {}

    for transform in manifest["transforms"]:
        name = transform["name"]
        expectations = [gxe.ExpectTableRowCountToBeBetween(
            min_value=1,
            description=f"{name} is not empty (an empty mart means the transform silently produced nothing)",
        )]

        grain = transform.get("grain") or []
        if grain:
            expectations.append(_grain(grain))
            expectations += [gxe.ExpectColumnValuesToNotBeNull(column=column) for column in grain]
        else:
            log.warning("transform %s declares no grain — skipping the uniqueness check", name)

        expectations += [
            gxe.ExpectColumnValuesToNotBeNull(column=column)
            for column in (transform.get("not_null") or [])
            if column not in grain
        ]

        # Reconciliation identities: SQL that must return zero rows. This is
        # where "the waterfall adds up" and "the splits sum to the total" live.
        expectations += [
            gxe.UnexpectedRowsExpectation(
                unexpected_rows_query=check["query"],
                description=check.get("description", f"{name} reconciles"),
            )
            for check in (transform.get("reconciliation") or [])
        ]

        suites[(schema, name)] = expectations

    return suites
