"""The skeleton a new connector starts from.

Small on purpose. A template that pre-fills plausible endpoints teaches an agent
to keep them, and a connector whose spec was half-guessed is worse than one that
was empty — it looks finished. What is filled in here is only what is true of
every connector: where the schema is, that the credential is named and never
written, and that nothing schedules until somebody says so.
"""

from __future__ import annotations

from pathlib import Path

from .spec import SPEC_FILENAME, sources_dir

SPEC_TEMPLATE = '''# yaml-language-server: $schema=../source.schema.json
#
# {name} — one-line description of what this API is and why we ingest it.
#
# Write down what the research turned up that the spec cannot hold: the
# pagination that lies, the field the docs omit, the region that answers 301.
# README.md beside this file is the place for the long version.

name: {name}
# `reference` until this has been proven to load. Flipping to `connected` is what
# schedules an unpaused DAG and demands {token_env} on every clone of this repo —
# add the name to sources/CONNECTED in the same commit.
status: reference
display_name: {name}
docs_url: https://example.com/api-docs
owner: data-eng

api:
  base_url: https://api.example.com
  auth:
    # bearer | api_key | http_basic | oauth2_client_credentials | extension
    type: bearer
    # The NAME of the variable, never the value. This is what lets the spec live
    # in a public repo.
    token_env: {token_env}

# Requests per minute per endpoint family, from the API's published limits.
# Researched, not discovered by being 429'd: the pacer spaces requests evenly.
rate_limits:
  {name}: 60

orchestration:
  schedule: null                # cron once connected; pick a minute nobody else uses
  pool: {name}_pipeline         # one slot, so runs cannot race the cursor
  runtime: standard             # light | standard | long | heavy
  timeouts_minutes:
    ingest: 55

pagination:
  # `cursor`, a dlt paginator name, or a raw dlt paginator config. Leave the key
  # out entirely to let dlt detect it.
  kind: cursor
  cursor_path: meta.next_cursor
  cursor_param: cursor
  data_selector: data

resources:
  - name: things
    primary_key: id
    write_disposition: merge
    endpoint:
      path: /things
      method: GET
      page_size: 100
    # `cursor` pushes the high-water mark into the API's own filter, so the
    # server sends only what changed. Drop the block for a small collection that
    # is cheap to re-read in full.
    incremental:
      strategy: cursor
      cursor_field: updated_at
      cursor_param: updated_since
      lookback_seconds: 3600
    timestamp_columns: [created_at, updated_at]

quality:
  # Identity checks — primary key not null, unique, at least one row — are
  # generated for every resource and are not declared here.
  required: [things]
  freshness:
    table: things
    column: updated_at
    hours: 24
    severity: warn
'''

README_TEMPLATE = """# {name}

What the research found. This file is the durable half of the add-source
conversation — the half that used to live in a chat log and be lost.

## Auth
How the credential is obtained, which variable holds it, and anything about
scopes or regions that took a wrong turn to discover.

## Pagination
The envelope, the terminating condition, and any way the API lies about having
more pages.

## Incremental
Which field actually moves when a record changes, and which endpoint filters on
it. Note where the documented behaviour and the observed behaviour differ.

## Rate limits
The published numbers, plus what was measured if they disagree.

## Deletions
Whether the API reports them at all, and how a deleted record is recognised.

## Fixtures
Captured from live responses on <date>, redacted. Re-capture when the API's
shape changes; the offline suite is only as honest as these are.
"""


def create(name, directory=None):
    """Create sources/<name>/ with a spec, a research note and a fixtures dir."""
    base = Path(directory) if directory else sources_dir()
    target = base / name
    if target.exists():
        raise FileExistsError(f"{target} already exists")

    token_env = f"{name.upper()}_API_KEY"
    (target / "fixtures").mkdir(parents=True)
    spec_path = target / SPEC_FILENAME
    spec_path.write_text(SPEC_TEMPLATE.format(name=name, token_env=token_env))
    readme_path = target / "README.md"
    readme_path.write_text(README_TEMPLATE.format(name=name))
    return {"dir": target, "files": [spec_path, readme_path, target / "fixtures"]}
