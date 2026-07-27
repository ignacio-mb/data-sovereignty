# pylon-pipeline

Pylon → Postgres ingestion, built on [dlt](https://dlthub.com) 1.x.

```
Pylon API ──ingest (merge on id)──▶ warehouse, schema raw_pylon
                                      issues, issue_messages,
                                      accounts, users, teams, contacts
```

Adapted from a production Pylon → ClickHouse pipeline. The API handling — cursor
pagination, per-endpoint rate pacing, HTML stripping, soft deletes — is
unchanged; the destination and dataset naming are not.

## Two fetch strategies, one table

**Incremental** (default, hourly). `POST /issues/search` filtered on `updated_at`
after the stored cursor, minus a one-hour lookback. Merge on `id` makes the
overlap idempotent. This is the steady-state path and it never re-scans history.

**Window** (`--start`, optional `--end`). `GET /issues` with `start_time` /
`end_time`. The API caps windows at 30 days and filters on **`created_at` only**,
so this answers "issues created in [start, end)" and will miss an old issue
updated inside the window. That constraint is why incremental mode exists — do
not collapse the two into one path.

`issue_messages` has no cursor. Its worklist is a warehouse watermark: issues
whose `latest_message_time` is newer than the newest message already loaded. The
worklist is passed in as a callable so it is evaluated at extract time, after the
issues from this same run have landed.

## Usage

```bash
uv run pylon ingest                                      # incremental
uv run pylon ingest --start 2026-01-01 --end 2026-02-01  # backfill a window
uv run pylon ingest --destination duckdb --sample 3      # local smoke test
```

While the stack is up, ingest through Airflow (`make ingest`) rather than
directly: a pool of one serializes runs, and two concurrent runs would race the
incremental cursor. The duckdb destination is always safe — it uses a separate
dlt pipeline name and cannot touch production state.

## Things that will bite you

- **`--mark-deleted` tombstones anything absent from the run's loads.** For
  `issues` it is only eligible when the run covered the full history
  (`--start <= 2019-01-01` and an end at roughly now); the guard lives in
  `cli.py`, and `test_incremental_run_never_soft_deletes_issues` protects it.
- **Directory resources merge rather than replace** on purpose. Replacing would
  delete the very rows the soft-delete pass needs to find.
- **A crashed run leaves a pending load package.** `pipeline.run(source)` would
  load *that* package and return without extracting, silently skipping the
  fetch. The CLI drains it first and excludes its load id from the soft-delete.
- **Nested JSON is stringified, not exploded.** `max_table_nesting=0` plus an
  explicit promotion list means a new Pylon custom field can never mint a
  surprise column or child table. Extract the hot ones in the `base_` transforms.

## Tests

`uv run pytest` from the repo root. Everything is mocked through
`requests-mock`; no network, no credentials. The end-to-end tests drive the real
Click CLI into duckdb inside a tmp directory.
