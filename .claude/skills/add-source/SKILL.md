---
name: add-source
description: Connect a new API to the warehouse — research it, agree the sync semantics, generate the connector, prove it loads. Triggers — "also ingest Zendesk", "add Salesforce data", "connect our Stripe account", "pull from the GitHub API", "I want another source", "how do I add a new connector?", "set up a new sync".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch, AskUserQuestion
---

# Connecting a source

A connector is a directory. `sources/<name>/source.yml` says what the API is, how
it pages, what is incremental, when it runs, and what "correct" means for the
tables it lands; the runtime does the rest. Beside the spec live the things a
contract cannot hold — `extension.py`, `fixtures/`, `README.md`. Most connectors
need no Python at all.

Start it with the skeleton rather than a blank file:

```bash
uv run ingest scaffold <name>
```

That writes `sources/<name>/source.yml` as **`status: reference`**, plus a
research README and an empty `fixtures/`. Reference means validated and built by
the test suite, and scheduled by nothing — a new connector should not begin life
demanding a credential nobody has pushed. Flipping it to `connected` is the last
step of phase 4, not the first of phase 3.

Four phases: **research the API, agree what only a human can decide, generate the
connector, prove it loads.** Do not skip to generating — the questions in phase 2
are the ones that silently lose rows when guessed.

## Phase 1 — Research the API

Find these out yourself. Asking a user for a rate limit they would have to go
look up is asking them to do your job.

| What | Where it usually is | Goes into |
|---|---|---|
| Base URL, auth scheme | "Authentication" / "Getting started" | `api.base_url`, `api.auth` |
| Endpoints and their shapes | the reference, or an OpenAPI/Swagger doc | `resources[].endpoint` |
| Pagination style | "Pagination" — cursor, offset, page number, link header | `pagination` |
| Page-size cap | often one sentence, often different per endpoint | `endpoint.page_size` |
| **Rate limits** | "Rate limits" / "Limits" — frequently per endpoint family | `rate_limits` |
| Which fields are timestamps | the response schema | `timestamp_columns` |
| Whether deletes are visible | "Deletions", or conspicuous silence | `soft_delete` |

Start with `WebSearch` for `<api> API documentation rate limits`, then `WebFetch`
the pages that matter. **If the API publishes an OpenAPI spec, fetch it** —
endpoints, pagination and field types come straight out of it.

Report what you found and, explicitly, what you could not. **Never invent a rate
limit.** A guessed budget gets discovered by being 429'd in production, usually
mid-backfill. If the docs are silent, say so and ask — that is the one limit
question worth a user's time.

Check the pagination against what the runtime can express. `cursor` and
`single_page` are shorthands; every other style — page number, offset, link header
— is written as a raw dlt paginator config dict under `pagination.kind`, which is
passed through untouched. See "The vocabulary the loader accepts" below. Only a
scheme dlt itself cannot page needs an extension, and it is much cheaper to know
that now than in phase 3.

## Phase 2 — Ask what research cannot answer

Use `AskUserQuestion`. These are decisions, not facts:

1. **Schedule and backfill window.** How often to sync, and how far back the
   first load reaches. Backfill cost is the window times the rate limit — if that
   is hours, say so before they agree to it, not after.

2. **The incremental strategy, per endpoint.** The one that silently loses data.
   Establish *which field is the cursor* and *which field the API filters on*.
   They are often not the same. If the list endpoint filters on creation time
   only, an incremental sync will never see an old record updated today — that
   needs a second endpoint filtering on update time, which is what
   `search_window` exists for. Ask; do not assume they match.

3. **Quality expectations and criticality.** Freshness SLO, whether the source
   gates the pipeline or is advisory, and which endpoints are *required* versus
   *optional*. Absence is often normal — a tenant with no teams has no teams
   table — and only the user knows whether that is expected or alarming.

4. **Deletes.** Does the API say when something is deleted? If not, the only
   signal is absence from a complete fetch, which means `soft_delete` and a
   periodic full-history reconcile. Wrong in the permissive direction, a partial
   fetch tombstones the warehouse.

## Phase 3 — Generate

Fill in `sources/<name>/source.yml`, leaving `status: reference` for now.

Read `sources/pylon/source.yml` first — the complete worked example, whose
comments explain why each field exists. It ships as `status: reference`, so the
test suite builds a source from it like any other connector and it cannot rot
into teaching something the loader rejects. Its `extension.py` is the reference
for the escape hatch, and `sources/CONTRACT.md` is that contract written down.

The vocabulary itself is `sources/source.schema.json`. Prefer it over any list in
this file: it is what the loader enforces, so it cannot go stale, and the
`# yaml-language-server: $schema=` header the scaffold writes means an editor
validates as you type. Then:

```bash
uv run ingest validate --source <name>
```

This is the check that makes the connector reviewable without a credential. The
schema rejects unknown keys everywhere — a misspelled `time_stamp_columns` is an
error rather than a line nothing reads — and the lints catch what one file cannot
see: a database, pool or DAG id colliding with another connector, a declared
extension that is missing, a delegated resource with no builder, a rate-limit
family nothing routes to, a cursor that is not hinted.

You do **not** write a DAG. `airflow/dags/source_dags.py` builds the ingest,
backfill and reconcile DAGs for every spec in `sources/`, using the schedule,
timeouts and pool the spec declares. Adding the file is the whole step.

Also generate:
- **the research note**: fill in `sources/<name>/README.md` with what phase 1
  turned up — the rate limits you verified, the field the docs omit, the region
  that answered 301. This is the durable half of the research; without it the
  next person repeats it.
- **the credential**: add the variable the spec's `token_env` names to `.env`.
  There is no list to update — the manifest carries it, so a new connector's
  credential is picked up on the next `make secrets-push` with no edit anywhere.
- **fixtures**: capture a redacted page of each resource's real response into
  `sources/<name>/fixtures/<resource>.json` (a JSON array of records), and note
  the capture date in the README. The generic contract suite then drives this
  connector end to end into duckdb with no further test-writing: it asserts the
  paginator is genuinely exercised, every request carries a timeout, the declared
  page-size parameter reaches the wire, primary keys are unique, cursors land
  typed, and a second run is merge-idempotent.
- **only what is genuinely this API's behaviour** as a hand-written test, in
  `sources/<name>/test_<name>.py`. If the assertion would be true of any
  connector, it belongs in the contract suite instead — and is probably already
  there. Anything the generic mock cannot express (an auth endpoint, a sparse
  projection, a page that lies) goes in `sources/<name>/fixtures/server.py`,
  which the harness imports and calls as `register(mock, spec, fixtures)`.

### The vocabulary the loader accepts

Guessing here fails at load time at best, and silently at worst. What exists:

**`api.auth.type`** — `bearer`, `api_key`, `http_basic`, `oauth2_client_credentials`.
All but the last are static: `token_env` names the variable holding the value that
goes on every request. `oauth2_client_credentials` is for a token that expires
mid-run, and takes two more keys:

```yaml
auth:
  type: oauth2_client_credentials
  token_env: SWOOGO_ENCODED_CREDENTIALS
  token_url: https://api.swoogo.com/api/v1/oauth2/token
  credentials_in: basic_header      # or `body` (the default)
```

`body` sends `client_id`/`client_secret` as form fields. `basic_header` sends
`token_env` verbatim as `Authorization: Basic …`, for providers that show a
pre-encoded `base64(urlencode(id):urlencode(secret))` string — take theirs rather
than assembling it, because a reserved character in the secret silently produces a
credential that never authenticates.

**`endpoint.params`** — arbitrary query parameters, sent on every request.
**`endpoint.page_size_param`** — the spelling of page size, default `limit`.

These two are load-bearing far more often than they look. An API that returns a
sparse default projection gives you `id, name` and no `updated_at` unless you ask
for the columns by name — so the cursor column does not exist, the incremental
comparison has nothing to compare, and the failure reads as "the source never
changes" rather than as an error. Swoogo does exactly this; check a real response
before assuming yours does not.

**`pagination.kind`** — the shorthands are `cursor` and `single_page`; anything else
must be a raw dlt paginator config dict, which is passed through untouched:

```yaml
pagination:
  kind: {type: page_number, base_page: 1, page_param: page, total_path: _meta.pageCount}
  data_selector: items
```

### When the spec is not enough

Some APIs do things no configuration language should have to describe. Pylon
returns pages claiming `has_next_page: true` while carrying no data, and its
messages have no cross-issue endpoint so the worklist is a warehouse query.

Try the declarative form first. Two strategies need no code at all — check
whether `cursor` fits before reaching further: if the API takes a
"changed since" filter as a query parameter, it does, and the connector has no
Python. An extension is real code with real maintenance. But do not contort the
spec to avoid one: a connector that quietly loses rows is worse than a connector
with a Python file.

**The contract is `sources/CONTRACT.md`** — read it rather than this summary.
The module lives at `sources/<name>/extension.py`, beside the spec, and the spec
says `extensions: true`. For each resource whose strategy is not declarative,
`build_source` looks for `build_<resource>(spec, resource, paced=None)`, falls
back to `build_resource(...)`, and raises if it finds neither — a connector that
quietly skips an endpoint looks exactly like one whose source has no data.

Because the module is loaded by path from outside the package, it imports the
public surface and nothing else:

```python
from ingest_runtime.extension_api import (
    auth_for, column_hints, endpoint_params, make_transformer,
    paced_session, session_for, warehouse_rows,
)
```

The obligations that signature does not state — return a source and not a bare
resource, route every request through `session_for(spec, paced)`, give every
request an explicit timeout, fail loudly on a short read, define `reset()` if you
keep run-scoped state — are all in CONTRACT.md, each with the failure that put it
there. `ingest validate` checks the ones it can see; the rest are in the
contract suite.

**`--start`/`--end` do not reach an extension.** `build_source` takes no window, so
a delegated resource bounds itself by its own persisted cursor and `make backfill`
fetches what an ordinary incremental run fetches. The CLI warns when you ask, and
refuses `--mark-deleted` for a `full_history` resource on a delegated strategy —
tombstoning on flags the fetch ignored would delete every row the cursor happened
not to re-fetch. If a source genuinely needs a bounded historical load, that is a
runtime change, not a spec one.

## Phase 4 — Prove it loads

In order. Each step is cheap and rules out a different failure.

```bash
# 0. The spec parses, and this is the name everything else is keyed on
docker compose --profile cli run --rm airflow-cli ingest sources

# 1. Fetch shape and transform, against duckdb — never touches production state
docker compose --profile cli run --rm airflow-cli \
  ingest run --source <name> --destination duckdb --sample 3

# 2. A bounded real load
docker compose --profile cli run --rm airflow-cli ingest run --source <name>

# 3. The quality gate — generated from this spec's own `quality` block
docker compose --profile cli run --rm airflow-cli dq run --source <name>
```

`--sample 3` prints records **post-transform, pre-load** — exactly as they will
land. Read them. A promoted field that is `None` for every record means the path in
`promote` is wrong, and no test will catch that.

Then check the warehouse:
`make ch-q Q="SELECT count() FROM raw_<name>.<table>"`.

### Connect it

Only once the steps above pass. Three edits, in one commit:

1. `status: reference` → `status: connected` in the spec
2. add `<name>` to `sources/CONNECTED` (one name per line, sorted)
3. `uv run ingest manifest && uv run ingest inventory` — regenerate what shell,
   compose, terraform and the docs read

`uv run ingest validate` fails if the status and `CONNECTED` disagree, which is
the tripwire: scheduling a connector is a deliberate act written down twice, and
neither half is something you can fall into.

Finish with `make up`. Two things only take effect once the stack is re-initialised:

- **the Airflow pool** `<name>_pipeline`, which `airflow-init` creates from the
  specs. Until it exists the generated DAG's fetch task queues forever — which reads
  as a slow run rather than a broken one.
- **the token**, if the containers were already running when it was added to `.env`:
  container environments are frozen at create time.

## Rules

- **`raw_<source>` is derived, never configured.** Two sources sharing a database
  share a soft-delete pass, where "absent from this run" comes to mean "belongs
  to the other source".
- **Each source gets its own pool of 1.** The pool stops concurrent runs racing
  *that source's* cursor. Sharing one serialises connectors that have no reason
  to wait for each other.
- **Hint only the columns the incremental logic compares.** A hint is a schema
  commitment; a column nothing compares should stay inferred.
- **Never ingest by hand against the production warehouse while the stack is up.**
  Airflow serialises through the pool; an out-of-band run races the cursor.
  `--destination duckdb` is always safe.
- **If a source needs a change to the runtime, stop and place it deliberately.**
  There are three answers, and editing the middle of `runtime.py` is not one of
  them — that is how a generic runtime becomes six connectors in a trench coat.
  If the need is genuinely shared between APIs, it is a registry entry
  (`auth.py`, `paginators.py`) plus its tests. If it is this API's peculiarity,
  it goes in that connector's `extension.py`, declared as `type: extension` or
  `paginator: extension`. If it is neither, the spec is probably wrong — say so.
