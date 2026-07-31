---
name: add-source
description: Connect a new API to the warehouse — research it, agree the sync semantics, generate the connector, prove it loads. Triggers — "also ingest Zendesk", "add Salesforce data", "connect our Stripe account", "pull from the GitHub API", "I want another source", "how do I add a new connector?", "set up a new sync".
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch, AskUserQuestion
---

# Connecting a source

A source is a file. `sources/<name>.yml` says what the API is, how it pages, what
is incremental, when it runs, and what "correct" means for the tables it lands;
the runtime does the rest. Most connectors need no Python at all.

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

Write `sources/<name>.yml`. Everything in that directory is **connected**: it
generates an unpaused hourly DAG and demands its credential on every clone of this
repo. That is the bar — put a spec there only when it is genuinely being run.

Read `reference/pylon.yml` in this skill's directory first. It is a complete
worked example, kept here rather than in `sources/` so it documents the contract
without the stack trying to run it, and its comments explain why each field
exists. A test asserts it still parses, so it cannot rot into teaching something
the loader rejects. Then:

```bash
uv run python -c "from ingest_runtime import spec; print(spec.load('<name>'))"
```

The loader rejects unknown keys, unknown incremental strategies, a bare `true`
for `soft_delete`, and referential edges naming tables the spec never declared.
A spec that loads is one the runtime can act on.

You do **not** write a DAG. `airflow/dags/source_dags.py` builds the ingest,
backfill and reconcile DAGs for every spec in `sources/`, using the schedule,
timeouts and pool the spec declares. Adding the file is the whole step.

Also generate:
- the credential: add the variable the spec's `token_env` names to `.env`. There is
  no list to update — `scripts/secrets_push.sh` greps `token_env` out of
  `sources/*.yml`, so a new spec's credential is picked up on the next
  `make secrets-push` with no edit anywhere
- a test modelled on `pipeline/tests/test_build_source.py` — a `requests_mock`
  whose handlers **implement the API's pagination**, not canned bodies. That test
  serves two pages precisely so the paginator is exercised rather than the first
  response happening to be the whole dataset; a single-page mock passes while a
  broken paginator silently truncates every real load.

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
messages have no cross-issue endpoint so the worklist is a warehouse query. Both
live in a module named by `extensions:` in the spec.

Reach for it only after trying the declarative form — an extension is real code
with real maintenance. But do not contort the spec to avoid one: a connector that
quietly loses rows is worse than a connector with a Python file.

**The contract.** The module lives at
`pipeline/src/ingest_runtime/sources/<name>.py`. For each resource whose strategy
is not `full_refresh`, `build_source` looks for `build_<resource>(spec, resource,
paced=None)` and falls back to `build_resource(spec, resource, paced=None)`, and
raises if it finds neither — a connector that quietly skips an endpoint looks
exactly like one whose source has no data.

Three things the signature does not tell you:

- **Return a dlt *source*, not a bare resource.** The CLI's samplers and the run
  summary both walk `.resources`. A `@dlt.resource` builds fine, runs fine under
  `pipeline.run()`, and dies on the first `--sample`.
- **`paced` is the run's `EndpointPacer` and you must use it.** Pass it to
  `paced_session(spec, paced)` and make every request through that session.
  Ignoring it means the spec publishes a rate limit the connector never obeys,
  and for a source whose resources are all delegated that is the entire budget.
- **Give every request an explicit `timeout=`.** `requests` waits forever by
  default, and a fan-out holds the source's pool of one while it does, so one hung
  call wedges every later run behind it.

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
- **If a source needs a change to the runtime, stop.** The abstraction leaked and
  the spec is probably wrong. Say so rather than editing `runtime.py` to fit one
  API — that is how a generic runtime becomes six connectors in a trench coat.
