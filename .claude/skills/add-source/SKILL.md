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

Check the pagination against the runtime's vocabulary — `cursor`, `offset`,
`page_number`, `json_link`, `header_link`, `single_page`. Something outside that
list means an extension, and it is much cheaper to know now than in phase 3.

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

Write `sources/<name>.yml`. Read `sources/pylon.yml` first — it is the reference
connector and its comments explain why each field exists. Then:

```bash
uv run python -c "from pylon_pipeline import spec; print(spec.load('<name>'))"
```

The loader rejects unknown keys, unknown incremental strategies, a bare `true`
for `soft_delete`, and referential edges naming tables the spec never declared.
A spec that loads is one the runtime can act on.

Also generate:
- the DAG file (thin, calling the shared factory)
- the credential: `<NAME>_API_KEY` in `.env.example` and in the secrets push list
- a test fixture in `pipeline/tests/` modelled on `conftest.py` — a mock whose
  handlers **implement the API's filtering and pagination**, not canned bodies.
  That is what makes the harness worth copying: it exercises the paginator
  against the envelope the real API actually returns.

### When the spec is not enough

Some APIs do things no configuration language should have to describe. Pylon
returns pages claiming `has_next_page: true` while carrying no data, and its
messages have no cross-issue endpoint so the worklist is a warehouse query. Both
live in a module named by `extensions:` in the spec.

Reach for it only after trying the declarative form — an extension is real code
with real maintenance. But do not contort the spec to avoid one: a connector that
quietly loses rows is worse than a connector with a Python file.

## Phase 4 — Prove it loads

In order. Each step is cheap and rules out a different failure.

```bash
# 1. Fetch shape and transform, against duckdb — never touches production state
docker compose --profile cli run --rm airflow-cli \
  ingest --destination duckdb --sample 3

# 2. A bounded real load
docker compose --profile cli run --rm airflow-cli ingest --destination clickhouse

# 3. The quality gate
docker compose --profile cli run --rm airflow-cli dq run --checkpoint raw_<name>
```

`--sample 3` prints records **post-transform, pre-load** — exactly as they will
land. Read them. A promoted field that is `None` for every record means the path
in `promote` is wrong, and no test will catch that.

Then check the warehouse: `make ch`, then `SELECT count() FROM raw_<name>.<table>`.

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
