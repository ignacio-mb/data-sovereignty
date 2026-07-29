---
name: api-research
description: Work out how a third-party API behaves before writing anything against it — auth, endpoints, pagination, rate limits, incremental filtering, deletes. Triggers — "how does the X API paginate?", "what are Zendesk's rate limits?", "can this API filter on updated_at?", "research this API before we connect it", "does this API tell us about deletions?".
allowed-tools: Bash, Read, WebFetch, WebSearch
---

# Researching an API

Establishing how an API behaves, from its documentation, before any code assumes
it. Loaded by `add-source` for phase 1; useful on its own when an existing
connector starts behaving oddly and the question is whether the API changed.

The output is a set of findings with **citations and admitted gaps** — not a
confident summary. A wrong fact here becomes a connector that looks correct and
loses rows.

## Find the machine-readable spec first

An OpenAPI/Swagger document answers most of this at once, and is far more
reliable than prose that drifts from the implementation. Look for `/openapi.json`,
`/swagger.json`, a "Download OpenAPI spec" link, or a `<vendor>-openapi` repo on
GitHub. If one exists, endpoints, methods, parameters, response schemas and
field types all come from it.

Prose docs are the fallback, and are frequently out of date in exactly the places
that matter — page-size caps and rate limits.

## The seven questions

**1. Auth.** Bearer token, API key in a header, basic, or OAuth2 client
credentials? Which header, and is there a prefix (`Bearer `, `Token `)? Does the
token expire — if so, refresh is a runtime concern, not a spec one.

**2. Endpoints.** Path, method, and what one record looks like. Note where a
"list" endpoint returns a *summary* and the detail needs a second call per
record — that shape is expensive and changes the schedule you should propose.

**3. Pagination.** Which of these, precisely:
   - **cursor** — an opaque token, in the response body or a header. Note *where*
     the next cursor lives and *how* it is sent back (query param vs body).
   - **offset / limit**, **page number**, or **link header** (RFC 5988).
   - Is there a "has more" flag, or is an empty page the only terminator?

   Also: what is the maximum page size, and does it differ per endpoint?

**4. Rate limits.** The number, the window, and the *scope* — per token, per IP,
per endpoint family? Many APIs publish different budgets for search versus list
endpoints. Note the 429 behaviour too: `Retry-After`, or a documented backoff.

**Never guess this.** If the docs are silent, report that it is unknown rather
than assuming a safe-looking number. An assumed budget is discovered by being
rate-limited in production, usually mid-backfill.

**5. Incremental filtering — the one that quietly loses data.** Two separate
questions that are easy to conflate:
   - Which timestamp fields exist on a record (`created_at`, `updated_at`, …)?
   - Which of them can you actually **filter on**, and on which endpoint?

   An API commonly exposes `updated_at` on the record while its list endpoint
   filters only on creation time. Syncing incrementally on that endpoint will
   never return an old record edited today, and nothing errors — the rows are
   just missing. If a search endpoint filters on update time, that is the one
   incremental must use.

   Also check whether the filter window is capped (30 days is common), and
   whether results are ordered.

**6. Deletes.** Does the API report deletions — a `deleted` flag, a tombstone
endpoint, an `include_deleted` parameter? If not, the only signal is absence from
a complete fetch, which forces a periodic full re-fetch to reconcile against.

**7. Nesting and custom fields.** Are records deeply nested? Can users define
custom fields that appear as new keys? That decides how much must be flattened
and JSON-stringified rather than exploded into columns — a source with
user-defined fields will otherwise mint warehouse columns on someone else's
schedule.

## Report like this

```
<API> — findings

  auth          bearer, `Authorization: Bearer <token>`      [docs URL]
  pagination    cursor in body at `meta.next`, sent as ?cursor=   [docs URL]
  page size     max 200 (100 on /search)                     [docs URL]
  rate limits   600/min per token; /search 60/min            [docs URL]
  incremental   list filters created_at only; /search filters updated_at [docs URL]
  deletes       not reported — absence is the only signal
  timestamps    created_at, updated_at, closed_at (RFC 3339)

  UNKNOWN       whether the 30-day filter cap applies to /search
                (docs mention it under /list only)
```

Citations are not decoration. The next person to hit a surprise needs to know
whether the docs were wrong or the reading was.

## Verify against the API where you can

Documentation drifts. If a token is available and a read-only call is harmless,
one request settles what the prose leaves ambiguous:

```bash
curl -sS -D- -o /dev/null -H "Authorization: Bearer $TOKEN" \
  'https://api.example.com/v1/things?limit=1'
```

Response headers often carry the rate limit (`X-RateLimit-Limit`,
`RateLimit-Policy`) more accurately than the docs. **Read-only calls only** —
research must never create, modify or delete anything in someone's account.
