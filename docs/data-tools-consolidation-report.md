# data-tools script comparison: what they share, how they differ, and whether they merge

**Status:** analysis report · **Date:** 2026-08-05 · **Measured against:** `data-tools` `origin/master` @ `4ddaf18` (the working tree sits on `pylon-clickhouse-update`, which is behind master and was not used)
**Why this exists:** evidence for the question behind the [connector modularization proposal](connector-modularization-proposal.md) — is a shared spec-driven engine honestly better than data-tools' one-script-per-connector shape, and could today's scripts actually be merged into it?

---

## 1. Verdict up front

**They are not mergeable into one *script*, and nobody should try. They are very mergeable into one *engine + declarative specs + a handful of small extensions + a "jobs" lane* — which is exactly the data-sovereignty shape.**

The numbers that carry the conclusion:

- The repo is **51 Python files, 9,981 lines (7,429 SLOC)**. Total shared code: **275 lines — 2.8%**. The only helper any *ingestion* pipeline shares is `utils.build_dlt_pipeline`, a **10-line if/else** (`utils.py:15-24`). Everything else — retry, pagination, watermarks, DSN parsing, flattening, logging, DDL — is re-implemented per file.
- Across the seven core API-pull scripts (~1,950 SLOC): **≈54% is plumbing** a shared engine owns outright, **≈23% is declarative fact** (endpoints, keys, page sizes, cadences, lookbacks) already expressible in a `sources/*.yml` vocabulary, and **≈23% is genuinely source-specific** — concentrated in four places (pylon's windowed history rescan and message fan-out, apollo's credit-budget arithmetic, athenahq's metric-shape normaliser, grainmate's roster matching — the last of which is dead code).
- The duplication cost is as much in the **surround** as in the scripts: 19 near-identical click wrappers (**~1,001 lines of pure option plumbing**), the ClickHouse tox preamble copy-pasted **9×**, the Slack-notify job copy-pasted **4×** across workflows, and every dual-destination source paying **2 tox envs + 2 workflow jobs in 2 workflow files** to express one `--destination` flag.
- The variation between scripts is overwhelmingly **variation without justification**: six independent HTTP retry policies, six rate-limit strategies, ten pagination loops in six idioms, `Retry-After` parsed three ways, two ClickHouse DSN parsers (one acknowledging the other in a docstring), three byte-equivalent copies of `_flatten_record`, and the same Mailchimp pagination-plus-retry block pasted three times *inside one 153-line file*.
- The scripts that most need shared machinery are the ones missing its basics: **pylon and apollo make HTTP calls with no timeout at all**, as do pipemate, linear_pylon_link, timeline_event_sync, ciomate (including a `pd.read_csv(url)` straight off the network), and the shared `rest_client`/`metabase_client` helpers themselves.

What does *not* merge — and shouldn't: the reverse-ETL and automation workflows (chimpmate, ciomate, pylon_link, timeline-sync, issue-mention), which aren't ingestion at all; metabase_store's irreducible ~580 SLOC of type coercion, budget arithmetic, and S3-export loading; and apollo, which is a warehouse-driven enrichment loop wearing a connector's clothes. Those belong in a thin **jobs lane** (hand-run containers behind small scheduled DAGs), not in a config vocabulary.

---

## 2. The corpus

| Category | Scripts | Lines (impl + wrapper) |
|---|---|---|
| Core API-pull ingestion (hourly) | pylon, octolens, grainmate, stripe, contrast, athenahq, apollo | 3,093 + 370 |
| API-pull ingestion (daily) | unifymate, pipemate | 302 + 82 |
| DB-pull ingestion | metabase_store (+ CLI + Lambda), metabase_events, screen_easy | 1,905 + 83 |
| Reverse-ETL / maintenance | chimpmate, ciomate (+cio_client), pylon_sync | 714 + 105 |
| Automations | timeline_event_sync, linear_pylon_link, issue_customer_mention_customerio | 1,310 |
| Unscheduled / dev tooling | gitmate; metamate, dbtmate, migration_mapper | 465 + 67; ~1,100 |
| Shared modules | utils, common, rest_client, cio_client, metabase_client | 275 |

Two runtimes cut across all of it: **Python 3.8 + `dlt[postgres]==0.3.16`** (the legacy Postgres path) and **Python 3.11 + `dlt[clickhouse]==1.28.1`** (the ClickHouse path), selected per tox env and mirrored by parallel workflow files. Seven sources run on *both*, which is how one connector comes to own four schedule definitions.

## 3. What they share

Honestly: almost nothing, and less than it looks.

| Shared module | SLOC | Who actually uses it |
|---|---|---|
| `utils.py` — `build_dlt_pipeline` | 10 | 6 of the 7 core scripts + unifymate + metabase_store. **pylon builds its pipeline inline and skips it** (`pylon.py:158-163`), as do screen_easy, gitmate (×2) |
| `utils.py` — `set_return_defaults` | ~10 | pylon only |
| `rest_client.py` `APIClient` | 45 | ciomate + gitmate only — **and it sends requests with no timeout** |
| `cio_client.py` | 9 | ciomate — and `issue_customer_mention` carries an **inline duplicate as an import fallback, which is *better* than the original** (the copy passes `timeout=30`; the shared one passes none) |
| `metabase_client.py` | 84 | dev tooling + timeline-sync — no timeout either |
| `common.py` | 46 | metamate/migration_mapper only |

**No shared HTTP layer, no shared retry, no shared pagination, no shared watermark helper, no shared logging.** The one shared *behavior* is accidental: octolens, contrast, athenahq, unifymate and pipemate import `dlt.sources.helpers.requests` — which silently adds dlt's own 5-attempt retry underneath their hand-written retry loops. Exactly one file noticed and disabled it to keep a single source of truth (`athenahq.py:40-48`); octolens documents the same collision and then stacks its retries on top anyway (`octolens.py:61-75`), so a 429 there can be retried 5 × 10 times.

Near-literal copy-paste found across files (the "this was one function once" census):

- `_safe_retry_after`: three copies (pylon/octolens/athenahq), differing only in `int()` vs `float()`.
- The 429-handler block: literal between octolens and athenahq bar the log prefix.
- `_flatten_record`: **byte-equivalent** in octolens, contrast, athenahq.
- The `DatabaseUndefinedRelation` bootstrap guard: five copies — four typed, one string-matching the exception text (`pylon.py:246`), which breaks on any wording change.
- The `data_type_map` + ValueError dispatch: three literal copies; pylon has the unvalidated ancestor (bare `KeyError`).
- The `'=' * 60` load-info banner: four copies.
- The CLI connect-string dispatch: seven literal occurrences across `scripts/`.
- ClickHouse DSN parsing: two independent parsers (`metabase_events.py:120-141` vs `metabase_store.py:635-659`).

## 4. How they differ

### 4.1 Differences forced by the APIs (legitimate — this is connector *fact*)

Pagination shape (cursor / page-number / short-page / total-count / has-more / SDK cursor / job-poll), auth header name (`Authorization: Bearer` vs `x-api-key` vs `X-Api-Key` vs query-string token), page sizes, rate budgets, and incremental semantics (which endpoints filter on what). These are precisely the things a spec vocabulary exists to declare — athenahq even keeps its metric endpoints in a config table already (`METRIC_ENDPOINTS`, `athenahq.py:29-34`), and metabase_store's `SCHEMA_CONFIG` (`metabase_store.py:63-196`) *is* a 130-line declarative spec embedded in Python.

### 4.2 Differences that are just drift (the cost of one-script-per-connector)

| Concern | The spread across scripts |
|---|---|
| HTTP retry | 6 independent policies: pylon retries 429 only; contrast retries once after a flat 60s; athenahq has three separate counters (429 / 5xx / network) with capped exponential backoff; grainmate has a header-fallback chain; apollo string-matches `'Too Many Requests'` and gives up; stripe has none |
| Rate limiting | reactive-only, proactive header-based (octolens), unconditional `sleep(2)` per call (pylon, pylon_link), env-tunable inter-page pause defaulting to 0 (grainmate), none (stripe, apollo, everything in §2's bottom half) |
| Timeouts | none (pylon, apollo, pipemate, pylon_link, timeline-sync, ciomate, rest_client, metabase_client) / flat 30s (octolens, contrast, grainmate) / proper connect-read tuple with rationale (athenahq only) |
| Incremental | destination `max(timestamp)`+1s (octolens/CH), fixed 72h lookback (octolens/PG — same source, different correctness per destination), `min`/`max(created)` with backfill inversion (stripe), full id-set into memory (grainmate), two-table join watermark with a +3s fudge (pylon), 7-day overlap + merge (athenahq), full re-scan since 2019 in 30-day windows (pylon issues), full refresh since 2023 by design (contrast), `dlt.sources.incremental` pushed into the API query (unifymate, metabase_store), cron-schedule-as-watermark with no state at all (ciomate — a missed day is lost, a double run double-posts), actions/cache JSON file (pylon_link — an eviction silently replays to 2025-09-29) |
| Destination handling | one flag through a shared constructor (unifymate, cleanest) / inline `destination="postgres"` hardcoded (pylon, screen_easy, gitmate) / a `--destination` flag that cannot work because the import fails on that runtime (metabase_store, `:39-45`, `:426-430`) |
| Logging | `print()` in six formats, `logging` in exactly one file (grainmate), `click.echo` in one CLI |
| CLI | four different click patterns, three no-click automations, one variadic-positional command; 8 of 19 wrappers runnable as modules, 11 not |
| Env naming | five DSN conventions, four token conventions, six Customer.io credential names with three `X or Y` fallbacks |
| Exit discipline | partial-failure paths exit 0 across the board; one script *always* exits 0 even when every API call failed (`issue_customer_mention:229-249`) — invisible to the Slack-on-failure job that is the repo's only alerting |

### 4.3 Same source, different behavior by destination — the sharpest symptom

Because "both destinations" means two tox envs and two workflow jobs, the two halves of one connector drift independently: octolens self-backfills from a warehouse watermark on ClickHouse but uses a fixed 72-hour lookback on Postgres; grainmate stops after two fully-known pages on ClickHouse but **re-paginates all history to 2020 every hour on Postgres**; pylon is Postgres-only and is simultaneously the only source with a delete story — so `--mark-deleted` exists nowhere on the ClickHouse path.

## 5. Defects surfaced in passing

Worth fixing (or making impossible) regardless of any migration:

1. **No HTTP timeout** in pylon, apollo, pipemate, linear_pylon_link, timeline_event_sync, ciomate, `rest_client`, `metabase_client` — hourly and 10-minute jobs that hang forever on one stuck socket.
2. `contrast`'s CLI cannot reach its own `recurring_event` resource — defined and registered (`contrast.py:286,298`) but missing from the `click.Choice` (`scripts/contrast.py:26`).
3. `pylon --to-schema` is half-honoured: the mark-deleted UPDATE is parameterised but three watermark queries hardcode `raw_pylon` (`pylon.py:229,235,250`); apollo similarly hardcodes `raw_apollo.enriched_contact` (`apollo.py:85`).
4. **grainmate is ~60% dead**: ~600 lines of commented-out OpenAI categorisation plus ~250 lines of live-syntax code nothing calls — including the Postgres roster read that still executes every run to feed functions that are never invoked (`grainmate.py:680-691` → dead consumers at `:329,:390`).
5. Class-level mutable header dicts in five scripts — two instances in one process share and clobber one auth header; grainmate is the only immune file.
6. `timeline_event_sync` runs a `DO $$` PL/pgSQL primary-key drop-and-recreate migration **every ten minutes** (`:320-339`) and opens a new Postgres connection per row inserted (`:377`).
7. `unifymate`'s bulk-job poll loop is unbounded — a job stuck `RUNNING` hangs the run forever (`:112-137`); ciomate's export poll is the same with `sleep(1)` (`:20-22`).
8. `pipemate` fetches with no `raise_for_status` — a 401/429 surfaces as `KeyError: 'data'` — and silently truncates at Pipedrive's default page size (no pagination at all).
9. `gitmate` (unscheduled) truncates before creating (first run against a fresh schema errors), carries an unresolved pagination TODO on a hardcoded GraphQL project id, and string-matches four different error messages to decide what to swallow.
10. Dependency drift: grainmate imports `thefuzz`, present in `requirements-clickhouse.txt` but absent from `requirements.txt` — the Postgres env works by accident of transitive installs.

## 6. Mergeability, script by script

Mapping each onto the data-sovereignty model — **spec** (declarative `source.yml`), **spec + ext** (spec plus a small colocated `extension.py`), **jobs** (thin scheduled DAG running a containerised script; the deliberate exception lane in the [infrastructure proposal](infrastructure-proposal.md) §8 P4), or **retire**:

| Script | SLOC | Verdict | What the engine absorbs | What genuinely remains |
|---|---:|---|---|---|
| octolens | 259 | **spec** | retry/throttle/pagination/flatten/watermark | nothing — the +1s inclusive-bound buffer and the lookback are config |
| contrast | 242 | **spec** | full-refresh-from-date, page-number paging | the page-100-cap → date-restart trick, generalisable as a paginator strategy (registry entry, not connector code) |
| stripe | 87 | **spec** | watermark + backfill inversion (config-shaped), cursor paging | nothing; consider dlt's verified Stripe source outright |
| athenahq | 442 | **spec + ext (small)** | client, three pagination idioms, per-website fan-out, 14 resource defs | the metric-shape normaliser (~40 lines) |
| grainmate | 486 live | **delete dead 60%, then spec + ext (small)** | retry, cursor paging, known-id stop | detail fan-out (a `parent_fanout`), creator-email probe, notes coercion (~60 lines) |
| pylon | 296 | **spec + ext** — already the *reference connector* in data-sovereignty | request/retry/paging/pipeline | windowed history rescan with 429 window-halving, message join-watermark + fan-out + HTML scrub, wall-clock budget (~120 lines) |
| apollo | 138 | **jobs** | — | it is a warehouse-driven enrichment loop (candidate SQL → API → splice), not a connector; keep as a containerised job with a budget |
| unifymate | 193 | **spec + ext (small)** | dlt incremental, both pagination modes | the attribute-schema introspection / `ADDRESS` blacklist (~50 lines) |
| pipemate | 48 | **spec + ext (tiny)** or fold into the warehouse | — | worklist-driven per-deal fetch; 48 lines that also need pagination *added* |
| screen_easy | 95 | **spec** (dlt `sql_database`) | everything | nothing — the strongest single merge candidate |
| metabase_store | 711 (+434) | **jobs (heavy tier)** | the Lambda's entire time/row truncation machinery dies with the 900s ceiling (infra proposal §5.3) | `SCHEMA_CONFIG` is already a spec; the interval TypeDecorator, DSN keepalives, closed-bound semantics, and the S3-export loader stay custom (export loader → runbook) |
| metabase_events | 227 | **jobs (thin SQL DAG)** | — | `remoteSecure()` push-down + hand-written DDL; the pattern is correct as-is, keep it |
| chimpmate | 111 | **jobs** | — | reverse-ETL; fix the thrice-pasted pagination/retry when containerised |
| ciomate + cio_client | 53 | **jobs** | — | reverse-ETL; needs real state (cron-as-watermark is a live correctness bug) |
| issue_customer_mention | 347 | **jobs** | — | reverse-ETL; fix always-exit-0; unify the six credential env names |
| linear_pylon_link | 387 | **jobs** | — | automation with API-side-effect workaround; move state from actions/cache to the warehouse/S3 |
| timeline_event_sync | 403 | **jobs → EventBridge** (infra proposal §6.5) | — | fix per-row connections + the 10-minute DDL migration when touched |
| gitmate | 410 | **retire / re-scope** | — | unscheduled, three unrelated jobs, known bugs; decide ownership before porting anything |
| pylon_sync | 483 | **jobs** (flag: currently unassigned in the migration tracker) | — | Pylon reverse-ETL split out of pylon.py |

**Roll-up.** Of ~5,000 SLOC of scheduled pipeline code: about **2,000 evaporates into the engine** (it already exists in data-sovereignty — pacing, retries, timeouts, cursor handling, flattening, soft-delete, run summaries), about **1,150 becomes YAML**, about **400 becomes five small colocated extensions** (pylon, athenahq, grainmate, unifymate, contrast's paginator registry entry), and about **1,450 stays imperative but relocates to the jobs lane** where being imperative is honest. On top of that, the entire surround — 1,001 lines of click wrappers, 9 tox preambles, 4 Slack jobs, dual workflow files — is replaced by generated DAGs, the manifest, and per-source task definitions, none of it hand-maintained.

Engine gaps this exercise exposes (work items for data-sovereignty, not blockers): a generic **destination-watermark** incremental strategy (octolens/stripe's pattern — currently extension-only territory), a **page-cap-restart** paginator shorthand (contrast), and the `jobs/` lane formalised as the infra proposal already assumes.

## 7. Answering the question directly

**"Are they honestly mergeable into one script?"** As one script — no, and the 23% of genuinely source-specific logic is the proof; any single script absorbing pylon's window rescan *and* apollo's credit budget *and* metabase_store's type coercion would just be seven scripts concatenated behind one argparse. As one **system** — yes, demonstrably: over half of every connector file is the same six concerns re-implemented with unjustified variation, the per-API facts fit a declarative vocabulary that already exists in data-sovereignty, and the residue is small, nameable, and exactly what the `extension.py` escape hatch and the jobs lane are for. The strongest argument is the defect distribution: every bug in §5 lives in hand-rolled plumbing, none of them lives in a declared fact — merge the plumbing once, and that entire bug class stops being writable.

This report is the incumbent-alternative evidence for [connector-modularization-proposal.md](connector-modularization-proposal.md) §12 — "one hand-written dlt script per connector, like data-tools today" is what the spec model replaces, and these are the measured reasons why.
