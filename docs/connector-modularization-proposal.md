# Connector modularization proposal: `sources/<name>/` as a dossier around the spec

**Status:** **implemented** — this document is the design; the code on this branch is the result · **Date:** 2026-08-04 · **Scope:** repo layout + the machinery that reads it
**Companion:** [infrastructure-proposal.md](infrastructure-proposal.md) — the target infra that consumes this layout (task definitions per source, baked specs, runtime tiers).
**Inventory:** [sources.md](sources.md), generated · **Contract:** [../sources/CONTRACT.md](../sources/CONTRACT.md) · **Vocabulary:** [../sources/source.schema.json](../sources/source.schema.json)

> **What landed, against what was proposed.** All nine PRs' content is on this
> branch, plus four things the work itself surfaced:
>
> - **A fifth strategy, `cursor`**, built declaratively from dlt's own endpoint
>   incremental. It was not in the proposal, and it matters more than anything
>   that was: it is the shape most REST APIs have — a "changed since" filter as a
>   query parameter — and it is what makes the majority of the `data-tools`
>   scripts (octolens, stripe, athenahq, unify, contrast's steady state) expressible
>   with no Python at all. See [data-tools-consolidation-report.md](data-tools-consolidation-report.md).
> - **`Resource.all_endpoints`.** A strategy with more than one endpoint only ever
>   contributed its primary one to the rate-limit routing table, so Pylon's three
>   declared budgets — `issues_list`, `issues_search`, `messages` — were published
>   and never applied. Found by the new lint, which reports a family nothing routes to.
> - **Path-template routing.** `/issues/{issue_id}/messages` is not a substring of
>   `/issues/abc123/messages`, so those requests fell through to whichever shorter
>   route prefix-matched and were billed to the list endpoint's budget.
> - **`session_for` handles every auth type.** The fan-out used to require oauth2
>   because it received a config dict for anything else and could do nothing with
>   it; extensions now work with any registered scheme.
>
> The Pylon reference now has a real `extension.py` and is `status: reference`
> inside `sources/`. It could not build before — it named a module that did not
> exist, and nothing noticed for as long as it lived outside everything that
> builds a source.

---

## 1. Executive summary

Scale the repo from 2 connectors to ~30 by making the unit of modularity a **directory per connector** — but the philosophy inverts nothing. The directory is a *dossier around the spec, not a module*: `source.yml` remains the whole contract, and everything else in the directory is evidence (research, fixtures, reviewed schemas) plus the one named escape hatch (`extension.py`). The refactor makes the declarative core **stronger** in three ways:

1. **Validation becomes real.** Today no JSON Schema exists, there is no `ingest validate`, and five of the eight nested spec blocks accept any key silently — the shipped reference spec carries a key nothing reads.
2. **The runtime stops absorbing per-API knowledge.** Extension code moves out of the installed package to live beside its spec; auth types and paginator shorthands become registries; the dead per-API code inside the "generic" runtime is deleted — which makes CLAUDE.md's "nothing about a particular API is compiled into pipeline/" *true*. Today `pipeline/src/ingest_runtime/sources/swoogo.py` falsifies it.
3. **Enumeration collapses from seven independent readers to two**: Python reads specs directly; shell, compose, and terraform read one generated `sources/manifest.json`.

The reviewer-visible payoff, before → after:

| | Today | Target |
|---|---|---|
| Declarative source | 6 files (2 unique + 4 shared prose/test edits) + env + init | **one directory + one line in `sources/CONNECTED` + two regenerated files** |
| Source needing custom code | 10 files, including edits to `runtime.py`, `spec.py`, `raw.py` | same directory + its `extension.py` + its test — **zero shared-code edits** |
| Concurrent connector PRs | guaranteed conflict on a pinned list + six prose files | auto-mergeable (each appends a distinct line) |

## 2. Today: what works, and where per-connector knowledge actually lives

The design's core claim — *a connector is a spec* — holds for configuration: fetch, orchestration, and quality all live in one YAML; `secrets_push.sh` derives required credentials by grepping `token_env:` from the specs; `ensure_database` creates `raw_<source>` at runtime; Metabase needs no per-source step. And two test files (`airflow/tests/test_dag_integrity.py`, `quality/tests/test_suites.py`) are already spec-driven and would scale unchanged — they are the model this refactor copies.

But an inventory of every *kind* of per-connector knowledge shows how much lives outside the contract:

| Knowledge | Where it lives today | Colocated? |
|---|---|---|
| Fetch / orchestration / quality config | `sources/<name>.yml` | ✅ |
| Credential *name* | `token_env` in the spec | ✅ (value out-of-band, by design) |
| Escape-hatch code | `pipeline/src/ingest_runtime/sources/<name>.py` — inside the generic package, three directories from its spec (`runtime.py:201` hard-codes the import path) | ❌ |
| API research ("the docs lie about X", verified rate limits) | **Discarded** — the api-research skill's report is a chat message; what survives is YAML comments | ❌ |
| Fixtures / mocked responses | Hand-written inside per-source test files (`test_swoogo_source.py` 284 lines, `test_customerio_spec.py` 249) — no `conftest.py` exists anywhere | ❌ |
| Reviewed dlt import schemas | `pipeline/schemas/<source>/import/` — exists, contains only `.gitkeep`; "schema evolution as a git diff" is unrealized | ⚠️ |
| Status / ownership / the connected list | **Six shared prose locations** (CLAUDE.md ×2, README ×2, pipeline/README, Makefile:23-24 — already stale — and `data-stack/SKILL.md:13-14` — also stale) plus the pinned list in `test_dag_integrity.py:83` | ❌ |
| The worked example | `.claude/skills/add-source/reference/pylon.yml`, pinned by three test files | (deliberate exile) |

### The twelve frictions, ranked

1. **Six prose files + one literal test pin name every source.** `assert specs == ["customerio", "swoogo"]` is a deliberate tripwire — and a guaranteed merge conflict on every connector PR at n=30, whose failure message instructs authors to hand-edit three more documents. Two of the six locations are already stale at n=2.
2. **Extension code lives inside the runtime package**, loaded via a hard-coded package prefix. Spec and code are reviewed as strangers; nothing links them but a string.
3. **The runtime leaks.** Connecting Swoogo edited `runtime.py` (+118), `spec.py` (+18), `raw.py` (+16). Each addition was genuinely generic (a new auth type, `endpoint.params`, `page_size_param`) — but the pattern is that every unseen API shape edits the engine, and the skill's "if a source needs a runtime change, stop" becomes either a lie or a bottleneck at 30 sources.
4. **No spec validation.** No JSON Schema; no `ingest validate`; `_reject_unknown` covers only 3 of 8 nested blocks. Live casualties: `pylon.yml:142` declares `has_more_path`, read by nothing; `incremental.parent` appears 13× and is read by nothing shipped. A reviewer cannot know a spec is correct without running it.
5. **The reference cannot build.** `spec.py` recognizes four incremental strategies; the runtime builds one; `search_window` and `parent_watermark` have zero implementations anywhere; and pylon.yml declares `extensions: pylon` while `sources/pylon.py` **does not exist**. No test notices, because nothing calls `build_source` on the reference. The worked example held up to every future agent is unbuildable.
6. **Seven independent readers of the flat glob** (`spec.py`, `source_dags.py` with its own YAML parse *and its own duplicated defaults*, the compose `airflow-init` pool loop, `secrets_push.sh`'s grep, `stack_update.sh`'s grep *and* its stem-based DAG-id prediction, the DAG tests). `spec.load()` keys on the filename stem; everything else keys on the `name:` field; nothing asserts they agree.
7. **Tests are per-source copy-paste** — identical env fixtures re-declared, paging mocks re-implemented, duckdb re-opened; the generic invariants (timeout on every request, page param on the wire, PK unique, built object is a source) are asserted only for Swoogo.
8. **Dead per-API code inside the "generic" runtime**: Pylon-shaped helpers in `ingest/transform.py` and `warehouse.py`, referenced by nothing.
9. **Schedule-minute collisions are hand-managed** in YAML comments (":23 — clear of :00 and of Pylon's :17"). No lint.
10. **No naming rules**: nothing validates `name:` as a legal ClickHouse identifier or Airflow dag_id, or as unique across specs — two specs with the same `name:` silently share a database and collide on DAG ids.
11. **`ops.pipeline_runs` has no `source` column** — source is inferred by parsing a nullable `dag_id`. (The CLI already emits `"source"` in the summary JSON; `run_log.py` drops it.)
12. **Wrong cardinality twice**: `pagination.data_selector` is source-wide where envelopes differ per endpoint (Customer.io had to omit it entirely); `quality.freshness` allows one table per source.

## 3. Target layout

```
sources/
├── source.schema.json          # THE spec schema — single source of truth (§7)
├── CONTRACT.md                 # the extension contract, formalized (§5)
├── CONNECTED                   # tripwire: one connected source per line (§4)
├── manifest.json               # GENERATED — consumed by shell/compose/terraform (§8)
├── swoogo/
│   ├── source.yml              # required; the whole contract (status: connected)
│   ├── extension.py            # present iff the spec says extensions: true
│   ├── README.md               # durable API research
│   ├── fixtures/               # recorded responses powering the generic harness
│   │   ├── events.json
│   │   ├── registrants.json
│   │   └── server.py           # optional shim for behaviors no spec key describes
│   ├── schemas/import/         # reviewed dlt import schema (moves from pipeline/schemas/)
│   └── test_swoogo.py          # extension semantics ONLY (~80 lines, from 284)
├── customerio/
│   ├── source.yml              # declarative → no extension.py, no test file
│   ├── README.md
│   └── fixtures/*.json
└── pylon/
    ├── source.yml              # status: reference — validates, BUILDS, never schedules
    ├── extension.py            # reference implementation of the delegated strategies
    ├── README.md
    ├── fixtures/
    └── test_pylon.py
```

**Rules.** Required: `source.yml`. Required for `status: connected`: fixtures for at least the `quality.required` resources (lint warns on the rest). Conditional: `extension.py` iff `extensions: true` — an orphan in either direction fails validation. Optional: `README.md` (always scaffolded), `schemas/import/`, `fixtures/server.py`, `test_<name>.py` (unique filename, so pytest never sees two same-named modules).

**Naming.** Uniform `source.yml` — every consumer gets exactly one glob (`sources/*/source.yml`; terraform `fileset("../sources", "*/source.yml")`), and the top-level meta files are excluded by construction rather than special-cased. Identity is the directory name; a lint asserts **directory name == `name:` == `^[a-z][a-z0-9_]{1,40}$`** (legal ClickHouse identifier, legal dag_id prefix, room for `raw_` and dlt staging suffixes). This closes friction 6's stem-vs-name split and friction 10's collision hole in one rule.

## 4. `status:`, the CONNECTED file, and pylon coming home

A required top-level **`status: connected | reference | paused`** — required *with no default*, so a spec cannot schedule without someone having written `status: connected` into a reviewable diff.

- **`connected`** — DAGs generated and live; token demanded by `secrets_push.sh`/`stack_update.sh`; pool created; a task definition exists in terraform.
- **`reference`** — validated, *built* by the generic harness, used as the skill's worked example; the DAG generator, pool loop, secrets scripts, and terraform all skip it. This is what lets pylon move into `sources/pylon/` without violating "never add a spec to `sources/` to demonstrate something": the rule's real content was always "never add a *connected* spec to demonstrate something," and now the machinery enforces it instead of a directory boundary. `.claude/skills/add-source/reference/` retires.
- **`paused`** — DAGs generated with `schedule=None` (manual trigger possible, nothing ticks); token still required. Covers "credential mid-rotation" without deleting history.

**The tripwire survives with its purpose intact.** `TestWhatThisCheckoutShips` exists so nothing schedules by accident and the connected list is acknowledged deliberately in a second place. It becomes: *the set of specs with `status: connected` must equal the lines of `sources/CONNECTED`* — a sorted, one-name-per-line file. Two connector PRs each append a distinct line and auto-merge; today they both rewrite one Python list literal. The six prose lists are deleted and replaced by a **generated inventory** (§8); CLAUDE.md keeps one paragraph describing the *mechanism* and pointing at `CONNECTED`.

**Credential loudness is preserved at three layers**: `secrets_push.sh` refuses a connected source with no pushed key; `stack_update.sh`'s pre-deploy token check fails; `runtime._auth` raises "`<VAR>` is not set" on every tick. New tests pin the new invariants: a `status: reference` spec contributes **zero DAGs and zero token requirements** (the property pylon's physical exile used to guarantee), and a `status: connected` spec with an unset `token_env` fails `build_source` loudly.

## 5. Extension colocation

`sources/<name>/extension.py`, loaded by a new `ingest_runtime/extensions.py`:

- `load_extension(spec)` resolves `spec.path.parent / "extension.py"`, loads via `importlib.util.spec_from_file_location("ds_source_ext.<name>", path)`, registers in `sys.modules`, caches by resolved path. This replaces the hard-coded package import at `runtime.py:201`.
- The spec key becomes **`extensions: true`** — the current string (`extensions: swoogo`) restates the location and invites drift; the legacy string is accepted during migration with a deprecation lint.
- **Packaging**: the extension stops being pip-installed — it is data next to the spec, exactly like the spec. Locally it rides the existing `./sources:/opt/project/sources:ro` bind mount; on the target infra it bakes into the data-runtime image with the specs. `DS_SOURCES_DIR` already tells every consumer where sources live; nothing new to plumb. Loading from a read-only mount works (`PYTHONDONTWRITEBYTECODE=1` added so laptop runs don't litter either).
- **Imports become a contract.** Colocated code can't use today's relative imports (`from ..runtime import _auth`). That forces the right thing: a **public `ingest_runtime.extension_api`** re-exporting exactly what extensions may touch — `auth_for(spec)`, `paced_session`, `column_hints`, `make_transformer`, `endpoint_params(resource)`, and the registry decorators (§6). Anything not exported is not contract.
- **`sources/CONTRACT.md`** formalizes what today lives as prose inside the add-source skill: `build_<resource>(spec, resource, paced=None)` with `build_resource` fallback; must return a dlt *source* (the CLI walks `.resources`); must route requests through `paced_session`; must set explicit timeouts; `--start/--end` never reach extensions; an optional `reset()` hook for run-scoped caches (generalizing the module-global event cache Swoogo's tests currently clear by hand).
- **No registry files, no entry points** — entry points require installation (defeats colocation); a registry file restates the filesystem. Path-loading plus two lints (declared ⇔ exists; every delegated resource has its builder) keeps "add a connector" a zero-shared-file operation.

## 6. Stopping the runtime leaks

- **Auth registry** (`ingest_runtime/auth.py`): `@auth_type("bearer")`-decorated builders replace the if-chain. A genuinely generic addition (OAuth2 client-credentials was one — it arrived with Swoogo but isn't Swoogo-specific) becomes one registered builder plus tests, touching no dispatch code. A genuinely per-source oddity declares `auth.type: extension` and supplies `build_auth(spec)` from its own directory — the oddity lives with the connector, not in the engine.
- **Paginator registry**: the shorthand branch becomes `@paginator_shorthand("cursor")` entries. Raw dlt config dicts still pass through untouched — that is the real escape hatch, and it already absorbed Swoogo's `page_number` shape without a code change (the design working as intended). `paginator: extension` lets an extension supply a paginator object for an otherwise-declarative resource — Pylon's glitch-tolerant cursor is exactly this shape.
- **Strategy truth-in-advertising.** Keep recognizing all four strategies — recognition is what makes the failure "you owe this connector a fetch function" and keeps soft-delete semantics expressible — but close the invisible gap: `ingest validate` fails any spec whose non-declarative resource lacks a builder, and the generic harness calls `build_source` on **every** spec, references included. Do **not** implement `search_window`/`parent_watermark` generically now: with zero shipped implementations, a generic engine would be invented from one imagined example — the exact "config language that grows a branch per API" failure the runtime's own docstring warns against. Revisit when a second real source demands one.
- **Fix, don't demote, the pylon reference.** Write `sources/pylon/extension.py` as the reference implementation of the two documented behaviors (glitch-tolerant paginator with bounded retries; warehouse-watermark worklist, exercised against duckdb), driven offline by fixtures. The escape hatch is the hardest part of the contract; demoting pylon to spec-only would leave it with no worked example at all. Alongside: strip the dead `has_more_path`, give the `window`/`search` sub-endpoints their `family:` keys so the declared rate budgets actually route, and let the extension honor `skip_statuses`/`budget_minutes` as strategy-owned vocabulary.
- **Delete the dead per-API code** in `ingest/transform.py` and `warehouse.py` — verified referenced by nothing.

## 7. Validation

**One JSON Schema** — `sources/source.schema.json` (draft 2020-12), `additionalProperties: false` at every level, conditional sub-schemas per `auth.type` and per `incremental.strategy` (so `token_url` is required exactly when OAuth is declared, `skip_statuses` is legal exactly under `parent_watermark`, and a typo is illegal everywhere). Living in `sources/` makes the directory self-describing and rides the same bind mount/image bake into both venv worlds. Consumers: `spec.py` validates on load (`jsonschema` dependency; `_reject_unknown` retires where the schema covers; semantic checks stay in Python); scaffolded specs carry `# yaml-language-server: $schema=../source.schema.json` for editor-time validation — for humans and agents alike; the add-source skill points at the schema instead of a prose vocabulary that goes stale.

**`ingest validate [--source X | --all]`** — schema validation plus the cross-spec lints no schema can express:

1. directory == `name:` == legal identifier; uniqueness of name, derived dag_ids, `raw_<name>`, pool;
2. schedule-minute collision detection across `connected` crons (replaces the hand-managed comments; warn, not error);
3. `extensions: true` ⇔ `extension.py` exists; every delegated resource has its builder; strategy-has-implementation;
4. `token_env` naming convention (`^[A-Z][A-Z0-9_]*$`, warn if not prefixed by the source name);
5. freshness tables/columns name declared resources; unrouted rate-limit families (warn);
6. connected-without-fixtures (warn); manifest freshness (`--check`).

Wired in three places: a pytest in the default suite runs `validate --all` (so **offline `make test` catches everything**), CI runs it as an explicit step, and the add-source skill's verification step becomes `ingest validate --source <name>` instead of a bare `spec.load()` one-liner.

## 8. One canonical enumeration

Two consumer classes, two answers:

- **Python** (`spec.py`, the generic harness) reads specs directly — it already owns the parser.
- **`airflow/dags/source_dags.py` keeps its deliberately-independent YAML read** (the venv rule is untouched) — new glob, `status` filter, and a **contract test** in the dag-tests environment asserting its derivations (pool, dag_ids, defaults) agree with `spec.py`'s, spec by spec, so the duplicated defaults can never drift silently again.
- **Shell, compose, and terraform** read a committed, **generated `sources/manifest.json`** (`ingest manifest`, CI-checked for freshness): `{name, status, path, token_env, pool, database, schedule, dag_ids[], runtime, resources[]}`. The `runtime` field is the `orchestration.runtime` tier the [infrastructure proposal](infrastructure-proposal.md) sizes Fargate tasks with (default `standard`). Grep-era consumers convert to `jq`: the compose pool loop, `secrets_push.sh` (**required** anyway — after pylon comes home, the token grep would wrongly demand `PYLON_API_KEY`), and `stack_update.sh` (whose stem-based DAG-id prediction is replaced by the manifest's resolved `dag_ids`, currently restated in three places). Terraform's per-source task definitions become `for_each` over the manifest's connected entries.

Also under this heading, three small correctness fixes: **`ops.pipeline_runs` gains a `source` column** (established `ADD COLUMN IF NOT EXISTS` pattern; the CLI already emits it); **`endpoint.data_selector` becomes a per-resource override** falling back to the source-wide value; **`quality.freshness` accepts a list** so a source can declare more than one freshness contract. And the six prose lists are replaced by a **generated `docs/sources.md`** plus a marker-delimited README table — prose keeps the mechanism, generation keeps the facts.

## 9. The shared test harness

- **`pipeline/tests/conftest.py`**: the `isolated` fixture (tmp DLT/schema dirs, dummy tokens for every spec's `token_env`, token-cache and extension `reset()` clearing), an `all_specs` parametrization over every `sources/*/source.yml`, duckdb helpers.
- **Generic mock server** (`pipeline/tests/spec_mock.py`): reads the spec and serves `fixtures/<resource>.json` honoring the *declared* pagination — cursor envelopes with proper termination, `page_number` with totals, single-page — always at a page size smaller than the fixture so pagination is genuinely exercised. `fixtures/server.py` extends it only for behaviors no spec key describes (Swoogo's sparse `fields` projection, its token endpoint). Declarative sources need zero shim.
- **Generic contract suite** (`test_connector_contract.py`), auto-running for every connector with no registration: spec validates; `build_source` returns a source; an end-to-end duckdb load lands rows for every fixture-backed resource; every request carried a timeout; the declared page-size param reached the wire; PKs unique in fixtures and in the landed table; hinted cursor columns land typed and non-null; every paced request attributes to a declared family; a second run is merge-idempotent and, for cursor strategies, sends a lower bound.
- **What stays hand-written** shrinks to extension semantics: Swoogo's fan-out-visits-every-event-and-page, the first-run-sends-no-filter rule, the `search=` grammar (~80 lines, from 284). Customer.io's test file dissolves into fixtures plus the generic suite. OAuth mechanics move to auth-registry unit tests.

## 10. Ergonomics — agents and humans

- **`ingest scaffold <name>`** creates the skeleton: a commented `source.yml` template with the schema header and **`status: reference`** — nothing schedules until the deliberate flip; a README research template; empty `fixtures/`. Flipping to `connected`, appending to `CONNECTED`, and running `ingest manifest` is the explicit final step.
- **api-research skill** gains one step: findings land in `sources/<name>/README.md`. The registrant-fields discovery that today survives only as YAML comments becomes a durable, reviewable artifact with a capture date.
- **add-source skill** — disappears: editing CLAUDE.md/README/pipeline-README (generated), the test-pin edit (one appended line), the prose about where extensions live (CONTRACT.md). Appears: `ingest validate`, fixture capture (redacted live samples), `CONNECTED`, `ingest manifest`. Unchanged: research → agree semantics → generate → **prove it loads** (`--sample 3`, duckdb rehearsal, `dq run`) — the live-API gate stays.
- **PR review** becomes one directory plus three small generated/pinned diffs (`CONNECTED`, `manifest.json`, `docs/sources.md`). A reviewer sees the whole connector in one tree; `CODEOWNERS` per connector directory becomes possible and optional.

## 11. Migration: nine mechanical PRs, each independently green

| PR | What | Verify |
|---|---|---|
| 1 | Schema + `ingest validate` on the *flat* layout; fix what it flags (pylon's dead keys, missing families) | `make test`; `ingest validate --all` |
| 2 | Correctness fixes: `ops.pipeline_runs.source`, per-resource `data_selector`, freshness-as-list, delete dead code | unit suite; `dq ops-init` idempotence in CI stack job |
| 3 | Dual-glob + `status:` in all seven readers; `status: connected` added to swoogo/customerio in place | `make test`, `make test-dags`, shellcheck, `airflow-init` creates both pools |
| 4 | Pure `git mv`: the two connectors into directories (+ schemas move); create `CONNECTED`; convert the pin test | same suite; deploy smoke; init seeds schemas from the new path |
| 5 | `extension_api` + path loader; `git mv` swoogo.py → `sources/swoogo/extension.py`; `extensions: true`; delete `ingest_runtime/sources/`; write `CONTRACT.md` | full suite; `ingest run --source swoogo --destination duckdb --sample 3` |
| 6 | Pylon unification: `git mv` reference → `sources/pylon/` (`status: reference`); write its reference `extension.py` + fixtures; repoint the three pinning tests; add the zero-DAGs/zero-tokens reference test; retire `reference/` | `make test`, `make test-dags` — **`build_source` runs on pylon for the first time** |
| 7 | `ingest manifest` (+`--check`); compose/secrets/stack_update converge on jq-over-manifest; drop dual-glob; hand the manifest to terraform's `for_each` | shellcheck; stack CI; one staged deploy; manifest-vs-fresh-derivation contract test |
| 8 | Generated `docs/sources.md` + README table; delete the six prose lists; CLAUDE.md rewritten to describe the mechanism | CI generated-files check; grep for stray hardcoded names |
| 9 | Auth/paginator registries; `conftest.py` + `spec_mock.py` + generic contract suite; fixtures for both connectors; shrink per-source tests; `ingest scaffold`; final skill-text pass | full suite; dry-run add-source against a toy API |

The dual-glob compatibility window is PRs 3–7 only. `git mv` PRs carry no content edits, keeping `--follow`/`blame -C` reliable.

## 12. Rejected alternatives

- **Per-connector Python packages / plugins (Airbyte-style).** Maximal isolation, and it destroys the property this repo is built on: that a connector is reviewable as one declarative file and most connectors need no Python at all. Entry points require installation — "add a source" becomes a packaging change again — and per-connector versioning at n=30 is a maintenance program. The visible failure mode is Airbyte itself: hundreds of connectors rotting independently. This design takes the *filesystem* half of the idea (colocation) and rejects the *module* half.
- **Splitting `sources/` into its own repo.** Breaks the atomicity that makes changes safe: a spec change and the runtime capability (or fixture) it needs can no longer land in one reviewed commit; `make test` can no longer validate specs against the loader that will read them; deploys grow cross-repo version pinning that "specs baked into both images" gets for free. Revisit only if a second *stack* wants the same specs.
- **Flat files + validation only.** Genuinely the cheapest option, and it fixes frictions 4, 6, 9, 10 — but it leaves extensions inside the runtime package (CLAUDE.md's core claim stays false), fixtures welded into test copies, research discarded, and 30 × ~400-line specs as siblings with their evidence elsewhere. It optimizes the reviewer's `ls` and pessimizes everything else this task cares about.
- **dlt's native `rest_api` YAML-only, escape hatch removed.** Pylon's lying paginator and warehouse-derived worklists, and Swoogo's per-parent fan-out with a proprietary search grammar, are not expressible in any configuration vocabulary that stays a vocabulary — pretending otherwise grows a config language with a branch per API or quietly loses rows. The hatch is load-bearing; the job is to fence it (colocated, contract-documented, validated), not remove it.

## 13. Risks

- **Loader indirection** — synthetic module names in tracebacks. Countered: stable `ds_source_ext.<name>` naming registered in `sys.modules`, one loader shared by runtime and tests, loader unit tests. dlt never serializes builder closures (state is JSON), so module identity doesn't leak into persisted state — asserted by the round-trip contract test.
- **Tripwire weakening** — three ways it could soften, each countered structurally: a `status` default (schema forbids — required, no default, ever); reference specs leaking into token demands or terraform (all consumers filter status via the manifest; the zero-DAGs/zero-tokens test pins it); a connected spec running without its secret (the three-layer loudness, now manifest-driven, plus an explicit test).
- **Doc/skill drift during the transition window** (PRs 3–7) — docs are updated inside the PR that changes their subject; dual-glob keeps both descriptions functional in between; PR 9's final pass greps for stale paths.
- **Manifest staleness/conflicts** — CI `--check` plus sorted per-source blocks keep it one command to fix and auto-mergeable.
- **Fixture rot** — fixtures become the offline contract-of-record; if they drift from the live API the suite proves the wrong thing. Countered: fixtures are captured from redacted live responses with the capture date recorded in the connector README, and the live duckdb rehearsal remains the add-source gate.
- **Git history churn** — two renames per connector; pure-move PRs preserve rename detection.

## 14. What explicitly does not change

- A connector is a spec; `source.yml` is the whole contract; `extensions:` is the one named escape hatch — now colocated and contract-documented.
- DAGs are generated; `source_dags.py` still reads YAML directly and never imports the spec parser; Airflow and the pipeline keep separate environments.
- Secrets: the spec names `token_env`; values live only in `.env`/SSM; connecting a credential is one line, no compose edit.
- `make test` stays offline — the harness strengthens this.
- `raw_<source>` derived, never configured; one pool of 1 per source; ingest through Airflow; an empty `sources/` schedules nothing but `stack_smoke`.
- Expectations generated from the spec; identity checks not opt-in; advisory-vs-gating semantics.
- The `ingest`/`dq` CLI surface is extended (`validate`, `manifest`, `scaffold`) — never broken.
- The skills remain the operating procedure; add-source remains research → agree → generate → prove.

## Appendix: repo hygiene noticed in passing

Untracked `.claude/worktrees/docs+readme-ports/` (a full duplicate checkout that doubles every repo-wide grep); untracked `metabase/` directory; a stray 7 MB `swoogo_duckdb.duckdb` at the repo root; ghost `__pycache__`-only trees under the pre-rename package names (`pipeline/src/pylon_pipeline/`, `quality/src/pylon_quality/`). None block anything; all are one `rm -rf` from clean.
