# Infrastructure proposal: the orchestration platform after the single box

**Status:** proposal for discussion · **Date:** 2026-08-04 · **Scope:** Data Stack 2.0 goal #2 (ingestion + orchestration infrastructure)
**Companion:** [connector-modularization-proposal.md](connector-modularization-proposal.md) — the repo-layout refactor this design consumes.

---

## 1. Executive summary

Move the orchestration layer from one docker-compose EC2 instance to:

- **Airflow control plane on ECS Fargate** — two small always-on services (apiserver; scheduler + dag-processor + triggerer) plus an RDS Postgres metadata database.
- **Every data run as an ephemeral Fargate task**, launched by a deferrable `EcsRunTaskOperator` running a dedicated `data-runtime` image (pipeline + quality, no Airflow). The repo's "DAGs shell out, never import" rule becomes an image seam instead of a venv seam.
- **dlt state stored in the destination** (ClickHouse Cloud), restored at task start. The cursor commits inside the same load as the data, which rebuilds the `warehouse-data`+`dlt-state` matched-pair invariant *by construction* — an invariant the move to ClickHouse Cloud silently breaks under every other option, including doing nothing.
- **Secrets stay in SSM Parameter Store**, but each source's task definition injects only that source's token. Rotation becomes "push the new value; the next task launch picks it up."
- **Deploys become image rolls** (GHA → ECR, tag = git SHA). The quiesce/pause/worktree/reset machinery in `scripts/stack_update.sh` mostly dissolves rather than being ported.
- **Zero public ingress preserved**: datateam VPC, PrivateLink to ClickHouse Cloud and to the Store Aurora reader, Tailscale for team access.
- **The laptop compose stack is unchanged** and remains the dev environment. The EC2 box is demoted to dev/dogfood, not deleted — decision revisited at day 90.

**What this is argued on** — and, just as deliberately, what it is not:

| Argued on | Explicitly NOT argued on |
|---|---|
| The deploy model dies at ~30 sources (§4.1) | Capacity — one bigger box runs this workload (§4.4) |
| Per-source credential scoping is impossible today (§4.2) | Cost — the delta is noise against three engineers (§9) |
| Host-ops elimination for a 3-person team; single-AZ / host-as-pet risk (§4.3) | Agent-operability — roughly neutral either way |
| Org platform direction is ECS ("Coredev, set up ECS") | |

Two **spike gates** must pass before commitment (§8, Phase 0). If the state spike fails, the fallback is a hardened single box with image-per-task execution (§7.1) — most of this proposal's mechanics survive that outcome.

## 2. Context and fixed decisions

Data Stack 2.0 replaces Fivetran with dlt, dbt with Metabase Transforms, and the Postgres/Redshift warehouses with ClickHouse. This repo is the working prototype: spec-driven connectors (`sources/*.yml`) generate Airflow DAGs, dlt lands data in `raw_<source>` databases, Great Expectations validates arrival, and results land in `ops.*`. Two sources run today on one `m8g.xlarge` via docker compose.

Fixed before this proposal (not relitigated here):

1. **Warehouse = ClickHouse Cloud** (the stats2 instance's warehouse). Warehouse compute, storage, and query concurrency are its problem; the [query-usage analysis](https://clickhouse.com/docs/guides/sizing-and-hardware-recommendations) done against stats showed a small cluster suffices (peak 4.3 QPS; concurrency driven by a slow-query tail that ClickHouse collapses). This document covers ingestion and orchestration only.
2. **Platform = AWS, ECS direction.** MWAA and EKS appear only as considered alternatives (§7.2).
3. **Orchestrator = Airflow.** The spec → generated-DAG machinery is built and working; the Dagster and Argo questions from earlier meetings are settled by sunk, working code, not by tool preference.
4. **No modeling in this stack.** Transforms are scheduled by Metabase. Only the seam is discussed here (§6.5).

## 3. The workload this must carry

From the source tracker spreadsheet, the migration-specs document, and an inventory of `data-tools` and `dbt-models`:

- **~25–30 spec-driven REST sources**: Salesforce and Google Drive at 15-minute cadence, most hourly, Swoogo 2h, Slack/Luma 6h, several daily. Each source generates ingest + backfill + reconcile DAGs with a per-source pool of 1.
- **~560 task executions/day of existing `data-tools` jobs** to absorb: mostly light, rate-limit-bound dlt API pulls on GitHub Actions cron; pylon runs multi-hour (paced by per-endpoint RPM budgets); a timeline-sync job every 10 minutes; reverse-ETL jobs (customer.io segment sync via pandas CSV downloads, mailchimp maintenance, pylon_link — which today persists its watermark in `actions/cache`, an evictable store).
- **metabase_store / harbormaster — the single biggest constraint.** Today a container-image Lambda inside the datateam VPC, reaching the Store Aurora reader over PrivateLink → NLB → DB proxy on port 5434. 51 tables plus audit; a 170.9 GB full snapshot; memory-bound (it OOM-killed a 3008 MB Lambda); and its entire chunking/truncation machinery (`CHUNK_SIZE=5000`, `--max-rows`, `--max-time`) exists *only* because of the 900-second Lambda ceiling. A run killed mid-load permanently duplicates its keyless append tables — task-stop semantics are a first-class design input here (§5.4).
- **metabase-events**: ClickHouse→ClickHouse `remoteSecure()` `INSERT … SELECT` — near-zero worker memory. A pattern to keep, not rewrite.
- **One-time historical bulk loads, ~6.9 TiB** (680 tables, ~66 B rows): Snowplow Redshift 2.55 TiB, db 26 Postgres 2.62 TiB, Hosting Insights Redshift 1.71 TiB; `query_execution` alone is 1 TiB / 9.1 B rows. These are supervised events, not DAGs (§6.4).
- **450 dbt models** migrate to Metabase Transforms, scheduled by Metabase — outside this stack by rule; the coupling question is §6.5.

Arithmetic the rest of the document leans on: ~720–900 scheduled ingest runs/day at target state, ~2–3 tasks per run, plus absorbed jobs ≈ **4–5k Airflow task instances/day ≈ 3 task starts/minute**. Small for Airflow by an order of magnitude. Peak concurrent memory (top-of-hour, with the schedule staggering the specs already practice): roughly 10–30 concurrent runs × 300–500 MB dlt processes, plus one 8 GB store run serialized by its own pool.

## 4. What actually breaks on one box — and what doesn't

The current deployment is better engineered than "a box with compose" suggests: zero-ingress SG with SSM-only access, IMDSv2 hop-limit 1, desired-SHA-in-Parameter-Store deploys with a converge timer, ancestry-gated deploy script installed outside the tree, matched-pair EBS snapshots. The problems below are scale problems, not craft problems.

### 4.1 The deploy model dies at ~30 sources

`scripts/stack_update.sh` requires a quiesce window: it defers immediately if any backfill/reconcile is running or queued, then polls up to 25 minutes for **zero** `%_ingest` DAG runs in flight before it will touch the tree. Today, with two sources at `:23` hourly and `:41`/6h, that window opens constantly. At 30 sources — two of them on 15-minute cadence, pylon rate-paced for hours, reconciles with 1440-minute timeouts — the probability of a zero-in-flight moment goes to approximately zero. The converge timer (`ds-converge.timer`, every 5 minutes) would defer forever and eventually write its own `HOLD` after three failures. Deploys land only via `--allow-in-flight`, which `git reset --hard`s a bind-mounted editable install *under running tasks* — the exact failure class the worktree-build dance exists to prevent.

The honest conclusion, and the load-bearing argument of this proposal: **what dies is live-tree execution, and what fixes it is image-versioned task execution** — tasks run a tagged, immutable image; a deploy is an image roll; in-flight tasks finish on the image they started with. ECS is the org-aligned way to get that property. (A `docker run`-per-task scheme on the box gets it too — that is exactly the fallback in §7.1.)

### 4.2 Per-source credential scoping is impossible as built

Every secret for every source lands in one `.env` read wholesale by every container (`docker-compose.yml`'s `env_file`). A Swoogo task can read the Salesforce token. Rotation means re-render plus `--force-recreate` of the entire stack, because container environments freeze at create time. This is structural: compose has no per-task secret injection. ECS task definitions do (§5.6).

### 4.3 Single AZ, host-as-pet, and team access

- EBS is AZ-bound; the stated recovery contract is up to 24 h of data at risk and an hour or two of work. Defensible for a prototype; not for the pipeline the company's analytics ride on.
- The checkout on the box is live code. `make hold`, the dirty-tree patch file, and the branch check are mitigations for "a human edited production" — the failure mode remains open as long as production *is* a checkout.
- Team access is per-person SSM tunnels to an Airflow running `SimpleAuthManager` with **all-admins**. There are no user accounts to scope; the third engineer gets the same everything as the first.

### 4.4 What does *not* break: capacity and cost

Said plainly so the proposal doesn't oversell: **a single `m8g.2xlarge` (8 vCPU/32 GB) runs this workload.** 3 task starts/minute is nothing; peak concurrent memory closes within 32 GB once ClickHouse (the largest tenant, 5 GB reserved) moves to Cloud; the 6.9 TiB bulk load is `s3Cluster()` work on ClickHouse Cloud's compute, not the box's. And cost is a wash: the target lands at ~$310–360/mo (§9) against ~$150/mo for the current box or ~$280/mo for the hardened one. Neither capacity nor cost decides this; §4.1–4.3 do.

## 5. Target architecture

```
GitHub (merge to main)
  └─ GHA: build ds/airflow + ds/data-runtime → ECR (tag = git SHA, SOCI index)
       └─ deploy: db-migrate one-off task → register task defs → update-service

ECS cluster ds-prod (datateam VPC, private subnets, zero public ingress)
  ├─ service airflow-web    (apiserver, 0.5 vCPU/2GB)  ← internal ALB ← Tailscale
  ├─ service airflow-core   (scheduler + dag-processor + triggerer, 1 vCPU/4GB)
  ├─ RDS Postgres db.t4g.small (Airflow metadata, PITR)
  └─ ephemeral tasks (data-runtime image, per-source task definitions)
        ├─ <source>_ingest / _backfill / _reconcile  (EcsRunTaskOperator, deferrable)
        └─ verify (dq), watchdog, sweeps
             │
             └─ ClickHouse Cloud (PrivateLink): raw_<source>.*, ops.*, _dlt_* state
                   └─ Metabase (stats2) reads raw_* + ops.*
```

### 5.1 Control plane

- **`airflow-web`**: the Airflow 3 apiserver. Health check `/api/v2/monitor/health` behind an internal ALB. It also serves the Task Execution API, so `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` points at this service's internal name — scheduler subprocesses are no longer co-resident.
- **`airflow-core`**: one task definition, three containers (scheduler, dag-processor, triggerer). They deploy as a unit anyway (same image, same SHA); one service halves the Fargate baseline; per-container log streams keep logs separated. Splitting later is a terraform refactor, not a redesign.
- **`airflow-init` becomes a one-off ECS task** run by the deploy pipeline before `update-service`: `airflow db migrate` plus the pool bootstrap ported from the compose block. **Pools must be created with `--include-deferred`.** With deferrable operators, a deferred ingest otherwise stops counting against its pool, and a second run of the same source can launch while the first's container is still loading — precisely the cursor race the pool-of-1 exists to prevent. This is the single most important configuration flag in the migration; CI should assert it.
- **Metadata DB**: RDS Postgres 16, `db.t4g.small`, 20 GB gp3, single-AZ with 7-day PITR and deletion protection. Single-AZ is defensible because the platform tolerates an hour of scheduler downtime — cursors live in the destination and simply resume. Multi-AZ is a one-flag toggle (+~$25/mo) to revisit when business-visible cadences tighten.
- **DAG delivery: baked into the image.** `COPY airflow/dags sources/` into `ds/airflow`; a deploy is an image roll; the git SHA is the image tag is what's running. Scheduler and dag-processor can never disagree about DAG code, and Airflow 3's DAG versioning gets a clean bundle per deploy. The `ds-deploy` ancestry gate's spirit survives as "only main-descended digests receive the prod tag" (ECR tag immutability + GitHub environment protection). Rejected: S3 bundle sync (a second artifact to keep consistent — the specs must reach the data-runtime image anyway, so you rebuild regardless); EFS-mounted DAGs (shared mutable state).

### 5.2 Task execution: deferrable `EcsRunTaskOperator`, and only that

Generated DAG tasks become thin wrappers that call `ecs.RunTask` on the data-runtime image and defer to the triggerer while the container runs.

- The repo's hard rule — *DAGs shell out; they never import the pipeline packages* — is already the containerized-task pattern. `common.py`'s `ingest_command()` becomes the container `command` verbatim; the `/opt/data-venv` seam becomes an image seam, which is strictly stronger.
- Pools, `max_active_runs=1`, retries, and timeouts keep working unchanged — they gate at scheduling time, before RunTask is called.
- Deferred wrappers cost ~nothing while a multi-hour pylon or backfill container runs. `AIRFLOW__CORE__PARALLELISM` goes from 4 to ~32 and stops being the constraint it is today (`docker-compose.yml:26`).
- `reattach=True` on the operator: a scheduler deploy that kills a wrapper mid-flight re-attaches to the still-running ECS task on retry instead of launching a duplicate against the same cursor.
- The change is localized: `common.py` grows an operator factory emitting `BashOperator` when `DS_EXECUTION_MODE=local` (laptop, unchanged) and `EcsRunTaskOperator` when `ecs`. `source_dags.py` calls the factory. One seam, both environments generated from the same spec.

**Rejected — AWS ECS Executor**: it runs the Airflow task machinery inside the task container, so the data image must contain Airflow (and reach the execution API). That re-merges the two dependency trees the entire two-venv `docker/airflow/Dockerfile` design exists to keep apart, and couples every task-image rebuild to Airflow upgrades. **Rejected — CeleryExecutor + ElastiCache**: adds a stateful broker and forces worker sizing to the worst-case task (the store job's 8 GB would size the whole fleet).

New failure modes to own honestly: the triggerer becomes load-bearing (a crashed triggerer leaves tasks deferred — it restarts under ECS and trigger state lives in the DB); RunTask/DescribeTasks eventual-consistency flakes (operator retries cover); a timeout-while-deferred must reliably stop the ECS task — this is spike gate #2, with an ops sweep for zombie tasks as belt-and-braces.

### 5.3 Runtime tiers, declared in the spec

A new `orchestration.runtime:` field (schema and manifest defined in the [companion proposal](connector-modularization-proposal.md)) maps to Fargate task sizes; the DAG generator and terraform both read it:

| Tier | Fargate size | Who |
|---|---|---|
| `light` (default…) | 0.25 vCPU / 1 GB | rate-limit-bound API pulls, `dq` verify, watchdog |
| `standard` (…for new specs) | 1 vCPU / 2 GB | fan-out sources (swoogo), most backfills |
| `long` | 0.5 vCPU / 2 GB | pylon — multi-hour, paced, low memory |
| `heavy` | 2 vCPU / 8 GB + 50 GB ephemeral | metabase_store snapshots, pandas reverse-ETL |

Backfill/reconcile take an optional override. The store job's 900-second truncation machinery is **deleted, not ported** — the ceiling that justified it no longer exists.

### 5.4 DAG shape, stop semantics, and the duplicate-append lesson

- **Fold `record_ops` into the ingest container.** A `run-and-record` entrypoint runs `ingest run --summary-json …` then `dq record-run` with the real status, exiting with ingest's code. This preserves "a failed run is still recorded" without the filesystem handoff (`/opt/dlt-state/run-summaries`, `common.py:23-26`) that has no home on Fargate, and cuts launches to two per run: `ingest_and_record (ECS) → verify_raw (ECS) → run_verdict (Empty)`. An OOM-killed container also kills its recorder — the same observable outcome as today's no-summary path (red DAG, absent ops row). Alternative: one container per DAG run (ingest→verify→record internally) — fewest cold starts, coarser retry granularity; worth adopting for `light`-tier sources if launch overhead ever matters.
- **Stop semantics**: ECS sends SIGTERM, then SIGKILL after `stopTimeout` — set to the Fargate maximum, 120 s, on every runtime task definition. The ingest CLI gains a SIGTERM handler: stop extracting, finish or cleanly abandon the load within the window. Loads run on on-demand Fargate, never Spot (Spot is an opt-in lever for backfills only, where merge idempotency makes interruption safe).
- **The harbormaster lesson, fixed structurally**: a task killed mid-load loses its pending package; the next run restores the last *committed* state and re-fetches from the previous cursor. For merge-disposition tables (the spec default) that is idempotent. For keyless append tables it can duplicate rows from the crashed package's completed jobs — so (1) the spec lint makes `append` require explicit justification, and (2) a weekly ops sweep deletes rows whose `_dlt_load_id` has no completed entry in `_dlt_loads` — the standard dlt cleanup for non-transactional destinations, and the surgical fix the Lambda never had.

### 5.5 dlt state: destination-stored, and why the alternatives are rejected

**The matched-pair invariant is already broken by the ClickHouse Cloud decision** — under *every* option, including keeping the box: the cursor would sit on an EBS volume in one AZ while the data it describes lives in a SaaS warehouse. No snapshot choreography can make those atomic again.

dlt already commits pipeline state into the destination (`_dlt_pipeline_state`, alongside `_dlt_loads`/`_dlt_version`) as part of every load, and restores it when the local working directory is empty; the ClickHouse destination implements state sync. So: each Fargate task starts with an empty `DLT_DATA_DIR` on ephemeral storage, restores cursor + schema from ClickHouse Cloud, runs, and **the state advances atomically with the load that carries it**. Consequences:

- Matched pair *by construction*: a ClickHouse Cloud backup restores rows and cursor together. The `dlt-state` volume concept, the EBS snapshot pairing (`terraform/backup.tf`), and the drain-a-pending-package path are deleted, not migrated.
- Import schemas (the reviewed overrides `airflow-init` seeds today) bake into the data-runtime image — read-only, versioned with the deploy. Export schemas become ephemeral; the "schema evolution as a git diff" loop weakens to per-run schema-change reports in logs plus an optional S3 artifact upload for diffing. The companion proposal's per-connector `schemas/import/` directory is where the reviewed schemas live in git.
- **Rejected — EFS**: dlt's extract/normalize writes thousands of small files per run; NFS round-trips and POSIX-locking-over-NFS is the pathological workload. **Rejected — S3 sync wrapper**: DIY crash-consistency with a last-writer-wins race under zombie-task retries; strictly worse than the durability dlt's own transaction already provides.

**Spike gate #1 (Phase 0) — prove on ClickHouse Cloud before commitment:** (a) cursor restore round-trips for *extension-built* resources (swoogo's `parent_fanout`, pylon's watermark — state written via source-state, not declarative config); (b) dlt tolerates a read-only baked import-schema dir with export redirected; (c) kill a task mid-load with SIGKILL, rerun in a fresh container, count duplicates — merge tables must self-heal. If the spike fails, the fallback keeps cursors on a volume: §7.1.

### 5.6 Secrets

- **Stay on SSM Parameter Store.** The `laptop .env → make secrets-push → /data-sovereignty/prod/*` flow survives byte-for-byte; standard parameters are free; ECS injects them natively via `secrets: [{name, valueFrom}]`. Secrets Manager buys rotation lambdas this stack doesn't need at $0.40/secret/mo — declined.
- **Per-source injection fixes §4.2.** Terraform generates one task definition per source from the connector manifest ([companion proposal §8](connector-modularization-proposal.md)), injecting exactly: that source's `token_env` parameter, the `DESTINATION__CLICKHOUSE__CREDENTIALS__*` set (now pointing at ClickHouse Cloud with `SECURE=1` — the runtime already supports this end-to-end; today's compose block simply overrides it to `warehouse-db`, `docker-compose.yml:49-61`), and generic runtime vars. A Swoogo container can no longer read the Salesforce token. One shared execution role with `ssm:GetParameters` on the prefix is the pragmatic middle; 30 per-source IAM roles are not worth it for 3 engineers — the isolation win is what reaches each process env, and that is now per-source.
- **Airflow's own secrets** (DB conn, fernet key, JWT secret, Slack webhook) inject the same way into the two service task definitions; the amazon provider's Parameter Store secrets backend serves the few Airflow Connections this stack needs (mainly Slack).
- **Rotation**: push the new value; the next ephemeral task launch resolves it. No re-render, no `--force-recreate` of the world. Only rotating Airflow's own secrets needs `--force-new-deployment`.

### 5.7 Networking, access, and the pieces the draft designs usually forget

- **Place the cluster in the datateam VPC**, private subnets across 2 AZs. This inherits the existing Aurora PrivateLink path for harbormaster as-is — one SG rule from the runtime task SG to the interface endpoint on 5434.
- **Egress**: one NAT gateway for SaaS API calls. Cost lever: tasks with public IPs in a zero-ingress SG (the box's exact posture today) make NAT ~$0; the private-subnet+NAT default is the more conventional posture — pick one in review, it changes nothing else.
- **VPC endpoints are load-bearing, not hygiene**: ~1,500–2,000 Fargate launches/day each pull image layers; without `ecr.api`, `ecr.dkr`, the S3 *gateway* endpoint (free; carries the actual layer bytes), `logs`, and `ssm`, that traffic transits NAT at $0.045/GB — hundreds of dollars a month by itself, and the first thing a NAT-bytes alarm should watch.
- **ClickHouse Cloud via PrivateLink** — keeps warehouse traffic, including bulk loads, off the public path and off NAT metering.
- **Team access: Tailscale first.** It is already wired in terraform (`enable_tailscale`) and off; turn it on, run a subnet router advertising the VPC CIDR with split-DNS, and the team reaches the internal ALB by name. Preserves literal zero public ingress. ALB+OIDC is the industry standard but opens authenticated public ingress — contradicts the stated posture; SSM port-forwarding stays as break-glass. Keep `SimpleAuthManager`-behind-tailnet for three admins now; note FAB/OIDC as a later hardening step.
- **Metabase and data docs — decided, not forgotten**: `ops.*` moves to ClickHouse Cloud with the warehouse, so the ops dashboards belong on **stats2**, next to the data they describe. The box's Metabase EE remains the dev/dogfood instance (it is the product this stack exists to dogfood). GX data docs move to a private S3 bucket (reachable over the tailnet) or stay on the box — either is fine; pick in review.
- **The locality guard inverts and must be rewritten.** `pipeline/src/ingest_runtime/locality.py` refuses *loopback* warehouse hosts because a tunnel once beat Docker on 127.0.0.1 and rotated production's Metabase key from a laptop. With ClickHouse Cloud, the dangerous case flips: a laptop `.env` pointing at the Cloud hostname is *not* loopback and sails through. Keep the lesson, invert the check: refuse the production hostname unless an environment marker only the ECS task definitions set is present (`DS_ALLOW_HOST_INGEST` remains the deliberate override). This is a small code change that should land with Phase 2.

### 5.8 CI/CD and what happens to the deploy machinery

- **Two ECR repos**: `ds/airflow` (base Airflow 3 + `providers[amazon]` + DAGs + specs) and `ds/data-runtime` (pipeline + quality + specs + baked import schemas). **arm64 throughout** — ~20% cheaper Fargate, and building natively on GitHub's `ubuntu-24.04-arm` runners (which CI already uses for exactly this reason) removes the 15-minute on-box zstd compile. Flipping to x86_64 is one variable if arm runner supply ever bites.
- **Pipeline**: merge to main → build both images tagged `<git-sha>` → push + SOCI indexes → run the db-migrate/pool-init one-off task → register task-def revisions → `update-service` → post-deploy smoke as a one-off ECS task (`dags list-import-errors` empty; expected DAG ids registered — the checks ported from `stack_update.sh`; run in-cluster because GHA can't reach the tailnet).
- **What replaces `ds-deploy`/`stack_update.sh`: mostly nothing, and that is the point.** The quiesce window dissolves — in-flight ingest containers are independent tasks that keep running through a control-plane deploy; deferred wrappers survive scheduler restarts; `reattach=True` covers the rest. No DAG pausing, no worktree pre-build, no `.env` completeness scan (a missing parameter becomes a *plan-time* terraform error, strictly earlier than today's deploy-time check). Keep writing the deployed SHA to `/data-sovereignty/prod/deploy/desired_sha` as provenance — it costs nothing and keeps "an agent can ask what should be running." The box, if retained, keeps its existing machinery untouched.
- **Terraform layout**: `modules/network` (VPC data sources, endpoints, NAT, SGs), `modules/airflow` (cluster, RDS, ALB, services, init task), `modules/runtime` (per-source task definitions generated from the connector manifest, log groups, tier map), `modules/secrets`, `modules/observability`. CI runs `plan` when `sources/` or `terraform/` change — a new spec now implies a new task definition, so "add a source = spec + secret" honestly gains a third step: an auto-applied plan.
- **Environments**: laptop compose is dev, unchanged. **No standing staging** for a 3-person team — CI (DAG-import tests, unit tests, duckdb smoke) plus canary rollout (enable one source first after risky changes) covers it, and an ephemeral `terraform workspace` is the staging-on-demand, which doubles as the DR rehearsal that makes "recreate the stack from scratch" a tested claim instead of a slogan.

### 5.9 Observability and alerting

`ops.pipeline_runs` + `ops.gx_results` in the warehouse, with Metabase dashboards on top, stay the primary lens — self-observability in the warehouse is a feature of this design, not a gap to fill with vendor tooling. Around it:

- **Logs**: one CloudWatch group per Airflow container; one `/ecs/ds-runtime` group with per-source stream prefixes, 30-day retention; the wrapper's `awslogs` config surfaces the container tail inside the Airflow task log; Airflow remote task logging to CloudWatch so logs survive service replacement. Runtime default log level stays WARNING — log volume is a real cost line.
- **Slack**: `on_failure_callback` on generated DAGs via the Slack webhook connection — the same channel habit `data-tools` already has.
- **Freshness**: Airflow 3 removed classic SLAs; freshness SLOs already live in the specs (`quality.freshness`). A generated `ops_watchdog` DAG (hourly, light tier — generated, honoring the no-hand-written-DAGs rule) compares `ops.pipeline_runs` and freshness verdicts against each spec's SLO and posts staleness to Slack. This watchdog is also how the Transforms seam is verified (§6.5).
- **Alarms, few and meaningful**: ALB target health on the apiserver, RDS storage/CPU, NAT-bytes anomaly (catches an endpoint misroute before the bill does), RunTask failure count.
- **Retry posture**: batch, not streaming — spec-declared retries with delay; RunTask launch failures retried by the operator; no literal DLQ. With destination-stored state, failed packages no longer persist as artifacts — the run summary and logs carry the error; the orphan-load sweep (§5.4) is the cleanup.

## 6. Deliberately not Airflow DAGs

Part of the proposal is what stays *out* of the orchestrator:

1. **metabase_store / harbormaster**: migrate as **dlt-in-Fargate on the heavy tier** (a thin `jobs/` DAG + dlt's `sql_database` source; the ceiling-free runtime deletes the Lambda's truncation machinery), with **ClickPipes CDC as the negotiated end-state**. ClickPipes deletes the problem class — no rescans, minutes-fresh, zero worker memory — but requires logical replication: a slot on the *writer*, `wal_level=logical`, and it cannot work through today's reader-proxy path. A stalled slot retains WAL on the production Store database, which makes this **the Store team's risk to accept, not ours to assume**. Run the evaluation with them in parallel; switch if writer access is granted.
2. **AWS CUR + CloudFront logs**: already in S3 — ClickHouse-native ingestion (`s3()` at transform time, or an `S3Queue`/refreshable materialized view for continuous landing, with an IAM role granted to ClickHouse Cloud). At most a thin DAG issuing one `INSERT … SELECT FROM s3()` if scheduling visibility is wanted.
3. **metabase-events**: keep the `remoteSecure()` pattern; a thin light-tier DAG that issues the SQL is fine — the DAG is the schedule and the audit trail; the warehouse does the work.
4. **One-time bulk historical loads (~6.9 TiB)**: runbook scripts in `docs/runbooks/`, not DAGs — export → S3 parquet → `s3Cluster()` for the Redshift sources; a temporarily oversized one-off task for db 26. A DAG implies repeatability; these are supervised events with human checkpoints.
5. **timeline-sync (10-minute cadence)**: EventBridge Scheduler → RunTask directly — a stateless sync gains nothing from Airflow, and a 60–90 s Fargate cold start against a 10-minute interval is a 10–15% duty-cycle tax. (The 15-minute Salesforce/Drive DAGs are fine: the same overhead against 15 minutes is acceptable, helped by SOCI and a lean runtime image.)
6. **The Transforms seam — decoupled, with a sign-off gate.** Metabase schedules its own Transforms. The acceptance criterion "whole pipeline can run hourly or less" is met by ingest cadences ≤ 1 h plus transform cadences ≤ 1 h, and *verified* by the freshness watchdog — not enforced by coupling. Airflow firing transform runs via mb-cli would buy tighter end-to-end latency at the price of crossing the repo's "no modeling here" boundary and importing a fan-in problem whose knowledge (which sources feed which transform) lives in Metabase, not in specs. If the team later wants it, the deliberate minimal version is a generic post-ingest notification ("source X fresh as of T") that Metabase-side automation may consume — flagged here as a decision requiring explicit sign-off, and out of scope for this proposal.

## 7. Alternatives considered

### 7.1 Hardened-EC2 v2 — the honest fallback

One `m8g.2xlarge` (~$280/mo with EBS), ClickHouse gone to Cloud (freeing the largest memory tenant), and the shell-out seam upgraded: each task becomes `docker run --memory=<tier>` of the data-runtime image — image-versioned task execution *on the box*, which fixes the live-tree half of §4.1. Add Tailscale and an auth-manager swap for team access. **What it cannot fix**: per-source secret scoping (one wholesale `.env` remains), single-AZ hours-long RTO, the Docker socket reachable from the scheduler container as a privilege-escalation surface, core-image deploys still needing a quiesce, and the host remaining a pet. It avoids roughly 60% of the ECS migration effort and is the designated landing zone if the state spike fails (cursors stay on the volume) or if scope shrinks to ≤10 sources.

### 7.2 MWAA — corrected, and taken seriously

**Correction to the obvious objection: MWAA supports Airflow 3.2 (April 2026) and 2.11.2 (July 2026).** The repo pins 3.3.0 — a one-minor step, and the generated-DAG surface is 3.2-compatible. Version support is *not* the blocker. The real ones: **no custom worker images** (requirements.txt + startup script only), which kills the shell-out design *on MWAA workers*; the ~$358/mo environment floor before any task compute; and forced version marches on AWS's EOL schedule.

The serious variant is therefore **MWAA as control plane + `EcsRunTaskOperator` for every real task**: image separation preserved entirely, managed multi-AZ scheduler, and IAM-authenticated UI out of the box — deleting the DIY auth/Tailscale work. It costs ~$300/mo more than self-managed core for the same task execution. **Verdict**: not now — but it is the named flip-condition if the team drops below three or loses its infra-comfortable engineer. The task-execution layer (this proposal's §5.2–5.6) is identical either way, so this door stays open at low cost.

### 7.3 Status-quo-plus — what "do nothing" actually costs

GHA keeps running the ~20 scripts; Airflow orchestrates only the new spec sources on the box. This is the default if the proposal stalls, so its costs are named: **pylon already exists in both repos** — the data-tools hourly job and this repo's spec — meaning two schedulers pacing one API budget with two cursor stores, a standing 429/cursor-race, not a hypothetical; the GHA 6-hour job ceiling blocks pylon backfills; `actions/cache` evicts the pylon_link watermark; **metabase_store can never run on GHA** (runners can't reach PrivateLink), so the Lambda and its ceilings persist as a third runtime; secrets stay split across GH Secrets and SSM with independent rotation; observability stays split across Slack-on-red and `ops.*`. The dual-run window during migration must be **bounded and short** for exactly the first reason.

### 7.4 Decision framework

| Requirement | Status-quo-plus | Hardened-EC2 v2 | **ECS target** | MWAA 3.2 + ECS tasks |
|---|---|---|---|---|
| Memory isolation per task | none on GHA | good (cgroups) | **best** | best |
| Deploy without quiesce | half | mostly (core still quiesces) | **yes** | yes |
| Per-source secret scoping | no | no | **yes** | yes |
| Blast radius / AZ | box + GHA split | one box | **task/service/AZ isolated** | managed core |
| VPC reach (Aurora PL) | GHA: impossible | yes (box in VPC) | **yes** | yes |
| Backfill throughput | 6 h GHA ceiling | box RAM cap | **elastic** | elastic |
| Cursor/state durability | 3 stores, one evictable | volume snapshots | **committed with data** (spike-gated) | same as ECS |
| Cost /mo | ~$150 | ~$280 | **~$310–360** | ~$450–550 |
| Ops burden (3 people) | two systems forever | one pet host | **terraform+ECS surface, no host** | lowest core ops, forced marches |
| Recreate from scratch | undefined for GHA half | hours (snapshot restore) | **<1 h (apply + pull; state in warehouse)** | ~similar |

**Three conditions that flip the recommendation**: (1) the state spike fails → Hardened-EC2 v2; (2) scope shrinks to ≤10 sources / data-tools absorption is cancelled → Hardened-EC2 v2; (3) the team loses its infra-comfortable engineer → MWAA hybrid.

## 8. Migration plan

| Phase | What | Effort | Rollback |
|---|---|---|---|
| **0** | **Spikes (gates)**: dlt destination-state restore on CH Cloud incl. extension-resource cursors + mid-load-kill duplicate count; deferrable EcsRunTask timeout-kill behavior; CH Cloud PrivateLink; arm64 CI build | ~1 wk | n/a |
| 1 | Foundations: terraform modules (network/endpoints, ECR, RDS, cluster, secrets), Tailscale router, CI image builds | 1–2 wk | `terraform destroy` |
| 2 | Airflow on ECS running swoogo + customerio against **CH Cloud**. Parallel-run is safe by construction: box→local CH and cloud→CH Cloud share no cursor. Locality-guard rewrite lands here. Validate a week of freshness + GX verdicts | 1–2 wk | box never stopped |
| 3 | Absorb data-tools GHA jobs, one source per PR: REST-shaped jobs (octolens, grain, stripe, contrast, athenahq, apollo, unify…) become specs; disable each GHA cron only after N green cloud runs | 2–4 wk elapsed | re-enable the cron, per source |
| 4 | Non-spec-shaped jobs → `jobs/` pattern (a deliberate, documented exception class of thin DAGs running the containerized data-tools image): reverse-ETL, pylon_link (watermark moves from actions/cache to the warehouse/S3); timeline-sync → EventBridge. Moving them beats leaving them: one pane, ops rows, secrets hygiene | 1–2 wk | re-enable crons |
| 5 | harbormaster: heavy-tier `jobs/` DAG + dlt `sql_database`; delete truncation machinery; parallel-run against the Lambda comparing counts; ClickPipes evaluation with the Store team in parallel | 2–3 wk | Lambda trigger re-enable |
| 6 | Bulk historical loads via runbooks; ClickPipes decision point | spread out | per-runbook |
| 7 | Box disposition: keep as sovereign dev/dogfood stack + Tailscale router host; optionally downsize. **Decide retire-vs-keep at day 90 with cost data in hand** | trivial | n/a |

## 9. Cost (us-east-1, monthly, arm64)

| Line | Est. |
|---|---|
| Always-on Fargate (core 1 vCPU/4 GB + web 0.5/2) | ~$50 |
| RDS db.t4g.small single-AZ + storage + backups | ~$27 |
| NAT gateway + modest egress (or ~$0 with public-IP posture, §5.7) | ~$36 |
| VPC interface endpoints + CH Cloud PrivateLink | ~$45 |
| Internal ALB (or $0 with Tailscale-only) | ~$18 |
| Ephemeral task-hours (~800 ingests/day × ~5 min + verifies + pylon ~5 h/day + store hourly heavy + cold starts) | ~$100–130 |
| CloudWatch logs (discipline-dependent) | ~$25–45 |
| ECR + S3 artifacts | ~$5 |
| **Total** | **~$310–360** |

Against ~$150/mo for the current box (which partially stays as dev) plus the invisible lines: ~560 GHA job executions/day and the Lambda. The MWAA floor alone (~$358) exceeds this design's entire always-on footprint. **At a delta under $200/mo against three engineers' time, cost decides nothing** — levers if wanted: Fargate Compute Savings Plan (~20%), public-IP egress posture, log-level discipline.

## 10. Risks

1. **State spike fails** (extension-cursor restore, read-only schema dir, kill-test duplicates) → gated in Phase 0; fallback designed (§7.1).
2. **`--include-deferred` missed on pools** → cursor race returns. CI assertion + init-task check.
3. **Timeout-while-deferred fails to stop the task** → zombie container writes past its window. Spike-verified; zombie-sweep DAG as backstop.
4. **NAT surprise** — image pulls or CH traffic miss the endpoints. Endpoints in terraform + NAT-bytes alarm from day one.
5. **Append-table duplicates on mid-load kill** → merge-by-default lint, SIGTERM handler, 120 s stopTimeout, orphan-load sweep.
6. **Cold start erodes 15-min cadences** as images fatten → SOCI, lean runtime image, schedule staggering (already spec convention), p95 start latency on the ops dashboard; hot capacity only if breached.
7. **Aurora proxy fragility on multi-hour snapshots** → keyed-chunk resumability, per-chunk retries, Store-team coordination; ClickPipes as the structural fix.
8. **Terraform-reads-manifest coupling** — a bad spec fails infra CI → schema-validate specs before plan (companion proposal §7); task-def generation pure and unit-tested.
9. **Airflow 3-on-ECS novelty for 3 people** → migrations as an explicit pipeline step, digest-pinned images (existing habit), runbooks, the box as a familiar reference environment.
10. **Scope creep at the Transforms seam** → recorded as decoupled-with-SLOs; any crossing requires explicit sign-off (§6.5).

## 11. What survives regardless of platform

The genuinely good ideas move, they don't die: spec-as-contract, extended to task definitions; generated DAGs; the shell-out seam (it *is* the migration path — `ingest_command()` becomes the container command verbatim); pools-of-1 and `max_active_runs=1` (more load-bearing once state is read-modify-write via the warehouse); record-on-failure ops discipline; the zero-ingress posture; desired-state deploys reborn as main-descended image tags; digest-pinned base images now built in CI; the matched-pair invariant reborn as destination-stored state; the locality guard's lesson with inverted logic; and the port-collision guards, which stay relevant exactly as long as laptop compose stays the dev environment — which it does.

## References

- Source tracker spreadsheet ("Migrating data pipelines to ClickHouse", Fivetran→dlt sheet) — cadences and migration methods per source.
- `analytics-warehouse-clickhouse-migration-specs.pdf` — 680-table inventory, physical design, §8 migration playbook.
- Analytics DW query-usage analysis (2026-07-06) — demand shape and CH sizing; conclusion: config over hardware.
- [MWAA: Airflow 3.2 support](https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-mwaa-now-supports-apache-airflow-3-2/) · [Airflow 2.11.2](https://aws.amazon.com/about-aws/whats-new/2026/07/amazon-mwaa-now-supports-apache-airflow-version-2-11-2/) · [MWAA pricing](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/)
- [dlt: pipeline state](https://dlthub.com/docs/general-usage/state) · [dlt ClickHouse destination](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse)
- [Airflow ECS Executor docs](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/executors/ecs-executor.html) (rejected option) · [Fargate SOCI](https://aws.amazon.com/blogs/aws/aws-fargate-enables-faster-container-startup-using-seekable-oci/)
