---
name: pipeline-status
description: Answer "is the pipeline healthy?" end to end — services, recent runs, freshness, data-quality verdicts, and transform history. Triggers — "is my pipeline healthy?", "verify the whole state", "what's the status?", "is the data fresh?", "did last night's run work?", "what's broken?".
allowed-tools: Bash, Read
---

# Reporting pipeline state

The user is asking one question: **can I trust the numbers right now?** Answer
that first, in a sentence, then show the evidence.

Work down the chain and stop early when you find the break — a stopped container
explains stale data, so there is no point reporting the staleness separately as
if it were a second problem.

## 1. Services

```bash
make status
```

Anything not `healthy`/`running` is the answer; go to `stack-ops`.

## 2. Recent runs

```bash
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d pylon_ingest_hourly --limit 5
```

The hourly DAG runs at :17. More than ~90 minutes since the last success means
the scheduler is stuck or runs are failing.

## 3. Everything else, in one query

`ops` exists so this is a single round trip rather than five:

```bash
make psql <<'SQL'
\pset pager off
SELECT 'last ingest'   AS signal,
       to_char(max(recorded_at), 'YYYY-MM-DD HH24:MI') AS at,
       (SELECT status FROM ops.pipeline_runs ORDER BY recorded_at DESC LIMIT 1) AS detail
  FROM ops.pipeline_runs
UNION ALL
SELECT 'data freshness',
       to_char(max(updated_at), 'YYYY-MM-DD HH24:MI'),
       age(now(), max(updated_at))::text
  FROM raw_pylon.issues
UNION ALL
SELECT 'quality (24h)',
       to_char(max(validated_at), 'YYYY-MM-DD HH24:MI'),
       count(*) FILTER (WHERE NOT success) || ' failed of ' || count(*)
  FROM ops.gx_results WHERE validated_at > now() - interval '24 hours'
UNION ALL
SELECT 'transforms',
       to_char(max(started_at), 'YYYY-MM-DD HH24:MI'),
       count(*) FILTER (WHERE status <> 'succeeded') || ' not succeeded of ' || count(*)
  FROM ops.mb_transform_runs WHERE started_at > now() - interval '24 hours';
SQL
```

If any signal is bad, drill in:

```sql
-- what exactly failed
SELECT asset, expectation, column_name, observed_value
  FROM ops.gx_results
 WHERE NOT success AND validated_at > now() - interval '24 hours'
 ORDER BY validated_at DESC LIMIT 20;

-- transforms that did not succeed
SELECT transform_name, status, started_at, message
  FROM ops.mb_transform_runs
 WHERE status <> 'succeeded' ORDER BY started_at DESC LIMIT 10;
```

## 4. Point them at the dashboard

Everything above is also the **Pipeline Health** dashboard in Metabase
(http://localhost:3100). Mention it — the user should not need to ask you next
time.

## Reading the signals honestly

**A stale `raw_pylon.issues.updated_at` is not automatically a failure.** It
means no ticket has been updated recently. On a quiet weekend that is correct.
Check whether ingest runs are *succeeding* before calling freshness a problem —
successful runs plus old data means the tenant is quiet; failing runs plus old
data means the pipeline is broken.

**Zero rows in `ops.*` means nothing has run yet**, not that everything is fine.
Say "no runs recorded yet" rather than reporting green.

**Transform statuses understate damage.** When a transform fails, the ones
downstream are *skipped* and keep their previous status — often `succeeded`. One
failure near the base of the stack can leave a dozen stale tables all reporting
success. If anything failed, treat everything downstream of it as suspect.

**Passing quality checks only cover what is declared.** They verify grain,
nulls, freshness and the declared reconciliations — not that a number is
business-correct. Do not translate "all checks passed" into "the data is right".
