---
name: pipeline-status
description: Answer "is ingestion healthy?" end to end — services, recent runs, freshness and data-quality verdicts, per source. Triggers — "is my pipeline healthy?", "verify the whole state", "what's the status?", "is the data fresh?", "did last night's run work?", "what's broken?", "did the sync work?".
allowed-tools: Bash, Read
---

# Reporting ingestion state

The user is asking one thing: **can I trust what is in the warehouse right now?**
Answer that in a sentence, then show the evidence.

Work down the chain and stop at the first break. A stopped container explains
stale data, so reporting the staleness separately as if it were a second problem
just makes the answer longer and less clear.

With more than one source connected, say **which** source is unhealthy. "Ingestion
is fine" is wrong if one of three connectors has been failing for a day.

## 1. Services

```bash
make status
```

Anything not `healthy`/`running` is the answer; go to `stack-ops`.

## 2. Recent runs, per source

```bash
ls sources/            # which sources exist
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d <source>_ingest_hourly --limit 5
```

Compare against that source's own schedule — `grep schedule: sources/<name>.yml`.
Being "late" only means something relative to how often it is supposed to run.

## 3. The rest in one query

`ops` exists so this is one round trip rather than five. Note this is ClickHouse:
`INTERVAL 24 HOUR`, not Postgres' `interval '24 hours'`.

```bash
make ch <<'SQL'
SELECT 'last run' AS signal,
       formatDateTime(max(recorded_at), '%F %R') AS at,
       argMax(status, recorded_at) AS detail
  FROM ops.pipeline_runs
UNION ALL
SELECT 'quality (24h)',
       formatDateTime(max(validated_at), '%F %R'),
       concat(toString(countIf(NOT success AND severity = 'error')), ' gating failures of ',
              toString(count()))
  FROM ops.gx_results WHERE validated_at > now() - INTERVAL 24 HOUR;
SQL
```

Then per source, whatever its freshness column is (from the spec's
`quality.freshness`):

```sql
SELECT max(updated_at), dateDiff('hour', max(updated_at), now()) AS hours_old
  FROM raw_<source>.<table>;
```

Drill into failures with the sentence the spec author wrote, not the machinery:

```sql
SELECT asset, description, observed_value, severity
  FROM ops.gx_results
 WHERE NOT success AND validated_at > now() - INTERVAL 24 HOUR
 ORDER BY validated_at DESC LIMIT 20;
```

## 4. Point them at the dashboard

All of this is also the **Pipeline Health** dashboard in Metabase
(http://localhost:3100). Mention it — the user should not have to ask you next
time.

## Reading the signals honestly

**Stale data is not automatically a failure.** It means the source system has not
changed anything recently, which on a quiet weekend is correct. Check whether
runs are *succeeding* first: successful runs plus old data means the source is
quiet; failing runs plus old data means the pipeline is broken. Those are opposite
answers from the same row count.

**Only gating failures make a run red.** `severity = 'warn'` is recorded and
reported but deliberately does not fail a checkpoint — freshness is usually one.
Counting warnings as breakage manufactures an incident.

**Zero rows in `ops.*` means nothing has run yet**, not that everything is fine.
Say "no runs recorded yet" rather than reporting green.

**Passing checks only cover what is declared.** They verify keys, nulls, freshness
and the declared referential edges — not that a number is business-correct. Do not
translate "all checks passed" into "the data is right".

**A skipped resource is not a failed one.** A source whose API returned nothing
for an endpoint has no table, and the suite skips it with a message. That is a
fact about the source, not a fault — unless the spec marks it `required`.

## Not this skill's job

Whether the modeled tables are correct. This reports on getting data in:
services, runs, freshness, and the ingest quality gate. `ops.mb_transform_runs`
records whether the transform step *ran*, which is orchestration — but whether
its output is right belongs to whoever authors the transforms. See the router.
