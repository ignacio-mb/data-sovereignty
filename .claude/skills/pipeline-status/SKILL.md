---
name: pipeline-status
description: Answer "is the pipeline healthy?" end to end — services, recent runs, freshness, and data-quality verdicts. Triggers — "is my pipeline healthy?", "verify the whole state", "what's the status?", "is the data fresh?", "did last night's run work?", "what's broken?".
allowed-tools: Bash, Read
---

# Reporting pipeline state

The user is asking one question: **can I trust the numbers right now?** Answer that
first, in a sentence, then show the evidence.

Work down the chain and stop early when you find the break — a stopped container
explains stale data, so there is no point reporting the staleness separately as if
it were a second problem.

## 0. Is anything connected at all

```bash
make sources
```

**No sources means there is nothing to be healthy or unhealthy about.** Say so
plainly — "the stack is up and no source is connected, so nothing is being
ingested" — and stop. Do not go hunting for empty tables and report them as
problems; a fresh checkout ingests nothing by design. Offer `add-source`.

## 1. Services

```bash
make status
```

Anything not `healthy`/`running` is the answer; go to `stack-ops`.

## 2. Recent runs

The DAGs are generated per source, so find them before querying them:

```bash
docker compose --profile cli run --rm airflow-cli airflow dags list
```

Then, for each `<source>_ingest`:

```bash
docker compose --profile cli run --rm airflow-cli \
  airflow dags list-runs -d <source>_ingest --limit 5
```

Compare against that source's own schedule from `make sources`: a source that runs
hourly and one that runs nightly are not late at the same point.

## 3. Everything else, in one query

`ops` exists so this is a single round trip rather than five. ClickHouse SQL, not
Postgres — and `make ch-q` runs one statement without a credential reaching the
command line:

```bash
make ch-q Q="
SELECT 'last ingest' AS signal,
       formatDateTime(max(recorded_at), '%F %R') AS at,
       argMax(concat(coalesce(dag_id, '?'), ' ', status), recorded_at) AS detail
  FROM ops.pipeline_runs
UNION ALL
SELECT 'quality (24h)',
       formatDateTime(max(validated_at), '%F %R'),
       concat(toString(countIf(NOT success)), ' failed of ', toString(count()))
  FROM ops.gx_results
 WHERE validated_at > now() - INTERVAL 24 HOUR
UNION ALL
SELECT 'freshness',
       formatDateTime(max(validated_at), '%F %R'),
       argMax(if(success, 'within SLO', 'STALE'), validated_at)
  FROM ops.gx_results
 WHERE validated_at > now() - INTERVAL 24 HOUR
   AND description LIKE '%within the last%'"
```

Freshness comes from the recorded verdict rather than from a raw table on purpose:
which table and column carry a source's freshness signal is declared in that
source's spec, so `ops.gx_results` is the one place that knows it for every source
at once.

If any signal is bad, drill in:

```bash
make ch-q Q="
SELECT checkpoint, asset, expectation, column_name, observed_value
  FROM ops.gx_results
 WHERE NOT success AND validated_at > now() - INTERVAL 24 HOUR
 ORDER BY validated_at DESC LIMIT 20"
```

`checkpoint` is `raw_<source>`, so with several sources connected it tells you
which one is unhappy.

## Reading the signals honestly

**Stale data is not automatically a failure.** It means the source has not changed
recently, which on a quiet weekend is correct. Check whether ingest runs are
*succeeding* first: successful runs plus old data means a quiet source, failing runs
plus old data means a broken pipeline. That is exactly why freshness is recorded as
a warning rather than a gate.

**Zero rows in `ops.*` means nothing has run yet**, not that everything is fine.
Say "no runs recorded yet" rather than reporting green.

**A source whose DAG exists but has no `ops.pipeline_runs` rows** is worth calling
out: the spec landed but no run has completed. Usual causes are a missing token, or
an Airflow pool that was never created because `airflow-init` has not run since the
source was added.

**Passing quality checks only cover what is declared.** They verify primary keys,
declared not-nulls, freshness and the declared referential edges on the raw layer —
not that a number is business-correct, and nothing at all about tables anyone builds
from it elsewhere. Do not translate "all checks passed" into "the data is right".
