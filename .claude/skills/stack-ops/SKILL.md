---
name: stack-ops
description: Bring the data stack up or down, run first-time setup, check service health, read logs, and reset. Triggers — "start the stack", "bring everything up", "is it running?", "shut it down", "reset everything", "set this up from scratch", "Metabase won't start".
allowed-tools: Bash, Read, Edit, AskUserQuestion
---

# Stack operations

## First-time setup

```bash
make env
```

Then check which required secrets are still blank — **without printing their
values**:

```bash
grep -E '^(MB_PREMIUM_EMBEDDING_TOKEN|MB_ADMIN_PASSWORD)=' .env \
  | sed -E 's/=(.+)/= [set]/'
```

Anything showing `=` with nothing after it needs the user. Ask for all missing ones
in a single question; do not ask them to paste values into the chat if you can avoid
it — tell them to edit `.env` directly, then continue.

Those two are all the stack itself needs. **Source credentials are separate**: each
spec names the variable holding its token in `api.auth.token_env`, and `add-source`
says which name to add when a source is connected.

They are not optional once a source ships, though. Check what `sources/` holds
before declaring setup done — anything there schedules an unpaused hourly DAG that
fails without its credential:

```bash
grep -h '^ *token_env:' sources/*.yml 2>/dev/null | awk '{print $2}'
```

```bash
make build   # ~5-10 min the first time
make up      # starts services, bootstraps Metabase, provisions an API key
```

`make bootstrap` (which `make up` runs, and which is safe to re-run on its own)
is the gate: it is what proves the instance is provisioned — set up, holding a
working API key, and connected to the warehouse. It also reports whether the licence
token was accepted. An unaccepted token is not a blocker here: the instance behaves
like OSS and everything this stack does — ingestion, quality, the warehouse, the
`ops` history — works either way. Say plainly that it is effectively OSS rather than
implying something is broken.

## Daily

```bash
make status        # health + URLs
make logs S=metabase   # one service; omit S for everything
make down          # stop, keep all data
```

## Things that will confuse you

**`make up` is staged and the order matters.** Metabase must be bootstrapped and
`MB_API_KEY` written to `.env` before the Airflow containers start: container
environments are frozen at create time, so anything started first carries the
pre-bootstrap `.env` until it is recreated. A bare `docker compose up -d` skips
the ordering. The fix is always `make up` again — it recreates the containers
with the now-populated environment.

**Init SQL runs once.** `warehouse/init/*.sql` only executes on first
initialization of an empty volume. Editing it does nothing to a running stack.

**`make nuke` destroys data and needs typed confirmation.** It removes the
warehouse, the dlt cursor state and everything in Metabase. `warehouse-data` and
`dlt-state` are a matched pair — never delete one alone, or the pipeline
believes it has already loaded rows that no longer exist. Confirm with the user
before running it, every time, even if they seemed to ask for it.

**Ports.** Metabase 3100, Airflow 8080, data docs 8081, warehouse 5434. Chosen
to coexist with the `metabase-demo` stack. If a port is taken, change it in
`.env` rather than stopping the other stack.

## Diagnosing a service that will not start

```bash
docker compose ps          # which one is unhealthy
docker compose logs --tail=80 <service>
```

- **Metabase unhealthy for >2 min on first boot** is usually normal: it is
  running app-db migrations. The healthcheck allows 3 minutes.
- **Metabase exits immediately** — almost always the license token. Look for
  `token` in the logs.
- **Airflow containers restart-looping** — check `airflow-init` completed:
  `docker compose logs airflow-init`. It runs the DB migration and creates the
  `ingest_runtime` pool; everything else waits on it.
- **A Postgres container is healthy but Metabase cannot reach it** — the app is
  configured with the *service* name (`warehouse-db:5432`), not localhost. If
  someone edited `.env` to fix a host-side connection, they may have broken the
  container-side one. Compose overrides those vars deliberately; check
  `x-airflow-env` in `docker-compose.yml`.
