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

Every connected source needs its own token, so derive the list from the specs
rather than hardcoding one API's variable:

```bash
# each source's token var, plus the two the platform itself needs
{ grep -h 'token_env:' sources/*.yml | awk '{print $2}'; \
  echo MB_PREMIUM_EMBEDDING_TOKEN; echo MB_ADMIN_PASSWORD; } \
| while read -r key; do
    printf '%s = %s\n' "$key" \
      "$(grep -qE "^${key}=.+" .env && echo '[set]' || echo 'MISSING')"
  done
```

Anything showing `=` with nothing after it needs the user. Ask for all missing
ones in a single question; do not ask them to paste values into the chat if you
can avoid it — tell them to edit `.env` directly, then continue.

```bash
make build   # ~5-10 min the first time
make up      # starts services, bootstraps Metabase, provisions an API key
make mb-audit
```

`make mb-audit` is the gate. If it fails on missing token features, the license
is not a valid production Enterprise token and modeling cannot proceed — say so
plainly rather than working around it.

## Daily

```bash
make status        # health + URLs
make logs S=metabase   # one service; omit S for everything
make down          # stop, keep all data
```

## Things that will confuse you

**`make up` is staged and the order matters.** Metabase must be bootstrapped and
`MB_API_KEY` written to `.env` before the Airflow containers start, or they
inherit an empty key and every transform task fails on auth. A bare
`docker compose up -d` skips that. The fix is always `make up` again — it
recreates the containers with the now-populated environment.

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
