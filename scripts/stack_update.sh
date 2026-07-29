#!/usr/bin/env bash
# Stage B of a deploy: take the working tree to a commit that Stage A has
# already vouched for, and converge the running stack onto it.
#
# Invoked as the tree owner by /usr/local/bin/ds-deploy, which holds the
# deploy lock for the whole run. Never run this directly on a live host — it
# assumes the ancestry check and the lock have already happened.
#
# Everything verbose goes to a log file. The only thing on stdout is a JSON
# summary, because stdout ends up in a GitHub Actions log on a public
# repository and error text from `mb` can carry connection details.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DEPLOY_DIR="${DS_DEPLOY_DIR:-/data/deploy}"
STATE_FILE="${DEPLOY_DIR}/state.json"
HISTORY="${DEPLOY_DIR}/history.log"

SHA=""; OLD_SHA=""; ALLOW_IN_FLIGHT="false"; FORCE_REBUILD="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sha)             SHA="$2"; shift 2 ;;
    --old-sha)         OLD_SHA="$2"; shift 2 ;;
    --allow-in-flight) ALLOW_IN_FLIGHT="$2"; shift 2 ;;
    --force-rebuild)   FORCE_REBUILD="$2"; shift 2 ;;
    *) echo "stack_update: unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -n "$SHA" ]] || { echo "stack_update: --sha is required" >&2; exit 2; }

mkdir -p "$DEPLOY_DIR"
LOG="${DEPLOY_DIR}/run-${SHA}.log"
# The log is a trace of a run that touches .env, so it is not world-readable.
# Scoped to creating it: a process-wide umask would also apply to `git reset`,
# and the files it recreates under warehouse/ are bind-mounted into ClickHouse,
# which runs as a different uid and would no longer be able to read them.
( umask 077; : >>"$LOG" )
exec 3>&1 1>>"$LOG" 2>&1
set -x

START_TS="$(date -u +%s)"
REBUILT=false; RECREATED=false; BOOTSTRAPPED=false; SMOKE=skipped
WARNINGS=()

warn() { WARNINGS+=("$1"); set +x; echo "WARNING: $1"; set -x; }

# The single exit path, so a summary always reaches the caller.
finish() {
  local code="$1" message="${2:-}"
  local services
  # `ps --format json` is newline-delimited objects in some compose versions
  # and a single array in others. flatten reads both; without it the newer
  # shape parses to [[…]] and every filter below quietly matches nothing.
  services="$(docker compose ps --format json 2>/dev/null \
    | jq -sc 'flatten | [.[] | {name: .Service, state: .State, health: .Health}]' || echo '[]')"
  jq -nc \
    --arg sha "$SHA" --arg old_sha "$OLD_SHA" --arg message "$message" \
    --argjson code "$code" --argjson rebuilt "$REBUILT" --argjson recreated "$RECREATED" \
    --argjson bootstrapped "$BOOTSTRAPPED" --arg smoke "$SMOKE" \
    --argjson services "$services" \
    --argjson warnings "$(printf '%s\n' "${WARNINGS[@]:-}" | jq -Rsc 'split("\n") | map(select(length > 0))')" \
    --argjson duration "$(( $(date -u +%s) - START_TS ))" \
    '{sha: $sha, old_sha: $old_sha, exit: $code, message: $message, rebuilt: $rebuilt,
      recreated: $recreated, bootstrapped: $bootstrapped, smoke: $smoke,
      services: $services, warnings: $warnings, duration_s: $duration}' >&3
  exit "$code"
}
fail() { finish "${2:-1}" "$1"; }

# ─── Preconditions ───────────────────────────────────────────────────────────

for tool in git jq docker mb curl openssl; do
  command -v "$tool" >/dev/null 2>&1 || fail "missing host tool: $tool"
done
[[ -f .env ]] || fail "no .env on this host — run scripts/render_env_from_ssm.sh"

avail_mb="$(df -Pm /data | awk 'NR==2 {print $4}')"
[[ "$avail_mb" -gt 5120 ]] || fail "/data has only ${avail_mb}MB free; refusing to build"

# ─── Is anything running? ────────────────────────────────────────────────────

running_count() { docker compose ps --status running -q 2>/dev/null | grep -c . || true; }
STACK_UP=false
[[ "$(running_count)" -gt 0 ]] && STACK_UP=true

# Read one value out of .env without sourcing it. Sourcing would put every
# secret into this shell's environment, and with the trace on it would print
# each assignment straight into the log.
env_value() { awk -v k="$1" 'index($0, k "=") == 1 { print substr($0, length(k) + 2); exit }' .env; }

airflow_db_query() {
  docker compose exec -T airflow-db \
    psql -U "$(env_value AIRFLOW_USER)" -d "$(env_value AIRFLOW_DB)" -tAc "$1"
}

# The logging level is not cosmetic: Airflow 3.3 writes its structlog INFO
# lines to STDOUT, mixed in with command output, so `-o json` is unparseable
# without this and every check below would fail on a healthy stack.
af() {
  docker compose exec -T -e AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING \
    airflow-scheduler airflow "$@"
}

# The pipeline DAGs are generated from the specs in sources/, so which ids exist
# depends on what is connected — and a checkout with no sources has none beyond
# stack_smoke. Nothing below may name one: a hardcoded list went stale the moment
# the DAGs became generated, and every deploy then failed on "DAG
# pylon_ingest_hourly is not registered". The id SUFFIX the generator produces is
# matched instead — <source>_ingest, <source>_backfill, <source>_reconcile — so
# keep those in step with source_dags.py.

# Every registered DAG except the smoke test, read live rather than predicted: a
# reconcile DAG exists only when its spec declares a history floor, so a predicted
# list would name DAGs that legitimately do not exist.
registered_pipeline_dags() {
  af dags list -o json 2>/dev/null \
    | jq -r '.[] | select(.dag_id != "stack_smoke") | .dag_id' 2>/dev/null || true
}

# ─── Nothing may be in flight ────────────────────────────────────────────────
# Queued counts as in flight: a source's ingest pool has one slot, so while its
# reconcile holds it the scheduled run sits queued and its timeout has not
# started ticking.
#
# Deferring is not losing the deploy. The target commit is already recorded in
# Parameter Store, and the converge timer retries every five minutes.

if [[ "$STACK_UP" == true && "$ALLOW_IN_FLIGHT" != "true" ]]; then
  long_running="$(airflow_db_query "
    select count(*) from dag_run
    where state in ('running','queued')
      and (dag_id like '%\_backfill' escape '\'
        or dag_id like '%\_reconcile' escape '\')" | tr -d '[:space:]')"
  if [[ "${long_running:-0}" -gt 0 ]]; then
    fail "a backfill or a reconcile is in flight; deferring" 76
  fi

  waited=0
  while :; do
    hourly="$(airflow_db_query "
      select count(*) from dag_run
      where state in ('running','queued')
        and dag_id like '%\_ingest' escape '\'" | tr -d '[:space:]')"
    # Every `make quality` and `make docs` is a one-off container that no
    # dag_run knows about. Resetting the tree underneath one mid-run is the
    # failure this catches.
    oneoff="$(docker ps -q \
      --filter "label=com.docker.compose.project=data-sovereignty" \
      --filter "label=com.docker.compose.service=airflow-cli" | grep -c . || true)"
    if [[ "${hourly:-0}" -eq 0 && "${oneoff:-0}" -eq 0 ]]; then
      break
    fi
    if [[ "$waited" -ge 1500 ]]; then
      fail "an ingest or CLI run has been in flight for 25 minutes; deferring" 76
    fi
    sleep 30
    waited=$(( waited + 30 ))
  done
fi

# Pause for the duration. Pause state lives in the metadata database, so it
# survives container recreation, and a deploy that dies half way leaves the
# stack visibly halted instead of quietly inconsistent.
#
# One dag_id per invocation: `airflow dags pause a b c` is an argparse error
# (exit 2), and a version of this that passed them all at once would fail
# silently and leave the scheduler free to start an ingest half way through the
# reset.
PAUSED_BY_US=()
unpause() {
  local dag
  # Only what this deploy paused. A DAG an operator paused deliberately stays
  # paused.
  for dag in "${PAUSED_BY_US[@]:-}"; do
    [[ -n "$dag" ]] || continue
    af dags unpause "$dag" >/dev/null 2>&1 || true
  done
}
if [[ "$STACK_UP" == true ]]; then
  listing="$(af dags list -o json 2>/dev/null || true)"
  while IFS= read -r dag; do
    [[ -n "$dag" ]] || continue
    if jq -e --arg d "$dag" \
         'any(.[]; .dag_id == $d and (.is_paused | tostring | ascii_downcase == "true"))' \
         >/dev/null 2>&1 <<<"$listing"; then
      continue
    fi
    af dags pause "$dag" >/dev/null || warn "could not pause ${dag}"
    PAUSED_BY_US+=("$dag")
  done < <(registered_pipeline_dags)
  trap unpause EXIT
fi

# ─── Preserve whatever the running stack wrote ───────────────────────────────
# Kept as a patch rather than a stash: greppable, prunable, and it does not
# accumulate invisible state in the repository.
if ! git diff --quiet; then
  git diff >"${DEPLOY_DIR}/dirty-${SHA}.patch"
  warn "the working tree was dirty; saved to ${DEPLOY_DIR}/dirty-${SHA}.patch"
fi

# ─── Rebuild or not ──────────────────────────────────────────────────────────
# The two packages are installed editable and their sources are bind
# mounted, so SQL, suites, DAGs and Python land without a rebuild. Only the
# dependency set and the image itself need one.
REBUILD_PATHS=(docker/ uv.lock pyproject.toml pipeline/pyproject.toml quality/pyproject.toml)

# The base is the last commit that finished, not HEAD: a previous deploy that
# reset the tree and then failed its build leaves HEAD already at the new
# commit, and a HEAD-to-target diff would come back empty and skip exactly the
# build that failed.
BASE=""
[[ -f "$STATE_FILE" ]] && BASE="$(jq -r '.last_complete_sha // empty' "$STATE_FILE")"
[[ -z "$BASE" ]] && BASE="$OLD_SHA"

REBUILD=false
if [[ "$FORCE_REBUILD" == "true" ]]; then
  REBUILD=true
elif ! docker image inspect data-sovereignty/airflow:local >/dev/null 2>&1; then
  REBUILD=true
elif [[ -z "$BASE" ]] || ! git cat-file -e "${BASE}^{commit}" 2>/dev/null; then
  warn "previous commit unknown or unreachable; rebuilding to be safe"
  REBUILD=true
else
  CHANGED="$(git diff --name-only "$BASE" "$SHA")"
  for path in "${REBUILD_PATHS[@]}"; do
    if grep -q "^${path}" <<<"$CHANGED"; then REBUILD=true; break; fi
  done
fi

CHANGED_ALL=""
if [[ -n "$BASE" ]] && git cat-file -e "${BASE}^{commit}" 2>/dev/null; then
  CHANGED_ALL="$(git diff --name-only "$BASE" "$SHA")"
  # Only ever executed by ClickHouse on the first init of an empty volume, so
  # a change here is a no-op on a live warehouse. Silent no-ops are worse than
  # loud ones.
  if grep -q '^warehouse/init/' <<<"$CHANGED_ALL"; then
    warn "warehouse/init/ changed, but it only runs on an empty volume — apply it by hand"
  fi
fi

# ─── Build before touching the tree ──────────────────────────────────────────
# uv's editable install points into the bind-mounted source directories, so the
# instant the tree resets, every `ingest`/`dq` call runs the new source against
# the old environment — for the fifteen minutes an arm64 build can take. So
# the new image is built from a detached worktree first, and the live tree is
# only moved once it exists.
if [[ "$REBUILD" == true ]]; then
  WORKTREE="${DEPLOY_DIR}/build/${SHA}"
  # The whole build directory, not just this commit's: a deploy killed between
  # `worktree add` and `worktree remove` leaves one behind, and prune will not
  # remove a directory that still exists.
  rm -rf "${DEPLOY_DIR}/build"
  git worktree prune
  git worktree add --detach "$WORKTREE" "$SHA"

  docker image tag data-sovereignty/airflow:local data-sovereignty/airflow:previous 2>/dev/null || true

  if ! docker compose \
      --project-name data-sovereignty-build \
      --project-directory "$WORKTREE" \
      --env-file "${REPO_ROOT}/.env" \
      -f "${WORKTREE}/docker-compose.yml" build; then
    docker image tag data-sovereignty/airflow:previous data-sovereignty/airflow:local 2>/dev/null || true
    git worktree remove --force "$WORKTREE" || rm -rf "$WORKTREE"
    fail "image build failed; the running stack was not touched"
  fi

  git worktree remove --force "$WORKTREE" || rm -rf "$WORKTREE"
  REBUILT=true
fi

# ─── Move the tree ───────────────────────────────────────────────────────────
# No `git clean`. With -x it would delete .env, whose MB_API_KEY cannot be
# recovered from anywhere else, and it buys nothing that reset --hard does not
# already do.
git -c core.hooksPath=/dev/null reset --hard "$SHA"
[[ "$(git rev-parse HEAD)" == "$SHA" ]] || fail "tree is not at ${SHA} after reset"

# ─── .env keeps up with docker-compose.yml ───────────────────────────────────
# The variables compose references *without* a default are the ones that must
# be present. `docker compose config -q` is no help: it warns about an unset
# variable and exits 0.

# The licence token is legitimately unset: Metabase boots without one and
# everything this stack does works against an unlicensed instance.
MAY_BE_EMPTY=(MB_PREMIUM_EMBEDDING_TOKEN)
mapfile -t COMPOSE_REQUIRED < <(
  # $${VAR} is an escape: compose passes it through as a literal for the
  # container's own shell to expand, so it is not a variable this file has to
  # supply. Strip those before looking for the real ones.
  sed -E 's/\$\$\{[A-Za-z_][A-Za-z0-9_]*\}//g' docker-compose.yml \
    | grep -oE '\$\{[A-Z_][A-Z0-9_]*\}' | tr -d '${}' | sort -u
)

env_before="$(sha256sum .env | cut -d' ' -f1)"
missing=()
for key in "${COMPOSE_REQUIRED[@]}"; do
  printf '%s\n' "${MAY_BE_EMPTY[@]}" | grep -qx "$key" && continue
  [[ -n "$(env_value "$key")" ]] || missing+=("$key")
done

if (( ${#missing[@]} )); then
  warn "missing from .env: ${missing[*]} — re-rendering from Parameter Store"
  bash scripts/render_env_from_ssm.sh || true
  still=()
  for key in "${missing[@]}"; do
    [[ -n "$(env_value "$key")" ]] || still+=("$key")
  done
  (( ${#still[@]} )) && fail "still missing from .env after re-rendering: ${still[*]}" 77
fi

# ─── Does Metabase need bootstrapping? ───────────────────────────────────────
# The probe is bootstrap's own key_works() check, and it goes through mb-cli
# rather than raw REST. Running the whole bootstrap unconditionally is not
# free: it re-syncs the warehouse schema every time.
NEED_BOOTSTRAP=false
if [[ -z "$(env_value MB_API_KEY)" ]]; then
  NEED_BOOTSTRAP=true
elif [[ -n "$CHANGED_ALL" ]] && grep -q '^scripts/bootstrap_metabase.sh$' <<<"$CHANGED_ALL"; then
  NEED_BOOTSTRAP=true
elif [[ "$STACK_UP" == true ]]; then
  # Through the environment, not --api-key, so the key never reaches the
  # process table — and with the trace off, so it never reaches the log
  # either. This is bootstrap's own key_works() check.
  set +x
  if ! MB_API_KEY="$(env_value MB_API_KEY)" MB_URL="$(env_value MB_URL)" \
       MB_CLI_DISABLE_KEYRING=1 mb auth status >/dev/null 2>&1; then
    NEED_BOOTSTRAP=true
  fi
  set -x
fi

# ─── Converge ────────────────────────────────────────────────────────────────
if [[ "$STACK_UP" != true ]]; then
  # Nothing running: one definition of the staged bring-up, and it is
  # stack_boot.sh's.
  bash scripts/stack_boot.sh || fail "stack_boot failed"
  [[ "$NEED_BOOTSTRAP" == true ]] && BOOTSTRAPPED=true
else
  if [[ "$REBUILT" == true ]]; then
    # airflow-init runs `airflow db migrate` with the new image; old
    # schedulers must not still be running against the database while it does.
    docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
  fi

  if [[ "$NEED_BOOTSTRAP" == true ]]; then
    docker compose up -d --wait warehouse-db metabase-app-db airflow-db metabase
    timeout 900 bash scripts/bootstrap_metabase.sh || fail "metabase bootstrap failed"
    BOOTSTRAPPED=true
  fi

  # Container environments are frozen at create time, so a warehouse credential
  # or a source token rewritten in .env only reaches the stack on a recreate.
  compose_args=(up -d --wait --wait-timeout 600)
  if [[ "$(sha256sum .env | cut -d' ' -f1)" != "$env_before" ]]; then
    compose_args+=(--force-recreate)
    RECREATED=true
  fi
  docker compose "${compose_args[@]}" || fail "services did not come up healthy"
fi

# ─── Verify ──────────────────────────────────────────────────────────────────
# --all, or an exited service is simply absent from the listing and the filter
# below can never see it. airflow-init is the one that legitimately exits.
unhealthy="$(docker compose ps --all --format json | jq -sr '
  flatten
  | [.[]
     | select(.Service != "airflow-init")
     | select(.State != "running" or (.Health // "") == "unhealthy")
     | .Service]
  | join(", ")')"
[[ -z "$unhealthy" ]] || fail "not healthy after deploy: ${unhealthy}"

# A DAG that fails to import does not raise anything — it silently disappears
# from the scheduler, and ingestion just stops happening.
#
# Fail closed. The CLI writes structured log lines to stderr and prints "No
# data found" rather than an empty list when there is nothing, so the only
# safe reading is: prove it is empty, or treat it as broken.
import_errors="$(af dags list-import-errors -o json 2>/dev/null || true)"
if grep -q 'No data found' <<<"$import_errors"; then
  : # nothing failed to import
elif jq -e 'length == 0' >/dev/null 2>&1 <<<"$import_errors"; then
  : # an explicit empty list
else
  fail "DAG import errors after deploy: $(tr '\n' ' ' <<<"$import_errors" | cut -c1-300)"
fi

# What must be registered is derived from the tree as it is NOW — after the reset,
# so a deploy that connects a source is checked against the source it just added,
# and one that removes a source no longer demands the DAGs it just deleted.
#
# stack_smoke unconditionally; then _ingest and _backfill for each connected
# source. Not _reconcile: that one is generated only when the spec declares a
# backfill_start for the tombstone guard to compare against, so requiring it would
# fail a legitimate spec.
expected_dags=(stack_smoke)
for spec in sources/*.yml; do
  [[ -e "$spec" ]] || continue
  source_name="$(basename "$spec" .yml)"
  expected_dags+=("${source_name}_ingest" "${source_name}_backfill")
done

registered="$(af dags list -o json 2>/dev/null || true)"
for dag in "${expected_dags[@]}"; do
  jq -e --arg d "$dag" 'any(.[]; .dag_id == $d)' >/dev/null 2>&1 <<<"$registered" \
    || fail "DAG ${dag} is not registered"
done
if [[ "${#expected_dags[@]}" -eq 1 ]]; then
  # Worth saying out loud: a stack that ingests nothing is the shipped state, not
  # a broken deploy, and the deploy log is where someone looks to tell them apart.
  set +x; echo "no sources connected — only stack_smoke is expected"; set -x
fi

if [[ "$REBUILT" == true ]]; then
  docker compose --profile cli run --rm airflow-cli bash -c \
    'ingest --help >/dev/null && dq --help >/dev/null' \
    || fail "the CLIs do not resolve against the rebuilt image"
  SMOKE=clis
fi

# ─── Record ──────────────────────────────────────────────────────────────────
jq -nc --arg sha "$SHA" --arg at "$(date -u +%FT%TZ)" \
  '{last_complete_sha: $sha, completed_at: $at}' >"${STATE_FILE}.tmp"
mv "${STATE_FILE}.tmp" "$STATE_FILE"
printf '%s  %s  rebuilt=%s bootstrapped=%s\n' "$(date -u +%FT%TZ)" "$SHA" "$REBUILT" "$BOOTSTRAPPED" >>"$HISTORY"

if [[ "$REBUILT" == true ]]; then
  # Never -a: :previous is kept deliberately, as the one image a rollback can
  # fall back to without a rebuild.
  docker image prune -f --filter until=336h || true
  docker builder prune -f --filter until=336h || true
fi

finish 0 "deployed ${SHA}"
