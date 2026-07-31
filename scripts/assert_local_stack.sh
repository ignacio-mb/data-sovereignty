#!/usr/bin/env bash
# Refuse to act on a local port that something other than Docker is serving.
#
# The failure this exists for actually happened. `make tunnels` used to forward
# the instance's Metabase onto localhost:3100 — the same port `make up` binds
# locally. An SSH tunnel binds 127.0.0.1 explicitly while Docker binds 0.0.0.0,
# and a loopback bind wins, so `localhost:3100` silently became PRODUCTION while
# every container kept running underneath. `make bootstrap` then rotated the
# production API key, believing it was talking to the laptop.
#
# `make tunnels` now uses 32xx/81xx, so the collision is gone by construction.
# This is the check for every other way it can come back: a hand-rolled `ssh -L`,
# a port-forward left running, a second stack. Addresses are not identities, and
# the only safe assumption about `localhost` is that it needs verifying.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

# Read a port from .env without sourcing it — .env holds secrets, and sourcing
# it to learn a port number is a needless way to get them into this shell.
port_from_env() {
  local key="$1" fallback="$2" value=""
  [[ -f "$ENV_FILE" ]] && value="$(awk -v k="$key" \
    'index($0, k "=") == 1 { print substr($0, length(k) + 2); exit }' "$ENV_FILE")"
  value="${value:-$fallback}"
  # A port may be written `127.0.0.1:3100` — scripts/render_env_from_ssm.sh does
  # exactly that on the instance, and Makefile's `status` target already strips
  # it the same way. lsof rejects that form outright ("unacceptable port
  # specification"), and the rejection used to be swallowed by `2>/dev/null ||
  # true`, so every check silently became a no-op and the guard reported clean.
  # A check that cannot fail is worse than no check.
  value="${value##*:}"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "  cannot read a port number for ${key} (got '${value}')" >&2
    echo "unreadable"
    return
  fi
  echo "$value"
}

# Who is listening, as a command name. Empty when the port is free, which is not
# a problem: an unbound port means the stack simply is not up yet.
listener_on() {
  local port="$1" out=""
  if command -v lsof >/dev/null 2>&1; then
    # stderr is deliberately NOT discarded into the void: an lsof that refuses
    # the argument must not look like an empty result set.
    out="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -Fc 2>/dev/null | sed -n 's/^c//p' | sort -u | paste -sd, -)"
    echo "$out"
    return
  fi
  if command -v ss >/dev/null 2>&1; then
    # Linux (the instance). `users:(("sshd",pid=...))` -> the process names.
    ss -H -ltnp "sport = :${port}" 2>/dev/null \
      | grep -oE 'users:\(\("[^"]+"' | sed 's/.*"\(.*\)"*/\1/' | tr -d '"' | sort -u | paste -sd, -
    return
  fi
  # Neither tool: say so rather than returning "" , which check_port would read
  # as "port free" and pass.
  echo "unknown"
}

FAILED=0
check_port() {
  local port="$1" label="$2" who proc
  if [[ "$port" == "unreadable" ]]; then
    FAILED=1
    echo "  ${label}: could not determine which port to check" >&2
    return
  fi
  who="$(listener_on "$port")"
  if [[ "$who" == "unknown" ]]; then
    FAILED=1
    echo "  ${label} (port ${port}): no lsof and no ss, so nothing could be checked" >&2
    return
  fi
  [[ -z "$who" ]] && return 0
  # Every listener must be Docker, not merely one of them. This is the whole
  # bug: a tunnel binds 127.0.0.1 while Docker binds 0.0.0.0, so BOTH answer on
  # the port and the loopback one wins for `localhost`. A check that passes
  # because it found Docker somewhere in the list is the same blind spot with
  # extra steps — it was written that way first, and it cleared a live tunnel.
  IFS=',' read -r -a procs <<< "$who"
  for proc in "${procs[@]}"; do
    # Docker Desktop's listener is com.docke(r); dockerd on Linux is docker-pr.
    case "$proc" in
      com.docke*|docker*) continue ;;
    esac
    FAILED=1
    echo "  port ${port} (${label}) also has a non-Docker listener: '${proc}'" >&2
  done
}

METABASE_PORT="$(port_from_env METABASE_HOST_PORT 3100)"
AIRFLOW_PORT="$(port_from_env AIRFLOW_HOST_PORT 8080)"
WAREHOUSE_PORT="$(port_from_env WAREHOUSE_HTTP_PORT 8124)"
DATADOCS_PORT="$(port_from_env DATADOCS_HOST_PORT 8081)"

check_port "$METABASE_PORT"  "Metabase"
check_port "$AIRFLOW_PORT"   "Airflow"
check_port "$WAREHOUSE_PORT" "ClickHouse HTTP"
check_port "$DATADOCS_PORT"  "data docs"

if (( FAILED )); then
  cat >&2 <<'EOF'

refusing to continue: a local port this stack uses is held by another process.

If that is an SSH tunnel to the instance, anything addressed at localhost —
`make bootstrap`, a host-side `ingest run`, `mb` — reaches PRODUCTION rather
than this laptop, and nothing in the output would say so.

  lsof -nP -iTCP:<port> -sTCP:LISTEN     # confirm what holds it
  pkill -f 'ssh -N -L .*data-sovereignty'  # drop the repo's own tunnels

`make tunnels` forwards production to 3200/8180/8181/8224 precisely so it can
never shadow the local stack. Re-run once the port is free.

Set DS_SKIP_LOCAL_CHECK=1 to bypass this — only when you have confirmed by hand
which instance you are pointed at.
EOF
  exit 1
fi
