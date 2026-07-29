#!/usr/bin/env bash
# Bring the stack up on a host that may or may not have been bootstrapped.
# Run by the systemd unit at boot, and by the deploy path when it finds
# services stopped.
#
# `make up` is staged for a reason: Metabase has to exist before
# bootstrap_metabase.sh can mint an API key, and the Airflow services have to
# be created after that key is in .env, because container environments are
# frozen at create time. A bare `docker compose up -d` on a fresh host gives
# every Airflow service an empty MB_API_KEY.
#
# Once the key is there, though, the staging has done its job: the containers
# already hold it, and a plain `up -d` is both correct and much faster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .env ]]; then
  echo "stack_boot: no .env — run scripts/render_env_from_ssm.sh first" >&2
  exit 1
fi

if grep -qE '^MB_API_KEY=.+' .env; then
  echo "stack_boot: API key present, starting directly"
  exec docker compose up -d --wait
fi

echo "stack_boot: no API key yet, running the full staged bring-up"
exec make up
