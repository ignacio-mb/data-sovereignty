#!/usr/bin/env bash
# Push the secrets in .env to SSM Parameter Store, one SecureString per key.
# Run from the laptop, once, and again whenever a secret is rotated.
#
# Parameter Store is the source of truth for the values the instance cannot
# invent for itself. MB_API_KEY is deliberately not among them: it is minted
# by scripts/bootstrap_metabase.sh and is only meaningful alongside the
# metabase-app-data volume that holds its other half, so it lives with that
# volume and is re-minted if it is ever lost.
#
# One parameter per key rather than the whole file: a rendered .env is over
# 4 KB, which is the standard-tier limit.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

# shellcheck source=/dev/null
[[ -f "${REPO_ROOT}/.deploy.env" ]] && . "${REPO_ROOT}/.deploy.env"
PREFIX="${DS_PARAMETER_PREFIX:-/data-sovereignty/prod}"
REGION="${AWS_REGION:-us-east-1}"

command -v aws >/dev/null 2>&1 || { echo "error: aws CLI is required" >&2; exit 1; }
[[ -f "$ENV_FILE" ]] || { echo "error: $ENV_FILE does not exist" >&2; exit 1; }

# Rotating the Fernet key makes every encrypted Airflow connection and
# variable undecryptable, so it is generated once, here, and carried forward —
# never regenerated on the instance.
REQUIRED=(
  MB_PREMIUM_EMBEDDING_TOKEN
  MB_ADMIN_EMAIL
  MB_ADMIN_PASSWORD
  AIRFLOW__CORE__FERNET_KEY
  AIRFLOW__API_AUTH__JWT_SECRET
  WAREHOUSE_USER
  WAREHOUSE_PASSWORD
  WAREHOUSE_DB
  METABASE_APP_USER
  METABASE_APP_PASSWORD
  METABASE_APP_DB
  AIRFLOW_USER
  AIRFLOW_PASSWORD
  AIRFLOW_DB
)

# Plus every credential a connected source names, discovered from that source's
# own spec rather than listed here. No source ships with the repo, so a clean
# checkout pushes none — and naming one would make an unconnected source's key
# mandatory configuration for a stack that ingests nothing.
#
# Required once a source IS connected: its DAG is scheduled from the moment the
# spec lands, and a scheduled fetch with no token fails hourly.
#
# ANY `*_env:` key, not just `token_env`. Most sources need exactly one variable,
# but an OAuth grant needs more than one — sources/youtube.yml names
# `client_id_env` and `client_secret_env` alongside its `token_env`, because
# Google's refresh-token exchange takes all three. Matching only `token_env`
# pushed one of the three and left the instance with a connector that could not
# authenticate, while `make secrets-push` reported success.
while IFS= read -r credential_var; do
  [[ -n "$credential_var" ]] || continue
  REQUIRED+=("$credential_var")
done < <(
  grep -hoE '^[[:space:]]*[a-z_]*_env:[[:space:]]*[A-Za-z_][A-Za-z0-9_]*' \
    "${REPO_ROOT}"/sources/*.yml 2>/dev/null \
    | awk '{ print $2 }' | sort -u
)

# Read a value from .env without sourcing the file or ever echoing it.
value_of() {
  awk -v k="$1" 'index($0, k "=") == 1 { print substr($0, length(k) + 2); exit }' "$ENV_FILE"
}

put() {
  local key="$1" value="$2"
  aws ssm put-parameter \
    --region "$REGION" \
    --name "${PREFIX}/${key}" \
    --type SecureString \
    --value "$value" \
    --overwrite >/dev/null
  echo "secrets_push: pushed $key"
}

missing=()
for key in "${REQUIRED[@]}"; do
  v="$(value_of "$key")"
  if [[ -z "$v" ]]; then
    missing+=("$key")
    continue
  fi
  put "$key" "$v"
done

if (( ${#missing[@]} )); then
  echo "secrets_push: these are empty in $ENV_FILE and were not pushed:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

echo "secrets_push: done — ${PREFIX}/* in ${REGION}"
