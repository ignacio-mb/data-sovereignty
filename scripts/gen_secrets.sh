#!/usr/bin/env bash
# Fill in generated secrets in .env, in place, only where the value is empty.
# Idempotent: re-running never rotates an existing key.
set -euo pipefail

ENV_FILE="${1:-.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "gen_secrets: $ENV_FILE does not exist (run 'make env' first)" >&2
  exit 1
fi

# Fernet requires exactly 32 url-safe base64-encoded bytes.
fernet_key() { openssl rand -base64 32 | tr '+/' '-_'; }
jwt_secret() { openssl rand -hex 32; }

set_if_empty() {
  local key="$1" value="$2"
  if ! grep -qE "^${key}=" "$ENV_FILE"; then
    printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
    echo "gen_secrets: appended $key"
    return
  fi
  if grep -qE "^${key}=.+" "$ENV_FILE"; then
    echo "gen_secrets: $key already set, leaving it alone"
    return
  fi
  # Value is empty — substitute. Use a temp file so we don't depend on GNU sed.
  awk -v k="$key" -v v="$value" \
    'BEGIN{FS=OFS="="} $1==k && $2=="" {print k "=" v; next} {print}' \
    "$ENV_FILE" >"${ENV_FILE}.tmp"
  mv "${ENV_FILE}.tmp" "$ENV_FILE"
  echo "gen_secrets: generated $key"
}

set_if_empty AIRFLOW__CORE__FERNET_KEY "$(fernet_key)"
set_if_empty AIRFLOW__API_AUTH__JWT_SECRET "$(jwt_secret)"

chmod 600 "$ENV_FILE"
