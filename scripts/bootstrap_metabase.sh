#!/usr/bin/env bash
#
# Bring a fresh Metabase instance to the point where the rest of the stack can
# use it:
#   1. wait for it to be healthy
#   2. run the setup wizard, or log in if it has already been run
#   3. verify the Enterprise license actually grants the features we need
#   4. connect the warehouse and sync its schema
#   5. provision an API key and write it into .env for mb / mbx / Airflow
#
# Idempotent. Re-running logs in instead of re-running the wizard, reuses an
# existing database connection, and keeps a working API key.
#
# Reads .env; requires bash, curl and jq.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

command -v jq >/dev/null 2>&1 || { echo "error: jq is required (brew install jq)" >&2; exit 1; }

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

# The script runs on the host, so it talks to the published port. The warehouse
# details below are how METABASE sees the warehouse, i.e. on the compose network.
MB_URL="${MB_URL:-http://localhost:${METABASE_HOST_PORT:-3100}}"
MB_ADMIN_EMAIL="${MB_ADMIN_EMAIL:?set MB_ADMIN_EMAIL in .env}"
MB_ADMIN_PASSWORD="${MB_ADMIN_PASSWORD:?set MB_ADMIN_PASSWORD in .env}"
MB_SITE_NAME="${MB_SITE_NAME:-Data Sovereignty}"
ADMIN_FIRST="${MB_ADMIN_FIRST:-Data}"
ADMIN_LAST="${MB_ADMIN_LAST:-Admin}"

WAREHOUSE_MB_NAME="${MB_WAREHOUSE_DB_NAME:-Warehouse}"
WAREHOUSE_HOST="warehouse-db"
WAREHOUSE_PORT=5432
WAREHOUSE_DB="${WAREHOUSE_DB:-warehouse}"
WAREHOUSE_USER="${WAREHOUSE_USER:-warehouse}"
WAREHOUSE_PASSWORD="${WAREHOUSE_PASSWORD:-warehouse}"

API_KEY_NAME="${MB_API_KEY_NAME:-data-sovereignty-automation}"

note() { printf '  %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*" >&2; }

# ─── 1. Wait for Metabase ────────────────────────────────────────────────────

echo "==> Waiting for Metabase at ${MB_URL}"
for attempt in $(seq 1 120); do
  status="$(curl -fsS "${MB_URL}/api/health" 2>/dev/null | jq -r '.status // empty' 2>/dev/null || true)"
  [[ "$status" == "ok" ]] && { note "healthy"; break; }
  if [[ "$attempt" == "120" ]]; then
    echo "error: Metabase did not become healthy within 240s" >&2
    exit 1
  fi
  sleep 2
done

# ─── 2. Authenticate ─────────────────────────────────────────────────────────

# setup-token lingers in the properties response after setup completes, so
# branching on its presence would re-run the wizard and fail. Use has-user-setup.
PROPS="$(curl -fsS "${MB_URL}/api/session/properties")"
HAS_USER_SETUP="$(jq -r '.["has-user-setup"] // false' <<<"$PROPS")"
SETUP_TOKEN="$(jq -r '.["setup-token"] // empty' <<<"$PROPS")"

if [[ "$HAS_USER_SETUP" != "true" && -n "$SETUP_TOKEN" ]]; then
  echo "==> Running the setup wizard"
  SETUP_BODY="$(jq -n \
    --arg token "$SETUP_TOKEN" --arg email "$MB_ADMIN_EMAIL" \
    --arg password "$MB_ADMIN_PASSWORD" --arg first "$ADMIN_FIRST" \
    --arg last "$ADMIN_LAST" --arg site "$MB_SITE_NAME" \
    '{token: $token,
      prefs: {site_name: $site, allow_tracking: false},
      user: {email: $email, password: $password, first_name: $first,
             last_name: $last, site_name: $site}}')"
  SESSION="$(curl -fsS -X POST "${MB_URL}/api/setup" \
    -H 'Content-Type: application/json' -d "$SETUP_BODY" | jq -r '.id // empty')"
  note "created admin ${MB_ADMIN_EMAIL}"
else
  echo "==> Already set up, logging in"
  SESSION="$(curl -fsS -X POST "${MB_URL}/api/session" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n --arg u "$MB_ADMIN_EMAIL" --arg p "$MB_ADMIN_PASSWORD" \
          '{username: $u, password: $p}')" | jq -r '.id // empty')"
fi

[[ -n "${SESSION:-}" ]] || { echo "error: could not obtain a Metabase session" >&2; exit 1; }
auth=(-H "X-Metabase-Session: ${SESSION}")
note "authenticated"

# ─── 3. License check ────────────────────────────────────────────────────────
# Enterprise boots happily without a token and then behaves like OSS. Finding
# that out here beats finding it out when `mbx transforms` fails.

echo "==> Checking Enterprise license features"
FEATURES="$(curl -fsS "${auth[@]}" "${MB_URL}/api/session/properties" \
  | jq -r '.["token-features"] // {} | to_entries | map(select(.value)) | map(.key) | join(" ")')"
missing=()
for feature in transforms remote_sync library; do
  grep -qw "$feature" <<<"$FEATURES" || missing+=("$feature")
done
if (( ${#missing[@]} )); then
  warn "license is missing: ${missing[*]}"
  warn "transforms, git-sync and the Library will not work until MB_PREMIUM_EMBEDDING_TOKEN is a"
  warn "valid production token. The rest of the stack still comes up."
else
  note "transforms, remote_sync and library all present"
fi

# ─── 4. Connect the warehouse ────────────────────────────────────────────────

echo "==> Connecting the warehouse"
# Match on the physical connection, not the display name: Enterprise may relabel
# an attached database in the UI, and a user may rename it.
DB_ID="$(curl -fsS "${auth[@]}" "${MB_URL}/api/database" \
  | jq -r --arg host "$WAREHOUSE_HOST" --arg db "$WAREHOUSE_DB" \
    '(.data // .)[] | select(.details.host == $host and .details.dbname == $db) | .id' | head -n1)"

if [[ -n "$DB_ID" ]]; then
  note "already connected (id ${DB_ID})"
else
  DB_BODY="$(jq -n \
    --arg name "$WAREHOUSE_MB_NAME" --arg host "$WAREHOUSE_HOST" \
    --argjson port "$WAREHOUSE_PORT" --arg dbname "$WAREHOUSE_DB" \
    --arg user "$WAREHOUSE_USER" --arg password "$WAREHOUSE_PASSWORD" \
    '{name: $name, engine: "postgres",
      details: {host: $host, port: $port, dbname: $dbname, user: $user,
                password: $password, "schema-filters-type": "all",
                ssl: false, "tunnel-enabled": false}}')"
  DB_ID="$(curl -fsS "${auth[@]}" -X POST "${MB_URL}/api/database" \
    -H 'Content-Type: application/json' -d "$DB_BODY" | jq -r '.id // empty')"
  [[ -n "$DB_ID" ]] || { echo "error: could not add the warehouse database" >&2; exit 1; }
  note "connected as '${WAREHOUSE_MB_NAME}' (id ${DB_ID})"
fi

curl -fsS "${auth[@]}" -X POST "${MB_URL}/api/database/${DB_ID}/sync_schema" >/dev/null
note "schema sync requested"

# ─── 5. Provision an API key ─────────────────────────────────────────────────
# mb, mbx and every Airflow task authenticate with this. It is created once and
# persisted to .env; the unmasked value is only ever returned at creation time.

echo "==> Provisioning an API key"
key_works() {
  [[ -n "${1:-}" ]] && curl -fsS -o /dev/null -H "x-api-key: ${1}" "${MB_URL}/api/user/current" 2>/dev/null
}

if key_works "${MB_API_KEY:-}"; then
  note "existing MB_API_KEY still authenticates, keeping it"
else
  GROUP_ID="$(curl -fsS "${auth[@]}" "${MB_URL}/api/permissions/group" \
    | jq -r '(.data // .)[] | select(.name == "Administrators") | .id' | head -n1)"
  [[ -n "$GROUP_ID" ]] || { echo "error: could not find the Administrators group" >&2; exit 1; }

  # A name collision is rejected, so retire any key we previously created.
  EXISTING_KEY_ID="$(curl -fsS "${auth[@]}" "${MB_URL}/api/api-key" \
    | jq -r --arg name "$API_KEY_NAME" '(.data // .)[]? | select(.name == $name) | .id' | head -n1)"
  if [[ -n "$EXISTING_KEY_ID" ]]; then
    note "deleting the previous '${API_KEY_NAME}' key (its secret is unrecoverable)"
    curl -fsS "${auth[@]}" -X DELETE "${MB_URL}/api/api-key/${EXISTING_KEY_ID}" >/dev/null || true
  fi

  RESPONSE="$(curl -fsS "${auth[@]}" -X POST "${MB_URL}/api/api-key" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n --arg name "$API_KEY_NAME" --argjson group "$GROUP_ID" \
          '{name: $name, group_id: $group}')")"
  NEW_KEY="$(jq -r '.unmasked_key // .key // empty' <<<"$RESPONSE")"
  if [[ -z "$NEW_KEY" ]]; then
    echo "error: the API key response had no unmasked key. Response shape:" >&2
    jq -r 'keys | join(", ")' <<<"$RESPONSE" >&2
    exit 1
  fi

  # Persist without ever echoing the value.
  if grep -qE '^MB_API_KEY=' "$ENV_FILE"; then
    awk -v v="$NEW_KEY" 'BEGIN{FS=OFS="="} /^MB_API_KEY=/{print "MB_API_KEY=" v; next} {print}' \
      "$ENV_FILE" >"${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
  else
    printf 'MB_API_KEY=%s\n' "$NEW_KEY" >>"$ENV_FILE"
  fi
  chmod 600 "$ENV_FILE"
  note "wrote MB_API_KEY to .env"

  key_works "$NEW_KEY" || { echo "error: the new API key does not authenticate" >&2; exit 1; }
  note "verified"
fi

cat <<EOF

Metabase is ready.

  UI          ${MB_URL}
  Admin       ${MB_ADMIN_EMAIL}
  Warehouse   '${WAREHOUSE_MB_NAME}' (id ${DB_ID}) -> schemas raw_pylon, analytics, ops

Next: make mb-audit
EOF
