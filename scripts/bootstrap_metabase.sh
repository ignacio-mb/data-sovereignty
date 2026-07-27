#!/usr/bin/env bash
#
# Bring a fresh Metabase instance to the point where the rest of the stack can
# use it:
#   1. wait for it to be healthy
#   2. run the setup wizard, unless it has already been run
#   3. provision an API key and write it into .env for mb / mbx / Airflow
#   4. verify the Enterprise license grants the features we need
#   5. connect the warehouse and sync its schema
#
# Idempotent. Re-running skips the wizard, keeps a working API key, and reuses
# an existing database connection.
#
# ── Why some steps use `mb` and others use curl ─────────────────────────────
#
# `mb` is the interface to Metabase wherever a command exists — it owns
# credential resolution, retries, redaction and a versioned contract, and
# re-implementing any of that against the REST API means owning it forever.
#
# Exactly three things here have no `mb` command and therefore use curl:
#   * the health poll                  (there is no `mb health`)
#   * password-based session login     (`mb auth login` wants an API key)
#   * creating a database connection and an API key
# Each is marked below. If a future mb release adds a command for one of them,
# delete the curl.
#
# Requires bash, curl, jq, and mb (npm install -g @metabase/cli).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"

for tool in jq mb; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: $tool is required" >&2
    [[ "$tool" == "mb" ]] && echo "  npm install -g @metabase/cli" >&2
    exit 1
  }
done

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

# The script runs on the host, so it talks to the published port. The warehouse
# details below are how METABASE sees the warehouse, i.e. on the compose network.
export MB_URL="${MB_URL:-http://localhost:${METABASE_HOST_PORT:-3100}}"
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

# mb never prompts for a keychain in a script.
export MB_CLI_DISABLE_KEYRING=1

note() { printf '  %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*" >&2; }

# Some Metabase list endpoints answer with a bare array and others with a
# {data: [...]} envelope. `.data // .` is not enough: jq raises on indexing an
# array with a string. (Only needed for the raw-REST fallbacks below; `mb`
# normalises this itself.)
UNWRAP='(if type == "object" then (.data // []) else . end)[]'

# ─── 1. Wait for Metabase ────────────────────────────────────────────────────
# curl: no `mb` command reports server health.

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

# ─── 2. Setup wizard ─────────────────────────────────────────────────────────

# The setup token lingers in the properties response after setup completes, so
# branching on its presence would re-run the wizard and fail. Use has-user-setup.
PROPS="$(curl -fsS "${MB_URL}/api/session/properties")"
HAS_USER_SETUP="$(jq -r '.["has-user-setup"] // false' <<<"$PROPS")"
SETUP_TOKEN="$(jq -r '.["setup-token"] // empty' <<<"$PROPS")"

if [[ "$HAS_USER_SETUP" != "true" && -n "$SETUP_TOKEN" ]]; then
  echo "==> Running the setup wizard"
  jq -n --arg token "$SETUP_TOKEN" --arg email "$MB_ADMIN_EMAIL" \
        --arg password "$MB_ADMIN_PASSWORD" --arg first "$ADMIN_FIRST" \
        --arg last "$ADMIN_LAST" --arg site "$MB_SITE_NAME" \
    '{token: $token,
      user: {email: $email, password: $password, first_name: $first, last_name: $last},
      prefs: {site_name: $site}}' \
    | mb setup --file /dev/stdin --json --max-bytes 0 >/dev/null
  note "created admin ${MB_ADMIN_EMAIL}"
else
  note "already set up"
fi

# ─── 3. Provision an API key ─────────────────────────────────────────────────
# Done before anything else that needs auth, because everything after this point
# is an `mb` call and `mb` authenticates with MB_API_KEY.
#
# curl: neither password-based session login nor API-key creation has an `mb`
# command — `mb auth login` needs a key, which is what we are here to create.

echo "==> Provisioning an API key"
key_works() {
  [[ -n "${1:-}" ]] && MB_API_KEY="$1" mb auth status --json --max-bytes 0 >/dev/null 2>&1
}

if key_works "${MB_API_KEY:-}"; then
  note "existing MB_API_KEY still authenticates, keeping it"
else
  SESSION="$(curl -fsS -X POST "${MB_URL}/api/session" \
    -H 'Content-Type: application/json' \
    -d "$(jq -n --arg u "$MB_ADMIN_EMAIL" --arg p "$MB_ADMIN_PASSWORD" \
          '{username: $u, password: $p}')" | jq -r '.id // empty')"
  [[ -n "$SESSION" ]] || { echo "error: could not log in as ${MB_ADMIN_EMAIL}" >&2; exit 1; }
  auth=(-H "X-Metabase-Session: ${SESSION}")

  GROUP_ID="$(curl -fsS "${auth[@]}" "${MB_URL}/api/permissions/group" \
    | jq -r "${UNWRAP} | select(.name == \"Administrators\") | .id" | head -n1)"
  [[ -n "$GROUP_ID" ]] || { echo "error: could not find the Administrators group" >&2; exit 1; }

  # A name collision is rejected, so retire any key we previously created.
  EXISTING_KEY_ID="$(curl -fsS "${auth[@]}" "${MB_URL}/api/api-key" \
    | jq -r --arg name "$API_KEY_NAME" "${UNWRAP} | select(.name == \$name) | .id" | head -n1)"
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
  export MB_API_KEY="$NEW_KEY"

  key_works "$MB_API_KEY" || { echo "error: the new API key does not authenticate" >&2; exit 1; }
  note "wrote MB_API_KEY to .env and verified it"
fi

# Everything below authenticates as mb, via MB_URL + MB_API_KEY in the environment.

# ─── 4. License check ────────────────────────────────────────────────────────
# Enterprise boots happily without a token and then behaves like OSS. Finding
# that out here beats finding it out when `mbx transforms` fails.

echo "==> Checking Enterprise license features"
FEATURES="$(mb setting get token-features --json --max-bytes 0 \
  | jq -r '.value // {} | to_entries | map(select(.value)) | map(.key) | join(" ")')"
missing=()
# Exact feature names as reported by the server. It is "transforms-basic":
# there is no feature called plain "transforms".
for feature in transforms-basic remote_sync library; do
  grep -qw "$feature" <<<"$FEATURES" || missing+=("$feature")
done
if (( ${#missing[@]} )); then
  warn "license is missing: ${missing[*]}"
  warn "transforms, git-sync and the Library will not work until MB_PREMIUM_EMBEDDING_TOKEN is a"
  warn "valid production token. The rest of the stack still comes up."
else
  note "transforms-basic, remote_sync and library all present"
fi

# ─── 5. Connect the warehouse ────────────────────────────────────────────────

echo "==> Connecting the warehouse"
# Match on the physical connection, not the display name: Enterprise may relabel
# an attached database in the UI, and a user may rename it.
#
# --full is load-bearing. mb's default list projection is compact and omits
# `details` entirely, so the match silently never fires and every run adds
# another duplicate connection.
DB_ID="$(mb db list --full --json --max-bytes 0 \
  | jq -r --arg host "$WAREHOUSE_HOST" --arg db "$WAREHOUSE_DB" \
    "${UNWRAP} | select(.details.host == \$host and .details.dbname == \$db) | .id" \
  | sort -n | head -n1)"

if [[ -n "$DB_ID" ]]; then
  note "already connected (id ${DB_ID})"
else
  # curl: `mb db` has list/get/schemas/sync-schema/rescan-values but no create.
  DB_BODY="$(jq -n \
    --arg name "$WAREHOUSE_MB_NAME" --arg host "$WAREHOUSE_HOST" \
    --argjson port "$WAREHOUSE_PORT" --arg dbname "$WAREHOUSE_DB" \
    --arg user "$WAREHOUSE_USER" --arg password "$WAREHOUSE_PASSWORD" \
    '{name: $name, engine: "postgres",
      details: {host: $host, port: $port, dbname: $dbname, user: $user,
                password: $password, "schema-filters-type": "all",
                ssl: false, "tunnel-enabled": false}}')"
  DB_ID="$(curl -fsS -H "x-api-key: ${MB_API_KEY}" -X POST "${MB_URL}/api/database" \
    -H 'Content-Type: application/json' -d "$DB_BODY" | jq -r '.id // empty')"
  [[ -n "$DB_ID" ]] || { echo "error: could not add the warehouse database" >&2; exit 1; }
  note "connected as '${WAREHOUSE_MB_NAME}' (id ${DB_ID})"
fi

mb db sync-schema "$DB_ID" --wait --json --max-bytes 0 >/dev/null
note "schema synced"

cat <<EOF

Metabase is ready.

  UI          ${MB_URL}
  Admin       ${MB_ADMIN_EMAIL}
  Warehouse   '${WAREHOUSE_MB_NAME}' (id ${DB_ID}) -> schemas raw_pylon, analytics, ops

Next: make mb-audit
EOF
