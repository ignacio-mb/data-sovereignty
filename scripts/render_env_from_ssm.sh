#!/usr/bin/env bash
# Render .env on the instance from SSM Parameter Store, plus the handful of
# settings that are true of this host and not of a laptop.
#
# Safe to re-run: it overwrites the keys Parameter Store owns and leaves
# everything else alone. In particular it never touches MB_API_KEY, which
# scripts/bootstrap_metabase.sh mints into this file and which pairs with the
# metabase-app-data volume beside it.
#
# It never calls gen_secrets.sh. That script appends a freshly generated value
# for a key that is missing, and a new AIRFLOW__CORE__FERNET_KEY makes every
# encrypted Airflow connection undecryptable — silently.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"
PREFIX="${DS_PARAMETER_PREFIX:-/data-sovereignty/prod}"
REGION="${AWS_REGION:-$(curl -fsS --max-time 2 -H "X-aws-ec2-metadata-token: $(curl -fsS --max-time 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo us-east-1)}"

command -v aws >/dev/null 2>&1 || { echo "error: aws CLI is required" >&2; exit 1; }

if [[ ! -f "$ENV_FILE" ]]; then
  cp "${REPO_ROOT}/.env.example" "$ENV_FILE"
  echo "render_env: created $ENV_FILE from .env.example"
fi
chmod 600 "$ENV_FILE"

# Set a key in place, appending if it is not there. The value goes through the
# environment rather than `awk -v`, which would interpret backslash escapes in
# a password.
set_key() {
  local key="$1" value="$2"
  DS_VALUE="$value" awk -v k="$key" '
    BEGIN { FS = OFS = "="; set = 0 }
    index($0, k "=") == 1 { print k "=" ENVIRON["DS_VALUE"]; set = 1; next }
    { print }
    END { if (!set) print k "=" ENVIRON["DS_VALUE"] }
  ' "$ENV_FILE" >"${ENV_FILE}.tmp"
  mv "${ENV_FILE}.tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}

# Direct children only, so /deploy/desired_sha and anything else nested stays
# out of .env.
#
# Values are carried base64-encoded: `--output text` splits on tabs, and an
# API key or password containing a tab or a newline would otherwise be split
# in the wrong place and written into .env as junk.
mapfile -t lines < <(
  aws ssm get-parameters-by-path \
    --region "$REGION" \
    --path "$PREFIX" \
    --with-decryption \
    --output json \
    | jq -r '.Parameters[] | [.Name, (.Value | @base64)] | @tsv'
)

count=0
for line in "${lines[@]}"; do
  name="${line%%$'\t'*}"
  value="$(base64 -d <<<"${line#*$'\t'}")"
  key="${name##*/}"
  # Host credential, not stack configuration.
  [[ "$key" == "TAILSCALE_AUTHKEY" ]] && continue
  set_key "$key" "$value"
  echo "render_env: set $key"
  count=$((count + 1))
done

if (( count == 0 )); then
  echo "render_env: no parameters under ${PREFIX} — run 'make secrets-push' from the laptop" >&2
  exit 1
fi

# ─── What is true of this host and not of a laptop ───────────────────────────

# A host seeded from an older .env.example still pins these as bare tags, which
# shadow the digest-pinned defaults in docker-compose.yml — and this image is
# built as root on the host, which is the whole reason those pins exist.
# Removing the keys hands the decision back to git.
drop_key() {
  grep -vE "^${1}=" "$ENV_FILE" >"${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}
for stale in AIRFLOW_BASE_IMAGE METABASE_IMAGE; do
  if grep -qE "^${stale}=" "$ENV_FILE"; then
    drop_key "$stale"
    echo "render_env: dropped ${stale}; docker-compose.yml owns it"
  fi
done

# The containers run as this uid so that files they write into the bind mounts
# — ./docs, ./airflow/logs, ./quality/gx_output — stay writable from a shell,
# and vice versa. Docker Desktop hides the mismatch; Linux does not.
set_key AIRFLOW_UID "$(id -u)"

# Published on the loopback interface only. The security group has no ingress
# rules, so this is belt and braces — but Docker writes its own iptables
# rules, and Airflow's UI grants an admin session to anyone who reaches it.
# SSM port forwarding still works: the agent connects from the host itself.
set_key METABASE_HOST_PORT "127.0.0.1:3100"
set_key AIRFLOW_HOST_PORT "127.0.0.1:8080"
set_key WAREHOUSE_HTTP_PORT "127.0.0.1:8124"
set_key WAREHOUSE_NATIVE_PORT "127.0.0.1:9001"
set_key DATADOCS_HOST_PORT "127.0.0.1:8081"

# The committed defaults are sized for a laptop that is also running a browser
# and an IDE. This host has 16 GB and runs nothing else, so:
#
#   ClickHouse            5.0     Metabase (2 GB heap)  3.0
#   Airflow, four         3.0     Postgres, two         0.5
#   ingest bursts         1.5     host and Docker       1.0
#                                 ─────────────────────────
#                                 14.0, plus a 4 GB swapfile as the shock
#                                 absorber and the remainder as page cache,
#                                 which is most of what ClickHouse speed is.
#
# Raise the Metabase heap and its limit together; the JVM must stay inside the
# cgroup or the kernel picks the container to kill, not the query.
set_key CLICKHOUSE_MEM_LIMIT "5g"
set_key METABASE_MEM_LIMIT "3g"
set_key METABASE_JAVA_OPTS "-Xmx2g"

echo "render_env: done — ${count} parameters, ports bound to loopback"
