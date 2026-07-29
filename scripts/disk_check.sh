#!/usr/bin/env bash
# What is using /data, and is it about to be a problem.
#
# A full data volume is the one host failure that breaks ClickHouse and
# corrupts an in-flight dlt load at the same time, and nothing inside the stack
# watches for it. CloudWatch alarms on the same threshold; this is the version
# you can read.
set -euo pipefail

THRESHOLD="${DS_DISK_THRESHOLD:-80}"

df -h /data
echo
docker system df 2>/dev/null || true

used="$(df -P /data | awk 'NR==2 { gsub(/%/, "", $5); print $5 }')"
echo
echo "/data is ${used}% used (threshold ${THRESHOLD}%)"

if [[ "$used" -ge "$THRESHOLD" ]]; then
  cat >&2 <<EOF

/data is over the threshold. In rough order of what to reclaim:
  docker image prune -f --filter until=336h     # old build layers
  docker builder prune -f --filter until=336h
  find airflow/logs -type f -mtime +30 -delete  # task logs
  ls -la /data/deploy/                          # deploy logs and saved patches
EOF
  exit 1
fi
