#!/usr/bin/env bash
# Move the host towards the commit that should be live, if it is not already.
# Installed to /usr/local/bin/ds-converge and run by a timer.
#
# The commit comes from Parameter Store, written by the CI role after the
# checks passed — not from polling GitHub — so this carries no gate of its own
# and cannot deploy something CI never approved.
#
# It exists because a deploy can legitimately decline to happen: the weekly
# reconcile holds the ingest pool for twenty hours, and a merge during that
# window would otherwise never land. It also covers a merge that arrived while
# the instance was rebooting.
set -euo pipefail

DEPLOY_DIR="${DS_DEPLOY_DIR:-/data/deploy}"
STATE_FILE="${DEPLOY_DIR}/state.json"
PARAM="${DS_DESIRED_SHA_PARAM:-/data-sovereignty/prod/deploy/desired_sha}"
REGION="${AWS_REGION:-$(curl -fsS --max-time 2 -H "X-aws-ec2-metadata-token: $(curl -fsS --max-time 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo us-east-1)}"

desired="$(aws ssm get-parameter --region "$REGION" --name "$PARAM" \
  --query 'Parameter.Value' --output text 2>/dev/null || true)"

if [[ ! "$desired" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ds-converge: no deployable commit recorded at ${PARAM}"
  exit 0
fi

current=""
[[ -f "$STATE_FILE" ]] && current="$(jq -r '.last_complete_sha // empty' "$STATE_FILE")"

if [[ "$desired" == "$current" ]]; then
  exit 0
fi

echo "ds-converge: ${current:-unknown} -> ${desired}"

# ds-deploy takes the lock, re-checks ancestry and may decline (76) because
# something is in flight. Declining is normal here — the next tick retries.
set +e
/usr/local/bin/ds-deploy --sha "$desired" >/dev/null
code=$?
set -e

FAILURES="${DEPLOY_DIR}/converge-failures"

case "$code" in
  0)  echo "ds-converge: deployed ${desired}"; rm -f "$FAILURES" ;;
  75) echo "ds-converge: a deploy is already running" ;;
  76) echo "ds-converge: work in flight, will retry" ;;
  79) echo "ds-converge: on hold or the checkout is on another branch" ;;
  *)
    # state.json only advances on success, so a commit that fails late would
    # otherwise be retried every five minutes forever — rebuilding each time,
    # because the diff base never moves. Stop after three and say so, rather
    # than grinding at the box unattended.
    count=$(( $(cat "$FAILURES" 2>/dev/null || echo 0) + 1 ))
    echo "$count" >"$FAILURES"
    echo "ds-converge: deploy of ${desired} failed with ${code} (attempt ${count}); see ${DEPLOY_DIR}/run-${desired}.log" >&2
    if (( count >= 3 )); then
      echo "${desired} failed ${count} times — converge stopped, clear this file to resume" >"${DEPLOY_DIR}/HOLD"
      echo "ds-converge: put the deploy on hold after ${count} failures" >&2
    fi
    ;;
esac

exit "$code"
