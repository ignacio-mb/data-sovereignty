#!/usr/bin/env bash
# Stage A of a deploy. Installed to /usr/local/bin/ds-deploy by the host
# bootstrap, deliberately OUTSIDE the working tree: everything here decides
# whether a commit may be deployed at all, so it must not be something a
# commit can change. Updating it is a host-provisioning step, not a merge.
#
# It does as little as possible, then hands over to scripts/stack_update.sh
# taken FROM THE COMMIT BEING DEPLOYED — so the deploy logic is versioned with
# the thing it deploys, while the trust decision is not.
set -euo pipefail

PROJECT="${DS_PROJECT:-data-sovereignty}"
REPO_DIR="${DS_REPO_DIR:-/data/${PROJECT}}"
BRANCH="${DS_BRANCH:-main}"
OWNER="${DS_OWNER:-ubuntu}"
LOCK_DIR="/run/${PROJECT}"
STAGE_B="${LOCK_DIR}/stack_update.$$.sh"

SHA=""
ALLOW_IN_FLIGHT="false"
FORCE_REBUILD="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sha)             SHA="$2"; shift 2 ;;
    --allow-in-flight) ALLOW_IN_FLIGHT="$2"; shift 2 ;;
    --force-rebuild)   FORCE_REBUILD="$2"; shift 2 ;;
    *) echo "ds-deploy: unknown argument $1" >&2; exit 2 ;;
  esac
done

[[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || { echo "ds-deploy: --sha must be a full commit sha" >&2; exit 2; }

# One deploy at a time. The lock directory is root-owned: /var/lock is
# world-writable, so any local user could pre-create the file and stall every
# deploy.
mkdir -p "$LOCK_DIR"
exec 9>"${LOCK_DIR}/deploy.lock"
if ! flock -w 900 9; then
  echo "ds-deploy: another deploy holds the lock" >&2
  exit 75
fi

if [[ -f /data/deploy/HOLD ]]; then
  echo "ds-deploy: on hold — $(cat /data/deploy/HOLD)" >&2
  exit 79
fi

cd "$REPO_DIR"

# This script runs as root against a checkout owned by someone else, which git
# refuses to touch — "detected dubious ownership" — unless the path is marked
# safe. Passed per command rather than written into root's global config, so
# the exemption is exactly this repository and nothing else.
git() { command git -c safe.directory="$REPO_DIR" "$@"; }

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" != "$BRANCH" ]]; then
  echo "ds-deploy: checkout is on '$CURRENT_BRANCH', not '$BRANCH' — someone is working here" >&2
  exit 79
fi

# Only ever fetch the deployed branch, and no tags.
runuser -u "$OWNER" -- git fetch --prune --no-tags origin \
  "+refs/heads/${BRANCH}:refs/remotes/origin/${BRANCH}"

# The guard that matters, and the reason it lives in Stage A: a commit is
# deployable only if it is already an ancestor of origin/main. Checking this
# after running any code from the target commit would let the payload
# authorise itself.
if ! git merge-base --is-ancestor "$SHA" "refs/remotes/origin/${BRANCH}"; then
  echo "ds-deploy: ${SHA} is not an ancestor of origin/${BRANCH}" >&2
  exit 78
fi

OLD_SHA="$(git rev-parse HEAD)"

# Extract Stage B without checking anything out, and with hooks disabled —
# a repository hook would otherwise run as root, here, before any of the
# safety checks in Stage B have happened.
git -c core.hooksPath=/dev/null show "${SHA}:scripts/stack_update.sh" >"$STAGE_B"
chmod 0755 "$STAGE_B"
chown "$OWNER" "$STAGE_B"
trap 'rm -f "$STAGE_B"' EXIT

# Everything from here runs as the user that owns the tree, never as root.
# bootstrap_metabase.sh replaces .env with a fresh file, and a root-owned
# replacement locks the stack out of its own state permanently.
#
# Run rather than exec, so the temporary file is cleaned up afterwards and the
# lock on fd 9 is held by this shell for the whole deploy. Stage B's JSON
# summary is on stdout and passes straight through.
set +e
runuser -u "$OWNER" -- "$STAGE_B" \
  --sha "$SHA" \
  --old-sha "$OLD_SHA" \
  --allow-in-flight "$ALLOW_IN_FLIGHT" \
  --force-rebuild "$FORCE_REBUILD"
code=$?
set -e
exit "$code"
