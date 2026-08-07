#!/usr/bin/env bash
# Runs the Lever RUNBOOK in sources/lever.yml end to end: one resource group at
# a time, waiting for each to actually succeed before triggering the next.
#
# The pool (lever_pipeline, size 1) and lever_backfill's own max_active_runs=1
# already make CONCURRENT execution impossible — triggering all ten groups
# back to back would still run them one at a time. What they do NOT do is stop
# the sequence on a failure: max_active_runs only tracks whether a run is
# ACTIVE, not whether it succeeded, so a failed run still frees the slot and
# the next queued group starts anyway. Ten `airflow dags trigger` calls fired
# up front can silently burn through the whole ~2.5-3 day sequence with one
# resource broken in the middle and nobody the wiser until every group has
# reported in. This script is the alternative to babysitting that by hand.
#
# Run this INSIDE the tmux session (ssh -t data-sovereignty tmux new -A -s ops
# per docs/deploy.md), not as a one-off SSH command — it blocks for the whole
# sequence and must survive a disconnect. Foreground on purpose: the point is
# to see it stop the moment something fails, not discover it a day later.
#
# Prerequisite: lever_backfill itself must be UNPAUSED. Only lever_ingest and
# lever_reconcile ship paused (ships_paused in the spec) — lever_backfill is
# schedule=None and manually triggered regardless, so ships_paused never
# touches it. If it was paused by hand during testing, undo that first:
#   docker compose --profile cli run --rm airflow-cli airflow dags unpause lever_backfill
set -euo pipefail

AIRFLOW="docker compose --profile cli run --rm airflow-cli airflow"

# Order matches the RUNBOOK comment in sources/lever.yml exactly: lookups
# first (cheap, no watermark to protect), then opportunities, then its nine
# children in any order.
RESOURCE_GROUPS=(
  "lookups:postings,users,stages,archive_reasons,sources,tags,requisitions,requisition_fields"
  "opportunities:opportunities"
  "applications:applications"
  "notes:notes"
  "interviews:interviews"
  "feedback:feedback"
  "offers:offers"
  "panels:panels"
  "referrals:referrals"
  "resumes:resumes"
  "forms:forms"
)

POLL_SECONDS=300  # these runs take hours; no need to poll more often than this

# The CLI prints plugin/alembic setup noise on every cold container start —
# `docker compose run` is a fresh container each call — so the JSON payload is
# reliably the LAST line of stdout, never the whole stream.
run_state() {
  $AIRFLOW dags list-runs lever_backfill -o json 2>/dev/null | tail -1 | python3 -c "
import json, sys
rows = json.load(sys.stdin)
row = next((r for r in rows if r['run_id'] == '$1'), None)
print(row['state'] if row else 'missing')
"
}

for entry in "${RESOURCE_GROUPS[@]}"; do
  label="${entry%%:*}"
  resources="${entry#*:}"
  run_id="backfill-${label}-$(date +%Y%m%dT%H%M%S)"

  echo "=== ${label} :: triggering ${resources} (run_id=${run_id}) ==="
  $AIRFLOW dags trigger lever_backfill \
    --run-id "${run_id}" \
    --conf "{\"resources\": \"${resources}\"}"

  while true; do
    state=$(run_state "${run_id}")
    echo "$(date -u +%H:%M:%S) ${label}: ${state}"
    case "$state" in
      success) break ;;
      failed)
        echo "!!! ${label} FAILED (run_id=${run_id}) -- stopping sequence." >&2
        echo "!!! Check logs, fix, and re-run just this group before continuing." >&2
        exit 1
        ;;
    esac
    sleep "$POLL_SECONDS"
  done
  echo "=== ${label} succeeded ==="
done

echo "All resource groups backfilled successfully."
echo "Now: airflow dags unpause lever_ingest && airflow dags unpause lever_reconcile"
