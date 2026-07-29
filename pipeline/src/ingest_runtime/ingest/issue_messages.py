"""issue_messages resource: per-issue fetch driven by a warehouse watermark.

Pylon has no cross-issue messages endpoint, so messages are fetched one issue
at a time. The worklist comes from the warehouse: issues whose
latest_message_time is newer than the newest message already loaded for them
(see warehouse.pending_message_issue_ids). An optional wall-clock budget ends
long runs *cleanly* — the partial batch loads, the watermark advances for the
issues that were fetched, and the next run resumes with the rest.
"""

import logging
import time

import dlt

from .hints import MESSAGE_HINTS
from .transform import enrich_message

log = logging.getLogger(__name__)


class TimeBudget:
    def __init__(self, minutes, clock=time.monotonic):
        self._clock = clock
        self._deadline = None if minutes is None else clock() + minutes * 60

    def exhausted(self):
        return self._deadline is not None and self._clock() >= self._deadline


def issue_messages_resource(client, pending_issue_ids, budget_minutes=None):
    """pending_issue_ids is a callable returning the worklist of issue ids;
    it runs at extract time so it sees the issues loaded earlier in this run."""

    @dlt.resource(name="issue_messages", write_disposition="merge", primary_key="id", columns=MESSAGE_HINTS)
    def issue_messages():
        ids = list(pending_issue_ids())
        budget = TimeBudget(budget_minutes)
        budget_note = f" (budget {budget_minutes} min)" if budget_minutes else ""
        log.info("[issue_messages] %d issues need message fetches%s", len(ids), budget_note)

        fetched_issues = 0
        total_messages = 0
        for issue_id in ids:
            if budget.exhausted():
                log.warning(
                    "[issue_messages] time budget exhausted after %d/%d issues; the rest resume next run",
                    fetched_issues, len(ids),
                )
                break
            messages = client.get_issue_messages(issue_id)
            fetched_issues += 1
            if messages is None:  # issue gone/scrubbed — skipped
                continue
            for message in messages:
                yield enrich_message(message, issue_id)
            total_messages += len(messages)
            if fetched_issues % 25 == 0:
                log.info("[issue_messages] %d/%d issues · %d messages so far",
                         fetched_issues, len(ids), total_messages)

        log.info("[issue_messages] done: %d messages from %d issues", total_messages, fetched_issues)

    return issue_messages
