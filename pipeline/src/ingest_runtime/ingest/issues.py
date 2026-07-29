"""Issues resources: windowed scan and incremental.

Two fetch strategies, one table:

- window mode — GET /issues with start_time/end_time. The API caps windows at
  30 days and filters on *created_at*, so this mode answers "issues created in
  [start, end)". Used for timeframe tests and historical backfills.
- incremental mode — POST /issues/search filtering on *updated_at* after the
  stored cursor (minus a lookback overlap). This is the steady-state hourly
  mode; unlike the legacy pipeline it never re-scans history to find updates.

The cursor lives in dlt resource state, which is committed atomically with each
successful load and persisted to the destination — a failed run never advances
it, and any machine resumes where the last one stopped.
"""

import logging

import dlt
import pendulum

from .hints import ISSUE_HINTS
from .settings import (
    BACKFILL_START,
    INCREMENTAL_LOOKBACK_SECONDS,
    ISSUES_LIST_PAGE_LIMIT,
    ISSUES_SEARCH_PAGE_LIMIT,
    ISSUES_WINDOW_DAYS,
)
from .transform import flatten_issue

log = logging.getLogger(__name__)


def rfc3339(dt):
    return dt.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def iter_windows(start, end, days=ISSUES_WINDOW_DAYS):
    cursor = start
    while cursor < end:
        window_end = min(cursor.add(days=days), end)
        yield cursor, window_end
        cursor = window_end


def issues_window_resource(client, start, end):
    @dlt.resource(name="issues", write_disposition="merge", primary_key="id", columns=ISSUE_HINTS)
    def issues():
        total = 0
        for window_start, window_end in iter_windows(start, end):
            label = f"issues {window_start:%Y-%m-%d}→{window_end:%Y-%m-%d}"
            params = {
                "start_time": rfc3339(window_start),
                "end_time": rfc3339(window_end),
                "limit": ISSUES_LIST_PAGE_LIMIT,
            }
            window_count = 0
            for page in client.paginate_get("issues", params, family="issues_list", label=label):
                for record in page:
                    yield flatten_issue(record)
                window_count += len(page)
            total += window_count
            log.info("[issues] window %s→%s: %d issues (%d total)",
                     window_start.date(), window_end.date(), window_count, total)
        log.info("[issues] window scan done: %d issues", total)

    return issues


def issues_incremental_resource(client, lookback_seconds=INCREMENTAL_LOOKBACK_SECONDS):
    @dlt.resource(name="issues", write_disposition="merge", primary_key="id", columns=ISSUE_HINTS)
    def issues():
        state = dlt.current.resource_state()
        stored = state.get("last_updated_at") or BACKFILL_START
        cursor = pendulum.parse(str(stored))
        since = cursor.subtract(seconds=lookback_seconds)
        now = pendulum.now("UTC")
        log.info("[issues] incremental: updated_at in [%s, %s] (cursor %s minus %ds lookback)",
                 rfc3339(since), rfc3339(now), rfc3339(cursor), lookback_seconds)

        max_seen = cursor
        total = 0
        # Bound each search to a <=30-day time_range: Pylon caps its GET time
        # windows at 30 days, and an open-ended time_is_after from 2019 (first
        # run / post-backfill) is the risky case. In steady state `since` is
        # within the last 30 days, so this is a single window. Contiguous
        # windows share their boundary instant; merge on id dedupes the overlap.
        for window_start, window_end in iter_windows(since, now):
            body = {
                "filter": {
                    "field": "updated_at",
                    "operator": "time_range",
                    "values": [rfc3339(window_start), rfc3339(window_end)],
                },
                "limit": ISSUES_SEARCH_PAGE_LIMIT,
            }
            label = f"issues/search {window_start:%Y-%m-%d}→{window_end:%Y-%m-%d}"
            for page in client.paginate_search(body, label=label):
                for record in page:
                    flat = flatten_issue(record)
                    updated_at = flat.get("updated_at")
                    if updated_at is not None and updated_at > max_seen:
                        max_seen = updated_at
                    yield flat
                total += len(page)

        if total == 0:
            # The scan proved [since, now] empty. Leaving the cursor where it is
            # would re-scan the same span every run — on a quiet or freshly
            # provisioned tenant that means re-windowing from BACKFILL_START
            # hourly, forever. Park it at the horizon we just cleared; the
            # lookback overlap on the next run still covers the boundary.
            max_seen = now

        state["last_updated_at"] = max_seen.isoformat()
        log.info("[issues] incremental fetched %d updated issues; cursor now %s", total, max_seen.isoformat())

    return issues
