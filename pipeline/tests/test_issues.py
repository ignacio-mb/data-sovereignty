from itertools import pairwise

import pendulum

from ingest_runtime.ingest.issues import iter_windows, rfc3339


def test_iter_windows_caps_at_30_days_and_handles_partial_tail():
    start = pendulum.datetime(2026, 1, 1, tz="UTC")
    end = pendulum.datetime(2026, 3, 15, tz="UTC")
    windows = list(iter_windows(start, end))
    assert windows[0] == (start, start.add(days=30))
    assert all((w_end - w_start).in_days() <= 30 for w_start, w_end in windows)
    # contiguous, no gaps or overlaps
    for (_, prev_end), (next_start, _) in pairwise(windows):
        assert prev_end == next_start
    assert windows[-1][1] == end


def test_iter_windows_single_short_window():
    start = pendulum.datetime(2026, 6, 1, tz="UTC")
    end = pendulum.datetime(2026, 6, 8, tz="UTC")
    assert list(iter_windows(start, end)) == [(start, end)]


def test_rfc3339_is_utc_zulu():
    dt = pendulum.datetime(2026, 6, 1, 14, 30, 0, tz="America/New_York")
    assert rfc3339(dt) == "2026-06-01T18:30:00Z"
