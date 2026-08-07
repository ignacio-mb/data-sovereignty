"""Proactive per-endpoint-family request pacing.

A spec publishes fixed per-endpoint-family budgets (requests/minute) under
`rate_limits`, filled in from the API's own documentation. Instead of
hammering until a 429 and sleeping on Retry-After, requests are spaced evenly:
before each request, sleep whatever remains of the family's minimum interval
since that family's previous request. Retry-After handling on an actual 429
still exists underneath in dlt's requests session; this pacer just makes 429s
rare in the first place.
"""

import threading
import time
from collections import Counter


class EndpointPacer:
    def __init__(self, budgets_per_minute, sleeper=None, clock=None):
        self._interval = {family: 60.0 / rpm for family, rpm in budgets_per_minute.items()}
        self._last = {}
        # Resolved here rather than as `sleeper=time.sleep` in the signature: a
        # default argument captures the function object at import time, which
        # silently defeats any later monkeypatch of time.sleep. Tests that
        # exercise pacing through the CLI depend on being able to patch it.
        self._sleep = sleeper if sleeper is not None else time.sleep
        self._clock = clock if clock is not None else time.monotonic
        self.requests_made = Counter()
        # A source with a concurrent fetch loop (Lever's per-opportunity
        # fan-out is the case this was added for) shares ONE pacer across
        # worker threads, so the budget is enforced across all of them
        # together rather than per-thread. Without the lock, "read remaining,
        # maybe sleep, write last" is three separate steps a second thread can
        # interleave into — two threads both reading a stale `last` value and
        # both concluding they are clear to go, letting the real request rate
        # slip past the budget. The lock costs nothing measurable in the
        # single-threaded case this was originally written for.
        self._lock = threading.Lock()

    def wait(self, family):
        """Block until the family's budget allows another request, then record it.

        A family with no published budget is counted but never slept on. A spec
        declares `rate_limits` per family and need not cover every endpoint, so an
        undeclared one means "no limit worth pacing" — and raising here would take
        a whole connector down over the one endpoint whose budget nobody wrote down.
        """
        with self._lock:
            interval = self._interval.get(family)
            if interval is None:
                self.requests_made[family] += 1
                return
            last = self._last.get(family)
            if last is not None:
                remaining = interval - (self._clock() - last)
                if remaining > 0:
                    self._sleep(remaining)
            self._last[family] = self._clock()
            self.requests_made[family] += 1
