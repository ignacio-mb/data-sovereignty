"""Proactive per-endpoint-family request pacing.

Pylon publishes fixed per-endpoint budgets (requests/minute). Instead of
hammering until a 429 and sleeping on Retry-After, requests are spaced evenly:
before each request, sleep whatever remains of the family's minimum interval
since that family's previous request. Retry-After handling on an actual 429
still exists underneath in dlt's requests session; this pacer just makes 429s
rare in the first place.
"""

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

    def wait(self, family):
        """Block until the family's budget allows another request, then record it."""
        interval = self._interval[family]
        last = self._last.get(family)
        if last is not None:
            remaining = interval - (self._clock() - last)
            if remaining > 0:
                self._sleep(remaining)
        self._last[family] = self._clock()
        self.requests_made[family] += 1
