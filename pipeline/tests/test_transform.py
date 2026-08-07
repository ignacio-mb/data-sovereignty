"""`_parse_ts`: the one function standing between a raw API value and a column
dlt will actually type as a timestamp. Unit-tested directly because the
full-pipeline route (spin up dlt against duckdb) is what originally hid the
milliseconds gap this covers — the coercion failure is silent at that layer,
not an error, so a fast direct test is what actually catches a regression.
"""

import datetime

import pendulum
import pytest

from ingest_runtime.ingest.transform import _parse_ts


class TestStrings:
    def test_rfc3339_string_parses(self):
        assert _parse_ts("2026-07-31T08:04:26Z") == pendulum.datetime(2026, 7, 31, 8, 4, 26)

    def test_none_and_empty_string_are_none(self):
        assert _parse_ts(None) is None
        assert _parse_ts("") is None


class TestEpochNumbers:
    """Customer.io sends seconds; Lever (and most JS-originated APIs) send
    milliseconds. Both must land as the same real instant."""

    def test_seconds_precision_int(self):
        # 10 digits — Customer.io's actual shape.
        result = _parse_ts(1785485066)
        assert result.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(1785485066, datetime.UTC)

    def test_milliseconds_precision_int(self):
        # 13 digits — Lever's actual shape. Same instant as the seconds case
        # above, just carrying three extra digits of (fractional) precision.
        result = _parse_ts(1785485066000)
        assert result.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(1785485066, datetime.UTC)

    def test_milliseconds_precision_float(self):
        result = _parse_ts(1785485066123.0)
        assert result.astimezone(datetime.UTC) == \
            datetime.datetime.fromtimestamp(1785485066.123, datetime.UTC)

    def test_zero_is_not_none(self):
        # Some APIs (Customer.io's `scheduled_start`) use 0 as a "never"
        # sentinel on a field that is deliberately NOT hinted as a timestamp
        # for exactly that reason — but _parse_ts itself must not treat a
        # real zero epoch as absence, only None/"" are.
        result = _parse_ts(0)
        assert result.astimezone(datetime.UTC) == datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

    def test_bool_is_not_treated_as_an_epoch(self):
        # bool is a subclass of int in Python; True/False must pass through
        # unchanged rather than being coerced into 1970-01-01 or thereabouts.
        assert _parse_ts(True) is True
        assert _parse_ts(False) is False


class TestPassthrough:
    def test_non_numeric_non_string_passes_through(self):
        already = pendulum.now()
        assert _parse_ts(already) is already


@pytest.mark.parametrize("digits, expect_ms", [(10, False), (13, True)])
def test_threshold_boundary_matches_real_epoch_digit_counts(digits, expect_ms):
    """Sanity check that the 1e12 cutoff actually separates real seconds-epoch
    values from real milliseconds-epoch values, not just the two fixtures
    above — any date from here to several centuries out stays on the correct
    side of it."""
    value = 10 ** (digits - 1)
    result = _parse_ts(value)
    reconstructed_ms = int(result.timestamp() * 1000)
    if expect_ms:
        assert reconstructed_ms == value
    else:
        assert reconstructed_ms == value * 1000
