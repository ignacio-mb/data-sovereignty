"""Record shaping: flatten nested JSON, promote hot scalars, parse timestamps.

House rule (from data-tools): keep the raw tables FLAT. Nested dicts/lists are
JSON-stringified into a single column instead of letting dlt unpack them into
per-key columns or child tables — Pylon's custom_fields is a slug-keyed map, so
unpacking would mint a new column every time support adds a custom field.
Analytically hot nested scalars are promoted to real columns first, by name, from
the spec's `promote:` block.

Everything here is generic. It used to also hold `flatten_issue`,
`enrich_message` and three tuples of Pylon's column names — per-API knowledge
compiled into the runtime, in the module whose header says the opposite. Nothing
called them; they were the shape of the connector this stack replaced.
"""

import json

import pendulum
from bs4 import BeautifulSoup


def _parse_ts(value):
    """RFC3339 string -> tz-aware datetime, so dlt types the column as timestamp
    and cursor/watermark comparisons are on datetimes, not strings."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return pendulum.parse(value)
    return value


def flatten_record(record, timestamp_keys=()):
    """JSON-stringify nested values, parse the given timestamp keys."""
    flat = {}
    for key, value in record.items():
        if key in timestamp_keys:
            flat[key] = _parse_ts(value)
        elif isinstance(value, (dict, list)):
            flat[key] = json.dumps(value, default=str)
        else:
            flat[key] = value
    return flat


def strip_html(html):
    """Plain text from message HTML. No escaping — dlt owns serialization.

    (The legacy pipeline hand-escaped quotes/newlines here, which stored
    literal backslashes in the warehouse. Deliberately not ported.)
    """
    if not html:
        return None
    return BeautifulSoup(html, "html.parser").get_text()
