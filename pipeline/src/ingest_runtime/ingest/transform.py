"""Record shaping: flatten nested JSON, promote hot scalars, parse timestamps.

House rule (from data-tools): keep the raw tables FLAT. Nested dicts/lists are
JSON-stringified into a single column instead of letting dlt unpack them into
per-key columns or child tables — Pylon's custom_fields is a slug-keyed map, so
unpacking would mint a new column every time support adds a custom field.
Analytically hot nested scalars are promoted to real columns first.
"""

import json

import pendulum
from bs4 import BeautifulSoup

# Issue keys promoted from nested objects to top-level scalar columns.
_ISSUE_PROMOTIONS = {
    "account": (("id", "account_id"),),
    "assignee": (("id", "assignee_id"), ("email", "assignee_email")),
    "requester": (("id", "requester_id"), ("email", "requester_email")),
    "team": (("id", "team_id"),),
}

_ISSUE_TIMESTAMP_KEYS = ("created_at", "updated_at", "latest_message_time", "resolution_time", "snoozed_until_time")
_MESSAGE_TIMESTAMP_KEYS = ("timestamp",)
_DIRECTORY_TIMESTAMP_KEYS = ("created_at", "updated_at", "latest_customer_message_time")


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


def flatten_issue(record):
    rec = dict(record)
    for source_key, promotions in _ISSUE_PROMOTIONS.items():
        nested = rec.get(source_key) or {}
        for nested_key, target in promotions:
            rec[target] = nested.get(nested_key)
    rec.setdefault("_deleted", False)
    return flatten_record(rec, _ISSUE_TIMESTAMP_KEYS)


def flatten_directory_record(record):
    rec = dict(record)
    rec.setdefault("_deleted", False)
    return flatten_record(rec, _DIRECTORY_TIMESTAMP_KEYS)


def strip_html(html):
    """Plain text from message HTML. No escaping — dlt owns serialization.

    (The legacy pipeline hand-escaped quotes/newlines here, which stored
    literal backslashes in the warehouse. Deliberately not ported.)
    """
    if not html:
        return None
    return BeautifulSoup(html, "html.parser").get_text()


def enrich_message(message, issue_id):
    rec = dict(message)
    rec["issue_id"] = issue_id
    rec["message_text"] = strip_html(rec.get("message_html"))
    return flatten_record(rec, _MESSAGE_TIMESTAMP_KEYS)
