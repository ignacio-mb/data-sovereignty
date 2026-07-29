"""Explicit dlt column hints where inference must not drift.

Timestamps get a deterministic type/precision because the incremental cursor
(issues.updated_at) and the messages watermark (issue_messages.timestamp vs
issues.latest_message_time) compare these columns; _deleted must be bool for
the soft-delete predicate.
"""

_TS = {"data_type": "timestamp", "precision": 6}
_BOOL = {"data_type": "bool"}

ISSUE_HINTS = {
    "created_at": _TS,
    "updated_at": _TS,
    "latest_message_time": _TS,
    "_deleted": _BOOL,
}

MESSAGE_HINTS = {
    "timestamp": _TS,
    "issue_id": {"data_type": "text"},
}

DIRECTORY_HINTS = {
    "_deleted": _BOOL,
}
