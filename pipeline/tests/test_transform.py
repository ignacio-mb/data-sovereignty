import json
from datetime import datetime

from ingest_runtime.ingest.transform import enrich_message, flatten_issue, strip_html
from tests.conftest import issue


def test_flatten_issue_promotes_hot_scalars_and_stringifies_nested():
    flat = flatten_issue(issue("iss_x", "2026-06-05T10:00:00Z", "2026-06-06T09:00:00Z",
                               "2026-06-06T08:59:00Z"))
    assert flat["account_id"] == "acc_for_iss_x"
    assert flat["assignee_id"] == "user_1"
    assert flat["assignee_email"] == "assignee@example.com"
    assert flat["requester_email"] == "requester@example.com"
    assert flat["team_id"] == "team_1"
    # nested structures become JSON strings, not child tables
    assert json.loads(flat["custom_fields"])["plan"]["value"] == "pro"
    assert json.loads(flat["tags"]) == ["bug", "billing"]
    assert isinstance(flat["account"], str)
    # timestamps become datetimes (stable dlt typing + comparable cursors)
    assert isinstance(flat["created_at"], datetime)
    assert isinstance(flat["updated_at"], datetime)
    assert flat["_deleted"] is False


def test_strip_html_produces_clean_text_without_escaping():
    text = strip_html('<p>He said "hi"<br>next \'line\'</p>')
    # the legacy pipeline stored literal backslashes here — regression guard
    assert "\\" not in text
    assert '"hi"' in text
    assert strip_html(None) is None
    assert strip_html("") is None


def test_enrich_message_adds_issue_id_and_text():
    flat = enrich_message(
        {"id": "m1", "timestamp": "2026-06-05T10:01:00Z",
         "message_html": "<p>Hello <b>world</b></p>", "author": {"id": "u1"}},
        issue_id="iss_1",
    )
    assert flat["issue_id"] == "iss_1"
    assert flat["message_text"] == "Hello world"
    assert isinstance(flat["timestamp"], datetime)
    assert json.loads(flat["author"]) == {"id": "u1"}
