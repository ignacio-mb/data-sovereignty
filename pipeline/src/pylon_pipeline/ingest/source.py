"""Assembles the pylon dlt source from the selected resources."""

import dlt

from .directory import make_directory_resource
from .issue_messages import issue_messages_resource
from .issues import issues_incremental_resource, issues_window_resource
from .settings import DIRECTORY_RESOURCES, INCREMENTAL_LOOKBACK_SECONDS

ALL_RESOURCES = ("issues", "issue_messages", *DIRECTORY_RESOURCES)


@dlt.source(name="pylon", max_table_nesting=0)
def pylon_source(
    client=None,
    selected=(),
    mode="incremental",
    start=None,
    end=None,
    pending_message_ids=None,
    budget_minutes=None,
    lookback_seconds=INCREMENTAL_LOOKBACK_SECONDS,
):
    resources = []
    if "issues" in selected:
        if mode == "window":
            resources.append(issues_window_resource(client, start, end))
        else:
            resources.append(issues_incremental_resource(client, lookback_seconds))
    for name in DIRECTORY_RESOURCES:
        if name in selected:
            resources.append(make_directory_resource(client, name))
    if "issue_messages" in selected:
        resources.append(issue_messages_resource(client, pending_message_ids, budget_minutes))
    return resources
