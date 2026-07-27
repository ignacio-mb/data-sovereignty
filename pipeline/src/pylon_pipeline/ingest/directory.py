"""Directory resources: accounts, users, teams, contacts.

Small entity sets, fully fetched every run and merged on id (merge, not
replace, so `_deleted` history survives — the soft-delete pass relies on rows
staying in the table after they disappear from the API).
"""

import logging

import dlt

from .hints import DIRECTORY_HINTS
from .settings import DIRECTORY_PAGE_LIMIT
from .transform import flatten_directory_record

log = logging.getLogger(__name__)


def make_directory_resource(client, name):
    @dlt.resource(name=name, write_disposition="merge", primary_key="id", columns=DIRECTORY_HINTS)
    def directory():
        total = 0
        for page in client.paginate_get(name, {"limit": DIRECTORY_PAGE_LIMIT}, family="directory", label=name):
            for record in page:
                yield flatten_directory_record(record)
            total += len(page)
        log.info("[%s] fetched %d records", name, total)

    return directory
