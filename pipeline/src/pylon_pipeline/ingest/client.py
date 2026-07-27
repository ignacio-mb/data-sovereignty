"""Pylon REST client on top of dlt's RESTClient.

Pylon envelope: {"data": [...], "pagination": {"cursor": "...", "has_next_page": bool}}.
Auth is a bearer token. 429s (with Retry-After) and 5xx are retried by dlt's
requests session (configured via runtime settings); the EndpointPacer keeps us
inside the published per-endpoint budgets so 429s stay rare.
"""

import logging
import time

from dlt.sources.helpers.requests import Client
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
from requests import HTTPError

from .pacing import EndpointPacer
from .settings import API_URL, RATE_LIMITS

log = logging.getLogger(__name__)

# HTTP statuses on issues/{id}/messages that mean "issue gone/scrubbed" — skip, not fail.
SKIPPABLE_MESSAGE_STATUSES = (400, 404, 410)

# A page that reports has_next_page=true but carries no data list is a known
# Pylon glitch; retry the same cursor with capped backoff before giving up.
GLITCH_MAX_RETRIES = 10
GLITCH_BACKOFF_MAX_SECONDS = 5


class PylonPaginator(JSONResponseCursorPaginator):
    """Stops on pagination.has_next_page; tolerates the key being absent, and
    retries the glitched "has_next_page but no data" page on the same cursor.

    The stock paginator raises when has_more_path is missing from a response,
    but Pylon omits the pagination object entirely on some responses (e.g. a
    messages fetch without limit/cursor), which simply means "no more pages".
    """

    def __init__(self, *args, sleeper=time.sleep, **kwargs):
        super().__init__(*args, **kwargs)
        self._sleep = sleeper
        self._glitch_retries = 0

    def _handle_missing_has_more(self, response_json):
        self._has_next_page = False

    def update_state(self, response, data=None):
        body = response.json()
        has_next = (body.get("pagination") or {}).get("has_next_page")
        if has_next and not isinstance(body.get("data"), list):
            # Same-cursor retry: leave _next_reference untouched so dlt re-requests
            # this page. (The stored cursor is whatever produced this response.)
            self._glitch_retries += 1
            if self._glitch_retries > GLITCH_MAX_RETRIES:
                raise RuntimeError(
                    f"Pylon returned {self._glitch_retries} consecutive glitched pages "
                    f"(has_next_page but no data) — endpoint appears broken")
            backoff = min(2 ** (self._glitch_retries - 1), GLITCH_BACKOFF_MAX_SECONDS)
            log.warning("[paginate] glitched page (has_next_page, no data); retrying same cursor in %ss", backoff)
            self._sleep(backoff)
            self._has_next_page = True
            return
        self._glitch_retries = 0
        super().update_state(response, data)


def _get_paginator():
    return PylonPaginator(
        cursor_path="pagination.cursor",
        cursor_param="cursor",
        has_more_path="pagination.has_next_page",
    )


def _post_paginator():
    return PylonPaginator(
        cursor_path="pagination.cursor",
        cursor_body_path="cursor",
        has_more_path="pagination.has_next_page",
    )


class PylonClient:
    def __init__(self, api_key, pacer=None, session=None):
        self.pacer = pacer or EndpointPacer(RATE_LIMITS)
        self._auth = BearerTokenAuth(api_key)
        self._client = RESTClient(
            base_url=API_URL,
            auth=self._auth,
            headers={"Cache-Control": "no-cache"},
            data_selector="data",
            session=session or Client(raise_for_status=False).session,
        )

    def _page_hook(self, family, label, level=logging.INFO):
        """Response hook: fail loudly on error statuses (the session has already
        retried 429/5xx), log each page, and pace before the next one is requested."""
        state = {"page": 0, "records": 0}

        def hook(response, *args, **kwargs):
            response.raise_for_status()
            state["page"] += 1
            try:
                body = response.json()
            except ValueError:
                return
            data = body.get("data") or []
            state["records"] += len(data)
            log.log(level, "[%s] page %d · +%d records (%d total)", label, state["page"], len(data), state["records"])
            if (body.get("pagination") or {}).get("has_next_page"):
                self.pacer.wait(family)

        return hook

    def paginate_get(self, path, params, family, label=None):
        """Yield pages (lists of records) from a cursor-paginated GET endpoint."""
        self.pacer.wait(family)
        yield from self._client.paginate(
            path=path,
            params=params,
            paginator=_get_paginator(),
            hooks={"response": [self._page_hook(family, label or path)]},
        )

    def paginate_search(self, body, label="issues/search"):
        """Yield pages from POST /issues/search; the cursor travels in the JSON body."""
        self.pacer.wait("issues_search")
        yield from self._client.paginate(
            path="issues/search",
            method="POST",
            json=body,
            paginator=_post_paginator(),
            hooks={"response": [self._page_hook("issues_search", label)]},
        )

    def get_issue_messages(self, issue_id):
        """All messages for one issue, or None if the issue is gone/scrubbed.

        Without limit/cursor params Pylon returns every message in a single
        response; the paginator is still attached in case that ever changes.
        """
        self.pacer.wait("messages")
        messages = []
        try:
            for page in self._client.paginate(
                path=f"issues/{issue_id}/messages",
                paginator=_get_paginator(),
                hooks={"response": [self._page_hook("messages", f"messages:{issue_id}", logging.DEBUG)]},
            ):
                messages.extend(page)
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in SKIPPABLE_MESSAGE_STATUSES:
                log.info("[messages:%s] skipped (HTTP %s: issue gone or scrubbed)", issue_id, status)
                return None
            raise
        return messages
