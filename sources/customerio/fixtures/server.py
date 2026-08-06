"""Customer.io's envelope, which is not one envelope.

The spec declares no `data_selector` on purpose, because there is no single
answer: the list arrives under `campaigns`, under `types`, and — for
/v1/transactional — under `messages`, which is also /v1/messages' key. dlt
detects the list per response, and this server is what keeps that detection
honest. A generic mock wrapping every payload under the resource name would
agree with itself and prove nothing.

Paging is Customer.io's, verified against the live API and not what the docs
imply: the last page that carries rows STILL hands back a `next`, and the
sequence terminates with an EMPTY page. An endpoint that is not paginated at
all omits `next` entirely, and a paginator treating a missing cursor as "keep
going" would loop on it forever.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

PAGE_SIZE = 2

# Envelope key per path, where it is not the resource name. Everything about
# this mapping is Customer.io's, which is why it lives here and not in a spec.
ENVELOPE = {
    "/v1/messages": "messages",
    "/v1/transactional": "messages",
    "/v1/object_types": "types",
}

# Endpoints that hand back no cursor at all.
UNPAGINATED = ("/v1/campaigns", "/v1/transactional", "/v1/object_types")


def _query(request):
    return parse_qs(urlparse(request.url).query)


def register(mock, spec, fixtures):
    base = spec.base_url.rstrip("/")

    for resource in spec.resources:
        rows = fixtures.get(resource.name)
        if rows is None:
            continue
        path = resource.endpoint.get("path", f"/{resource.name}")
        key = ENVELOPE.get(path, resource.name)
        mock.get(f"{base}{path}", json=_responder(rows, key, path in UNPAGINATED))


def _responder(rows, key, unpaginated):
    def respond(request, context):
        if unpaginated:
            return {key: rows}
        start = int(_query(request).get("start", ["0"])[0])
        window = rows[start:start + PAGE_SIZE]
        # `next` even on the last page that carries rows: only the empty page
        # that follows it stops the paginator.
        return {key: window, "next": str(start + PAGE_SIZE) if window else ""}

    return respond
