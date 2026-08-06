"""Auth builders, registered by name rather than dispatched by an if-chain.

The chain this replaces ended in `raise`, which made every new scheme a diff to
the middle of the runtime — the third connector to arrive with an unseen auth
type edited the same twenty lines the second one had. A registry keeps that
growth at the edges: a genuinely generic scheme is one decorated function plus
its tests, and a per-source oddity (request signing, a rotating HMAC nonce)
declares `type: extension` and lives in the connector's own extension.py, where
it belongs.

The token is never in the spec — only the NAME of the variable holding it. That
is what lets a spec live in a public repo.
"""

from __future__ import annotations

import os

_BUILDERS = {}


def auth_type(name):
    """Register a builder for `api.auth.type: <name>`.

    A builder takes (spec, auth_block, credential) and returns whatever dlt's
    rest_api client accepts: a config dict for the simple schemes, or an auth
    object for anything that has to outlive a single request.
    """
    def register(builder):
        _BUILDERS[name] = builder
        return builder
    return register


def registered():
    return tuple(sorted(_BUILDERS))


def credential_for(spec):
    """The secret itself, read from the environment the spec names."""
    auth = spec.api["auth"]
    token = os.environ.get(auth["token_env"], "").strip()
    if not token:
        raise RuntimeError(
            f"{spec.name}: {auth['token_env']} is not set. "
            f"Add it to .env (never to the spec)."
        )
    return token


def build(spec, extension=None):
    """Auth for `spec`, from the registry or from its extension.

    `type: extension` is the escape hatch's escape hatch: a scheme no generic
    builder covers hands construction to the connector, which returns a callable
    request signer. Everything else resolves here.
    """
    auth = spec.api["auth"]
    kind = auth["type"]

    if kind == "extension":
        builder = getattr(extension, "build_auth", None) if extension else None
        if builder is None:
            raise RuntimeError(
                f"{spec.name}: auth type 'extension' needs build_auth(spec) in "
                f"{spec.extension_path.name}, which it does not define."
            )
        return builder(spec)

    builder = _BUILDERS.get(kind)
    if builder is None:
        raise RuntimeError(
            f"{spec.name}: auth type {kind!r} is not registered. "
            f"Known: {', '.join(registered())}. A scheme genuinely shared between "
            f"APIs belongs here; one API's peculiarity belongs in its extension, "
            f"declared as `type: extension`."
        )
    return builder(spec, auth, credential_for(spec))


@auth_type("bearer")
def _bearer(spec, auth, credential):
    return {"type": "bearer", "token": credential}


@auth_type("api_key")
def _api_key(spec, auth, credential):
    return {
        "type": "api_key",
        "api_key": credential,
        "name": auth.get("header", "Authorization"),
        "location": "header",
    }


@auth_type("http_basic")
def _http_basic(spec, auth, credential):
    return {
        "type": "http_basic",
        "username": auth.get("username", credential),
        "password": auth.get("password", credential),
    }


# Per-source token sources, alive for the length of the process. An Airflow task
# is a fresh process per run, so this is a run-scoped cache in practice.
_TOKEN_SOURCES = {}


def reset_token_cache():
    """Drop minted OAuth token sources. For tests, and for a long-lived REPL."""
    _TOKEN_SOURCES.clear()


@auth_type("oauth2_client_credentials")
def _client_credentials(spec, auth, credential):
    """OAuth2 client-credentials, for APIs whose token expires mid-run.

    The other three types are static: the env var holds the value that goes on
    every request. Here it holds the *credential used to mint* short-lived
    bearer tokens, so the auth object has to outlive any single request and
    re-mint on expiry. dlt's OAuth2ClientCredentials already does that; the only
    thing it does not do is put the credential where a given API wants it.

    Two placements, because both are common:

      body          client_id/client_secret as form fields — dlt's default, and
                    what most providers document.
      basic_header  the pair pre-joined and Base64'd into `Authorization: Basic`.
                    `token_env` then holds that finished string rather than
                    either half, which is deliberate: the encoding is
                    `base64(urlencode(id):urlencode(secret))`, and hand-rolling
                    it silently produces a non-working credential whenever the
                    secret contains a reserved character. Providers that want
                    this generally show the encoded value in their UI, so taking
                    it verbatim removes the step that can be got wrong.

    One object serves the whole run, cached per source. Both the declarative
    path and every extension builder ask for auth independently, so without the
    cache a connector with twelve delegated resources mints thirteen tokens per
    run — each with its own expiry clock, each spending from the same budget the
    pacer is trying to protect.
    """
    from dlt.common.configuration import configspec
    from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials

    cached = _TOKEN_SOURCES.get(spec.name)
    if cached is not None:
        return cached

    token_url = auth.get("token_url")
    if not token_url:
        raise RuntimeError(
            f"{spec.name}: auth type oauth2_client_credentials needs `token_url` "
            f"— the endpoint that exchanges the credential for a bearer token."
        )
    placement = auth.get("credentials_in", "body")
    extra = dict(auth.get("token_request_data") or {})

    if placement == "body":
        return _TOKEN_SOURCES.setdefault(spec.name, OAuth2ClientCredentials(
            access_token_url=token_url,
            client_id=auth.get("client_id", ""),
            client_secret=credential,
            access_token_request_data=extra,
        ))
    if placement != "basic_header":
        raise RuntimeError(
            f"{spec.name}: credentials_in must be 'body' or 'basic_header', "
            f"got {placement!r}."
        )

    # Content-Type carries the charset because some token endpoints (Swoogo's
    # among them) reject the bare form media type.
    #
    # @configspec is not decoration: dlt's rest_api runs every auth object
    # through resolve_configuration, which rejects anything that is not one.
    @configspec
    class _BasicHeaderCredentials(OAuth2ClientCredentials):
        def build_access_token_request(self):
            return {
                "headers": {
                    "Authorization": f"Basic {credential}",
                    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                },
                "data": {"grant_type": "client_credentials", **extra},
            }

    # client_id/client_secret are inherited required fields, and dlt resolves
    # them before it ever calls the override that ignores them — leave them
    # unset and resolution goes looking in secrets.toml and fails. The
    # credential is the encoded pair, so it belongs in client_secret; there is
    # no separate id to give.
    return _TOKEN_SOURCES.setdefault(spec.name, _BasicHeaderCredentials(
        access_token_url=token_url,
        client_id="unused-in-basic-header-mode",
        client_secret=credential,
    ))
