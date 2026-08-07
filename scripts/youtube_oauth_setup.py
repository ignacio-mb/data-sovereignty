#!/usr/bin/env python3
"""Mint the YouTube refresh token, once, and write it into .env.

Run this on a laptop with a browser. It is the one step in this repo a human
has to do interactively, and it cannot be automated away: every YouTube
Reporting API method needs OAuth 2.0 user-delegated consent, and Google closes
both alternatives — a service account cannot be linked to a YouTube channel
("attempts to authorize requests with this flow will generate a
NoLinkedYouTubeAccount error") and the device flow is unsupported for these
APIs. So a person who owns the channel has to click Allow, exactly once.

    uv run python scripts/youtube_oauth_setup.py

Before running, put the OAuth client in .env — Google Cloud console ->
APIs & Services -> Credentials -> Create credentials -> OAuth client ID ->
**Desktop app**:

    YOUTUBE_OAUTH_CLIENT_ID=...
    YOUTUBE_OAUTH_CLIENT_SECRET=...

The refresh token is written straight back into .env and is never printed.
That is not decoration: CLAUDE.md's rule is that a secret is never echoed and
never passed through a command that gets logged, and a token pasted from a
terminal into a file goes through both. Nothing here writes it to stdout, to a
log, or to an argv.

Two things about the credential that will otherwise bite months later:

  publish the consent screen   While the OAuth consent screen's publishing
      status is "Testing", Google issues a refresh token that "expires in
      7 days" for any scope outside name/email/profile. The connector then
      fails every hour until someone re-runs this script. Set the app to
      "In production" before relying on it.

  do not re-run this casually  Google keeps "a limit of 100 refresh tokens per
      Google Account per OAuth 2.0 client ID. If the limit is reached, creating
      a new refresh token automatically invalidates the oldest refresh token
      without warning." Minting one per debugging attempt eventually revokes
      the one production is using.

Desktop clients must use the loopback redirect. The copy/paste "OOB" flow
(urn:ietf:wg:oauth:2.0:oob) was blocked for all clients on 2023-01-31, so this
starts a one-request HTTP server on 127.0.0.1 and closes it again.
"""

from __future__ import annotations

import argparse
import http.server
import secrets
import socket
import sys
import threading
import typing
import urllib.parse
import webbrowser
from pathlib import Path

import requests
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC = REPO_ROOT / "sources" / "youtube.yml"
AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"


def read_spec():
    """The scopes, token URL and variable names, from the spec itself.

    Read rather than restated so this script cannot drift from the connector it
    credentials: a scope added to the spec but not here would produce a token
    that authenticates and then 403s on one resource.
    """
    document = yaml.safe_load(SPEC.read_text())
    auth = document["api"]["auth"]
    return {
        "scopes": list(auth["scopes"]),
        "token_url": auth["token_url"],
        "client_id_env": auth["client_id_env"],
        "client_secret_env": auth["client_secret_env"],
        "token_env": auth["token_env"],
    }


def env_value(env_file, key):
    """One value from .env, without sourcing the file or echoing anything."""
    if not env_file.is_file():
        return None
    for line in env_file.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{key}="):
            return stripped[len(key) + 1:].strip()
    return None


def write_env_value(env_file, key, value):
    """Set `key` in .env in place, replacing a live OR commented-out line.

    The commented case is the one that matters: .env.example ships these three
    keys commented out, `make env` copies it verbatim, and appending a second
    live line below a commented one works — but appending below an EXISTING live
    line would leave two, and which one wins depends on who reads the file.
    """
    lines = env_file.read_text().splitlines() if env_file.is_file() else []
    replacement = f"{key}={value}"
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith((f"{key}=", f"# {key}=")):
            lines[index] = replacement
            break
    else:
        lines.append(replacement)
    env_file.write_text("\n".join(lines) + "\n")


class _Capture(http.server.BaseHTTPRequestHandler):
    """Catches the single redirect Google sends back, then nothing else."""

    result: typing.ClassVar[dict] = {}

    def do_GET(self):  # BaseHTTPRequestHandler's own spelling, not ours
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        _Capture.result = {k: v[0] for k, v in query.items()}
        ok = "code" in _Capture.result
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        body = (
            "<h2>Consent captured.</h2><p>Close this tab and go back to the terminal.</p>"
            if ok else
            f"<h2>No authorization code came back.</h2><pre>{_Capture.result}</pre>"
        )
        self.wfile.write(body.encode())

    def log_message(self, *_args):
        # Silence. The default handler logs the request line, which carries the
        # authorization code as a query parameter.
        return


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def consent(client_id, scopes, port):
    """Open the browser, wait for the redirect, return the authorization code."""
    redirect_uri = f"http://127.0.0.1:{port}"
    state = secrets.token_urlsafe(16)
    url = AUTH_ENDPOINT + "?" + urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        # Without access_type=offline the response carries no refresh token at
        # all — Google: the refresh_token field "is only present in this
        # response if you set the access_type parameter to offline".
        "access_type": "offline",
        # And without prompt=consent an account that has already consented gets
        # an access token and NO refresh token, because "the refresh_token is
        # only returned on the first authorization". Re-running this script for
        # a lost token needs this.
        "prompt": "consent",
        "state": state,
    })

    server = http.server.HTTPServer(("127.0.0.1", port), _Capture)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    print(f"Opening a browser to consent as the channel owner (listening on {redirect_uri}).")
    print("If nothing opens, paste this into a browser:\n")
    print(f"  {url}\n")
    webbrowser.open(url)
    # No timeout: the human may need to pick between Google accounts, and
    # failing out from under them would waste a refresh token from the 100.
    thread.join()
    server.server_close()

    result = _Capture.result
    if result.get("state") != state:
        raise SystemExit("error: the redirect's `state` did not match. Aborting rather than "
                         "exchanging a code that may not be ours.")
    if "code" not in result:
        raise SystemExit(f"error: no authorization code came back ({result.get('error', result)}).")
    return result["code"], redirect_uri


def exchange(token_url, client_id, client_secret, code, redirect_uri):
    response = requests.post(token_url, timeout=30, data={
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    })
    if not response.ok:
        # The body can contain the code but never the secret; still, only the
        # error field is surfaced rather than the whole payload.
        raise SystemExit(
            f"error: Google refused the code exchange ({response.status_code}: "
            f"{response.json().get('error', 'unknown')}). Check the client is a "
            f"**Desktop app** OAuth client and that the secret in .env matches it."
        )
    payload = response.json()
    if "refresh_token" not in payload:
        raise SystemExit(
            "error: Google returned an access token but NO refresh token. That happens "
            "when the account has consented before — this script already sends "
            "prompt=consent to force a new one, so if you see this, revoke the app at "
            "https://myaccount.google.com/permissions and run it again."
        )
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    args = parser.parse_args()

    config = read_spec()
    env_file = args.env_file
    if not env_file.is_file():
        raise SystemExit(f"error: {env_file} does not exist. Run `make env` first.")

    client_id = env_value(env_file, config["client_id_env"])
    client_secret = env_value(env_file, config["client_secret_env"])
    missing = [name for name, value in
               ((config["client_id_env"], client_id), (config["client_secret_env"], client_secret))
               if not value]
    if missing:
        raise SystemExit(
            f"error: {', '.join(missing)} is not set in {env_file}.\n"
            f"Create a **Desktop app** OAuth client in the Google Cloud console "
            f"(APIs & Services -> Credentials) and put both values there first."
        )

    print("Scopes being requested:")
    for scope in config["scopes"]:
        print(f"  {scope}")
    print()

    code, redirect_uri = consent(client_id, config["scopes"], free_port())
    payload = exchange(config["token_url"], client_id, client_secret, code, redirect_uri)
    write_env_value(env_file, config["token_env"], payload["refresh_token"])

    granted = payload.get("scope", "").split()
    print(f"\nWrote {config['token_env']} to {env_file}. The value was not printed.")
    print(f"Granted {len(granted)} scope(s); access token valid {payload.get('expires_in')}s.")
    for scope in config["scopes"]:
        if scope not in granted:
            print(f"  WARNING: {scope} was NOT granted — the resources needing it will 403.")
    print(
        "\nNext:\n"
        "  1. Set the OAuth consent screen to 'In production'. While it is 'Testing',\n"
        "     Google expires this refresh token every 7 days.\n"
        "  2. make secrets-push        (so the instance gets all three variables)\n"
        "  3. make up                  (creates the youtube_pipeline Airflow pool, and\n"
        "                               recreates containers so they see the new .env)\n"
        "  4. make ingest SOURCE=youtube\n"
        "\nThe first run creates 20 reporting jobs. Their reports do not exist for ~48\n"
        "hours, and history begins 30 days before today — permanently. Until then the\n"
        "report tables are legitimately empty; the metadata tables load immediately."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
