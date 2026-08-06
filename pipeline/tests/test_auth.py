"""The auth registry: one entry per genuinely shared scheme, and an escape hatch.

The if-chain this replaced ended in `raise`, so the third connector to arrive
with an unseen scheme edited the same twenty lines the second one had. What
matters now is that the edge stays an edge: a registered type is a decorated
function, a per-source oddity declares `type: extension` and lives in the
connector's own file, and neither can be reached by accident.

The OAuth2 case gets the most attention because it is the one that is not
static. The env var holds the credential used to MINT short-lived tokens rather
than a token, the object has to outlive a request, and where the credential
goes differs per provider — the kind of thing that produces a
non-working credential rather than an error.
"""

from __future__ import annotations

import pytest

from ingest_runtime import auth, spec

SPEC = """
name: probe
status: reference
api:
  base_url: https://probe.test
  auth: {{type: {kind}, token_env: PROBE_TOKEN{extra}}}
resources:
  - {{name: widgets, primary_key: id}}
"""


def write(tmp_path, kind, extra=""):
    (tmp_path / "probe").mkdir(exist_ok=True)
    (tmp_path / "probe" / "source.yml").write_text(SPEC.format(kind=kind, extra=extra))
    return spec.load("probe", directory=tmp_path)


class TestTheRegistry:
    def test_the_shared_schemes_are_registered(self):
        assert set(auth.registered()) >= {
            "bearer", "api_key", "http_basic", "oauth2_client_credentials"}

    def test_an_unregistered_type_says_where_a_new_one_belongs(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROBE_TOKEN", "secret")
        # Built by hand: the JSON Schema enumerates the types, so a spec naming
        # an unknown one cannot be loaded to reach the registry at all.
        probe = write(tmp_path, "bearer")
        probe._doc["api"]["auth"]["type"] = "hmac_nonce"
        with pytest.raises(RuntimeError, match="not registered"):
            auth.build(probe)

    def test_a_missing_credential_names_the_variable(self, tmp_path, monkeypatch):
        """The name, never the value. That is what lets a spec live in a public
        repo, and what makes "is it set" the only question worth asking."""
        monkeypatch.delenv("PROBE_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="PROBE_TOKEN"):
            auth.build(write(tmp_path, "bearer"))

    def test_a_blank_credential_counts_as_missing(self, tmp_path, monkeypatch):
        """An empty variable is the shape a half-finished .env has, and it
        would otherwise sign every request with nothing and get 401s."""
        monkeypatch.setenv("PROBE_TOKEN", "   ")
        with pytest.raises(RuntimeError, match="PROBE_TOKEN"):
            auth.build(write(tmp_path, "bearer"))


class TestTheSimpleSchemes:
    @pytest.fixture(autouse=True)
    def credential(self, monkeypatch):
        monkeypatch.setenv("PROBE_TOKEN", "secret")

    def test_bearer_hands_dlt_the_token(self, tmp_path):
        assert auth.build(write(tmp_path, "bearer")) == {"type": "bearer", "token": "secret"}

    def test_api_key_defaults_to_the_authorization_header(self, tmp_path):
        built = auth.build(write(tmp_path, "api_key"))
        assert built == {"type": "api_key", "api_key": "secret",
                         "name": "Authorization", "location": "header"}

    def test_api_key_can_name_its_own_header(self, tmp_path):
        built = auth.build(write(tmp_path, "api_key", extra=", header: X-Api-Key"))
        assert built["name"] == "X-Api-Key"

    def test_http_basic_uses_the_credential_for_both_halves_by_default(self, tmp_path):
        built = auth.build(write(tmp_path, "http_basic"))
        assert built == {"type": "http_basic", "username": "secret", "password": "secret"}


class TestOAuth2ClientCredentials:
    """The one scheme whose token expires mid-run."""

    BODY = ", token_url: 'https://probe.test/oauth2/token'"
    HEADER = BODY + ", credentials_in: basic_header"

    @pytest.fixture(autouse=True)
    def credential(self, monkeypatch):
        monkeypatch.setenv("PROBE_TOKEN", "ZW5jb2RlZDpjcmVkcw==")

    def test_it_needs_the_endpoint_that_mints_the_token(self, tmp_path):
        with pytest.raises(RuntimeError, match="token_url"):
            auth.build(write(tmp_path, "oauth2_client_credentials"))

    def test_the_body_placement_is_dlt_s_own_object(self, tmp_path):
        built = auth.build(write(tmp_path, "oauth2_client_credentials", extra=self.BODY))
        assert built.access_token_url == "https://probe.test/oauth2/token"
        assert str(built.client_secret) == "ZW5jb2RlZDpjcmVkcw=="

    def test_basic_header_sends_the_encoded_pair_verbatim(self, tmp_path):
        """`token_env` holds the finished Base64 string, not either half.

        The encoding is base64(urlencode(id):urlencode(secret)), and
        hand-rolling it silently produces a non-working credential whenever the
        secret contains a reserved character. Providers that want this show the
        encoded value in their UI, so taking it whole removes the step that can
        be got wrong.
        """
        built = auth.build(write(tmp_path, "oauth2_client_credentials", extra=self.HEADER))
        request = built.build_access_token_request()
        assert request["headers"]["Authorization"] == "Basic ZW5jb2RlZDpjcmVkcw=="
        # Some token endpoints reject the bare form media type.
        assert "charset=UTF-8" in request["headers"]["Content-Type"]
        assert request["data"]["grant_type"] == "client_credentials"

    def test_an_unknown_placement_is_refused_rather_than_guessed(self, tmp_path):
        with pytest.raises(RuntimeError, match="credentials_in"):
            auth.build(write(tmp_path, "oauth2_client_credentials",
                             extra=self.BODY + ", credentials_in: query"))

    def test_one_token_source_serves_the_whole_run(self, tmp_path):
        """Both the declarative path and every extension builder ask for auth
        independently. Without the cache a connector with twelve delegated
        resources mints thirteen tokens a run — each with its own expiry clock,
        all spending from the budget the pacer is trying to protect."""
        probe = write(tmp_path, "oauth2_client_credentials", extra=self.HEADER)
        assert auth.build(probe) is auth.build(probe)

    def test_the_cache_does_not_outlive_a_reset(self, tmp_path):
        probe = write(tmp_path, "oauth2_client_credentials", extra=self.HEADER)
        first = auth.build(probe)
        auth.reset_token_cache()
        assert auth.build(probe) is not first


class TestTheExtensionEscapeHatch:
    """A scheme no generic builder covers — request signing, a rotating HMAC."""

    def test_it_delegates_to_the_connector_s_own_build_auth(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROBE_TOKEN", "secret")
        probe = write(tmp_path, "extension")
        signer = object()
        extension = type("Extension", (), {"build_auth": staticmethod(lambda s: signer)})
        assert auth.build(probe, extension=extension) is signer

    def test_a_connector_that_did_not_write_it_is_told_so(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PROBE_TOKEN", "secret")
        probe = write(tmp_path, "extension")
        with pytest.raises(RuntimeError, match="build_auth"):
            auth.build(probe, extension=object())
