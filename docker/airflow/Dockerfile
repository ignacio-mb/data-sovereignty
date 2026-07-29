# Pinned by digest because this image is built on the production host as root: a
# republished tag would execute there during a routine deploy. The multi-arch index
# digest still resolves per-platform. A tag bump means re-resolving the digest with
# `docker buildx imagetools inspect <ref> --format '{{json .Manifest}}' | jq -r .digest`.
ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.0-python3.12@sha256:96e99f25815f533b298a4d53f283adf5c84c27334ea16ef232777cb800bddf10
FROM ${AIRFLOW_BASE_IMAGE}

# Which mb-cli to install. Defaults to the pinned published package so a clean
# checkout builds reproducibly. `make mb-cli-local` packs your working copy into
# docker/airflow/vendor/ instead, which takes precedence — that is the path to
# use when you are developing mb-cli itself.
ARG MB_CLI_SPEC=@metabase/cli@0.2.2

# The stack's three Python tools (pylon, dq, mbx) live in their own virtualenv at
# /opt/data-venv, deliberately NOT in Airflow's. DAGs shell out to them rather
# than importing them, so dlt and Great Expectations can never drag Airflow's
# own dependency tree into a resolution conflict.

USER root

# jq: the Metabase bootstrap and several skills parse `mb --json` output.
# git:  Metabase remote-sync and dlt schema export both want it present.
# Node: the Metabase CLI (`mb`) is an npm package.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends jq git curl ca-certificates gnupg; \
    mkdir -p /etc/apt/keyrings; \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
      | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg; \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \
      > /etc/apt/sources.list.d/nodesource.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends nodejs; \
    apt-get purge -y --auto-remove gnupg; \
    rm -rf /var/lib/apt/lists/*

# A locally packed mb-cli wins over the published spec. The directory is always
# present (it holds only a .gitkeep by default) so the COPY never fails.
COPY docker/airflow/vendor/ /tmp/mb-cli-vendor/
RUN set -eux; \
    if ls /tmp/mb-cli-vendor/*.tgz >/dev/null 2>&1; then \
      tarball="$(ls -t /tmp/mb-cli-vendor/*.tgz | head -1)"; \
      echo "installing mb-cli from local pack: ${tarball}"; \
      npm install -g "${tarball}"; \
    else \
      echo "installing mb-cli from registry: ${MB_CLI_SPEC}"; \
      npm install -g "${MB_CLI_SPEC}"; \
    fi; \
    rm -rf /tmp/mb-cli-vendor; \
    mb --version

# uv builds the workspace venv. Digest-pinned for the same reason as the base
# image; a tag bump means re-resolving it.
COPY --from=ghcr.io/astral-sh/uv:0.8.17@sha256:e4644cb5bd56fdc2c5ea3ee0525d9d21eed1603bccd6a21f887a938be7e85be1 /uv /usr/local/bin/uv

WORKDIR /opt/project

# Manifests first so a code-only change reuses the resolved dependency layer.
COPY pyproject.toml uv.lock ./
COPY pipeline/pyproject.toml ./pipeline/
COPY quality/pyproject.toml ./quality/
COPY metabase/pyproject.toml ./metabase/

# Source needed for the editable installs to build. Compose bind-mounts these
# same paths at runtime, so edits to SQL, suites and DAGs land without a rebuild
# — but a DEPENDENCY change still requires `make build`.
COPY pipeline/src ./pipeline/src
COPY quality/src ./quality/src
COPY metabase/src ./metabase/src

ENV UV_PROJECT_ENVIRONMENT=/opt/data-venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
# gcc and the Python headers are build-time only. clickhouse-sqlalchemy depends
# on asynch, which depends on zstd, which publishes no linux/arm64 wheel and so
# compiles from source. They are purged in the same layer to keep the runtime
# image free of a toolchain.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends gcc python3-dev libc6-dev; \
    uv sync --frozen --no-dev; \
    apt-get purge -y --auto-remove gcc python3-dev libc6-dev; \
    rm -rf /var/lib/apt/lists/*; \
    for tool in ingest dq mbx; do ln -sf /opt/data-venv/bin/$tool /usr/local/bin/$tool; done; \
    chown -R airflow:0 /opt/data-venv /opt/project

USER airflow

# Fail fast at build time rather than in a DAG task at 03:00.
RUN ingest --help >/dev/null && dq --help >/dev/null && mbx --help >/dev/null && mb --version >/dev/null
