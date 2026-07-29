# Pinned by digest because this image is built on the production host as root: a
# republished tag would execute there during a routine deploy. The multi-arch index
# digest still resolves per-platform. A tag bump means re-resolving the digest with
# `docker buildx imagetools inspect <ref> --format '{{json .Manifest}}' | jq -r .digest`.
ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.0-python3.12@sha256:96e99f25815f533b298a4d53f283adf5c84c27334ea16ef232777cb800bddf10
FROM ${AIRFLOW_BASE_IMAGE}

# The stack's two Python tools (ingest, dq) live in their own virtualenv at
# /opt/data-venv, deliberately NOT in Airflow's. DAGs shell out to them rather
# than importing them, so dlt and Great Expectations can never drag Airflow's
# own dependency tree into a resolution conflict.

USER root

# curl is load-bearing: the smoke DAG probes Metabase's health endpoint with it.
# jq and git are for the operator in a one-off `airflow-cli` container — nothing
# the image runs unattended shells out to either.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends jq git curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# uv builds the workspace venv. Digest-pinned for the same reason as the base
# image; a tag bump means re-resolving it.
COPY --from=ghcr.io/astral-sh/uv:0.8.17@sha256:e4644cb5bd56fdc2c5ea3ee0525d9d21eed1603bccd6a21f887a938be7e85be1 /uv /usr/local/bin/uv

WORKDIR /opt/project

# Manifests first so a code-only change reuses the resolved dependency layer.
COPY pyproject.toml uv.lock ./
COPY pipeline/pyproject.toml ./pipeline/
COPY quality/pyproject.toml ./quality/

# Source needed for the editable installs to build. Compose bind-mounts these
# same paths at runtime, so edits to SQL, suites and DAGs land without a rebuild
# — but a DEPENDENCY change still requires `make build`.
COPY pipeline/src ./pipeline/src
COPY quality/src ./quality/src

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
    for tool in ingest dq; do ln -sf /opt/data-venv/bin/$tool /usr/local/bin/$tool; done; \
    chown -R airflow:0 /opt/data-venv /opt/project

USER airflow

# Fail fast at build time rather than in a DAG task at 03:00.
RUN ingest --help >/dev/null && dq --help >/dev/null
