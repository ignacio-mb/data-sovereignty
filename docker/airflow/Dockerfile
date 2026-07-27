ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.0-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

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
    npm install -g @metabase/cli@0.2.2; \
    apt-get purge -y --auto-remove gnupg; \
    rm -rf /var/lib/apt/lists/*

# uv builds the workspace venv.
COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /usr/local/bin/uv

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
RUN set -eux; \
    uv sync --frozen --no-dev; \
    for tool in pylon dq mbx; do ln -sf /opt/data-venv/bin/$tool /usr/local/bin/$tool; done; \
    chown -R airflow:0 /opt/data-venv /opt/project

USER airflow

# Fail fast at build time rather than in a DAG task at 03:00.
RUN pylon --help >/dev/null && dq --help >/dev/null && mbx --help >/dev/null && mb --version >/dev/null
