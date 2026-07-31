SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
# Run one-off commands in the Airflow image: it has ingest and dq on PATH.
RUN := $(COMPOSE) --profile cli run --rm airflow-cli

.PHONY: help env require-env require-local up down nuke bootstrap sources ingest backfill quality \
        docs status logs ch ch-q test test-dags build smoke secrets-push secrets-pull \
        remote tunnels deploy-status hold unhold disk

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ─── Lifecycle ───────────────────────────────────────────────────────────────

env: ## Create .env from .env.example and generate Airflow secrets
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; \
	 else echo ".env already exists, leaving it alone"; fi
	@bash scripts/gen_secrets.sh .env
	@echo "Now fill in MB_PREMIUM_EMBEDDING_TOKEN and MB_ADMIN_PASSWORD."
	@echo "Then connect a source: nothing is ingested until you do."

# Every credential in docker-compose.yml interpolates from .env, and the
# containers read it wholesale so a source's token needs no compose edit. Without
# it the stack used to come up with blank passwords; now it says so first.
require-env:
	@test -f .env || { echo "No .env yet — run 'make env' first."; exit 2; }

# Every target that mutates the local stack goes through here first. `localhost`
# names a port, not an instance: if a tunnel holds one of ours, the command runs
# against the instance instead and says nothing. See scripts/assert_local_stack.sh.
require-local:
	@test -n "$(DS_SKIP_LOCAL_CHECK)" || bash scripts/assert_local_stack.sh

build: require-env ## Build the Airflow image (ingest + dq)
	$(COMPOSE) build

up: require-env require-local ## Bring the stack up in stages and bootstrap Metabase
	$(COMPOSE) up -d --wait warehouse-db metabase-app-db airflow-db metabase
	bash scripts/bootstrap_metabase.sh
	$(COMPOSE) up -d --wait airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer datadocs
	@$(MAKE) --no-print-directory status

down: ## Stop the stack, keep all data
	$(COMPOSE) --profile cli down

nuke: ## Stop the stack and DESTROY all volumes (warehouse + dlt state + Metabase content)
	@echo "This deletes the warehouse, dlt cursor state and everything in Metabase."
	@read -p "Type 'nuke' to confirm: " c && [ "$$c" = "nuke" ]
	$(COMPOSE) --profile cli down -v

bootstrap: require-local ## (Re-)run Metabase bootstrap: setup, warehouse connection, API key
	bash scripts/bootstrap_metabase.sh

# ─── Pipeline ────────────────────────────────────────────────────────────────
# Every target here takes SOURCE, because this repo ships with no sources: the
# DAGs are generated per spec in sources/, so there is no default one to mean.
# `make sources` lists what is connected.

sources: ## List the connected sources
	$(RUN) ingest sources

ingest: ## Trigger a source's ingest DAG now: make ingest SOURCE=name
	@test -n "$(SOURCE)" || (echo "usage: make ingest SOURCE=name  (make sources to list)" && exit 2)
	$(RUN) airflow dags trigger $(SOURCE)_ingest

backfill: ## Backfill a range: make backfill SOURCE=name START=2026-01-01 [END=2026-02-01]
	@test -n "$(SOURCE)" || (echo "usage: make backfill SOURCE=name START=YYYY-MM-DD [END=YYYY-MM-DD]" && exit 2)
	@test -n "$(START)" || (echo "usage: make backfill SOURCE=name START=YYYY-MM-DD [END=YYYY-MM-DD]" && exit 2)
	$(RUN) airflow dags trigger $(SOURCE)_backfill \
		--conf '{"start":"$(START)","end":"$(END)"}'

quality: ## Validate a source's raw contract: make quality SOURCE=name
	@test -n "$(SOURCE)" || (echo "usage: make quality SOURCE=name  (make sources to list)" && exit 2)
	$(RUN) dq run --source $(SOURCE)

docs: ## Rebuild Great Expectations data docs (served on $$DATADOCS_HOST_PORT)
	$(RUN) dq docs-build

# ─── Inspection ──────────────────────────────────────────────────────────────

status: ## Show service health and host URLs
	@$(COMPOSE) ps
	@set -a; . ./.env 2>/dev/null; set +a; \
	 echo; \
	 echo "  Metabase    http://localhost:$${METABASE_HOST_PORT##*:}"; \
	 echo "  Airflow     http://localhost:$${AIRFLOW_HOST_PORT##*:}"; \
	 echo "  Data docs   http://localhost:$${DATADOCS_HOST_PORT##*:}"; \
	 echo "  Warehouse   clickhouse http://localhost:$${WAREHOUSE_HTTP_PORT##*:} (native $${WAREHOUSE_NATIVE_PORT##*:})"

logs: ## Tail logs: make logs [S=metabase]
	$(COMPOSE) logs -f --tail=100 $(S)

ch: ## Open a clickhouse-client shell on the warehouse
	@set -a; . ./.env; set +a; \
	 $(COMPOSE) exec warehouse-db \
	   clickhouse-client --user "$$WAREHOUSE_USER" --password "$$WAREHOUSE_PASSWORD"

# The credentials are expanded INSIDE the container, from the environment the
# warehouse already has, so the password never reaches a host command line and
# never lands in a shell history or a CI log. Only the query does.
ch-q: ## Run one SQL statement on the warehouse: make ch-q Q='SELECT 1'
	@test -n "$(Q)" || (echo "usage: make ch-q Q='SELECT 1'" && exit 2)
	@$(COMPOSE) exec -T -e DS_QUERY="$(Q)" warehouse-db sh -c \
	  'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --query "$$DS_QUERY"'

smoke: ## Run the stack_smoke DAG against the running stack
	$(RUN) airflow dags test stack_smoke

# ─── The AWS host ────────────────────────────────────────────────────────────
# Merging to main is what deploys; see docs/deploy.md. These are the targets
# for the things a merge cannot do.
#
# .deploy.env is git-ignored and holds this host's coordinates:
#   DS_REMOTE_HOST=data-sovereignty   (the ~/.ssh/config alias)
#   DS_INSTANCE_ID=i-...
#   AWS_REGION=us-east-1
-include .deploy.env
DS_REMOTE_HOST ?= data-sovereignty

secrets-push: ## Push the secrets in .env to SSM Parameter Store (run once, and on rotation)
	bash scripts/secrets_push.sh

secrets-pull: ## Render .env on the instance from Parameter Store
	bash scripts/render_env_from_ssm.sh

remote: ## Run a command on the instance: make remote CMD='make status'
	@test -n "$(CMD)" || (echo "usage: make remote CMD='make status'" && exit 2)
	ssh -t $(DS_REMOTE_HOST) 'cd /data/data-sovereignty && $(CMD)'

tunnels: ## Forward every UI from the instance to localhost:32xx (Ctrl-C to stop)
	@echo "  PRODUCTION — these are the instance's services, not this laptop's."
	@echo "  Metabase http://localhost:3200   Airflow http://localhost:8180"
	@echo "  Data docs http://localhost:8181  ClickHouse http://localhost:8224"
	ssh -N -L 3200:localhost:3100 -L 8180:localhost:8080 \
	        -L 8181:localhost:8081 -L 8224:localhost:8124 $(DS_REMOTE_HOST)

# Distinguishes the three things that used to look identical, because the old
# one-liner ended in `tail` of a file that does not exist until the first deploy
# and so exited 1 — indistinguishable from "the instance is unreachable", which
# is what it was read as.
deploy-status: ## What is live on the instance, and the last few deploys
	@ssh -o BatchMode=yes -o ConnectTimeout=20 $(DS_REMOTE_HOST) true 2>/dev/null || { \
	  echo "cannot reach '$(DS_REMOTE_HOST)'."; \
	  echo "  ssh goes over SSM, so this is usually expired AWS credentials — try 'aws login'."; \
	  echo "  Check with: aws ssm describe-instance-information --query 'InstanceInformationList[].PingStatus'"; \
	  exit 1; }
	@ssh $(DS_REMOTE_HOST) 'set -e; \
	  if [ -s /data/deploy/state.json ]; then cat /data/deploy/state.json; \
	  else echo "no deploy has run on this instance yet (/data/deploy is empty)."; \
	       echo "The checkout is at $$(git -C /data/data-sovereignty rev-parse --short HEAD 2>/dev/null || echo unknown), placed there by the host bootstrap rather than by a deploy."; fi; \
	  echo; \
	  if [ -s /data/deploy/history.log ]; then tail -5 /data/deploy/history.log; \
	  else echo "(no deploy history)"; fi'

hold: ## Stop deploys landing: make hold REASON='migrating the warehouse'
	@test -n "$(REASON)" || (echo "usage: make hold REASON='why'" && exit 2)
	@ssh $(DS_REMOTE_HOST) "echo '$(REASON) — $$(date -u +%FT%TZ)' | sudo tee /data/deploy/HOLD"
	@echo "deploys will be declined until 'make unhold'"

unhold: ## Let deploys land again
	@ssh $(DS_REMOTE_HOST) 'sudo rm -f /data/deploy/HOLD'
	@echo "released; the converge timer will pick up any commit that was waiting"

disk: ## What is using /data on the instance
	@ssh $(DS_REMOTE_HOST) 'cd /data/data-sovereignty && bash scripts/disk_check.sh'

# ─── Tests ───────────────────────────────────────────────────────────────────

test: ## Run the offline test suite (mocked API, duckdb, no network)
	uv run pytest

test-dags: ## Import-check the generated DAGs (needs Airflow, not in the default env)
	uv sync --group dag-tests && uv run pytest airflow/tests
