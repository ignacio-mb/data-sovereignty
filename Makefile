SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
# Run one-off commands in the Airflow image: it has ingest and dq on PATH.
RUN := $(COMPOSE) --profile cli run --rm airflow-cli

.PHONY: help env up down nuke bootstrap ingest backfill quality docs status logs ch \
        test test-warehouse build smoke secrets-push secrets-pull remote tunnels \
        deploy-status hold unhold disk

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ─── Lifecycle ───────────────────────────────────────────────────────────────

env: ## Create .env from .env.example and generate Airflow secrets
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; \
	 else echo ".env already exists, leaving it alone"; fi
	@bash scripts/gen_secrets.sh .env
	@echo "Now fill in PYLON_API_KEY, MB_PREMIUM_EMBEDDING_TOKEN and MB_ADMIN_PASSWORD."

build: ## Build the Airflow image (ingest + dq)
	$(COMPOSE) build

up: ## Bring the stack up in stages and bootstrap Metabase
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

bootstrap: ## (Re-)run Metabase bootstrap: setup, warehouse connection, API key
	bash scripts/bootstrap_metabase.sh

# ─── Pipeline ────────────────────────────────────────────────────────────────

ingest: ## Trigger the hourly ingest DAG now
	$(RUN) airflow dags trigger pylon_ingest_hourly

backfill: ## Trigger a backfill: make backfill START=2026-01-01 [END=2026-02-01]
	@test -n "$(START)" || (echo "usage: make backfill START=YYYY-MM-DD [END=YYYY-MM-DD]" && exit 2)
	$(RUN) airflow dags trigger pylon_backfill \
		--conf '{"start":"$(START)","end":"$(END)"}'

quality: ## Run the raw data-quality checkpoint
	$(RUN) dq run --checkpoint raw_pylon

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

tunnels: ## Forward every UI from the instance to localhost (Ctrl-C to stop)
	@echo "  Metabase http://localhost:3100   Airflow http://localhost:8080"
	@echo "  Data docs http://localhost:8081  ClickHouse http://localhost:8124"
	ssh -N -L 3100:localhost:3100 -L 8080:localhost:8080 \
	        -L 8081:localhost:8081 -L 8124:localhost:8124 $(DS_REMOTE_HOST)

deploy-status: ## What is live on the instance, and the last few deploys
	@ssh $(DS_REMOTE_HOST) 'cat /data/deploy/state.json 2>/dev/null; echo; tail -5 /data/deploy/history.log 2>/dev/null'

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

test-warehouse: ## Run tests that need the live warehouse
	uv run pytest -m clickhouse
