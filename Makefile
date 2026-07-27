SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
# Run one-off commands in the Airflow image: it has pylon, dq, mbx and mb on PATH.
RUN := $(COMPOSE) --profile cli run --rm airflow-cli

.PHONY: help env up down nuke bootstrap ingest backfill quality docs status logs psql \
        test test-postgres build mb-audit mb-transforms mb-semantics mb-metadata \
        mb-dashboards mb-sync

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ─── Lifecycle ───────────────────────────────────────────────────────────────

env: ## Create .env from .env.example and generate Airflow secrets
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env"; \
	 else echo ".env already exists, leaving it alone"; fi
	@bash scripts/gen_secrets.sh .env
	@echo "Now fill in PYLON_API_KEY, MB_PREMIUM_EMBEDDING_TOKEN and MB_ADMIN_PASSWORD."

build: ## Build the Airflow image (pylon + dq + mbx + mb)
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

quality: ## Run both data-quality checkpoints
	$(RUN) dq run --checkpoint raw_pylon
	$(RUN) dq run --checkpoint marts

docs: ## Rebuild Great Expectations data docs (served on $$DATADOCS_HOST_PORT)
	$(RUN) dq docs-build

# ─── Metabase ────────────────────────────────────────────────────────────────

mb-audit: ## Verify Metabase version + EE token features, write docs/10_instance_capabilities.md
	$(RUN) mbx audit

mb-transforms: ## Build/refresh all transforms from metabase/transforms/manifest.yml
	$(RUN) mbx transforms

mb-semantics: ## Create/update metrics and segments
	$(RUN) mbx semantics

mb-metadata: ## Apply display names, semantic types and FK wiring
	$(RUN) mbx metadata

mb-dashboards: ## Build the Success Engineering and Pipeline Health dashboards
	$(RUN) mbx dashboards

mb-sync: ## Export Metabase content to the git-sync repo (no-op if unconfigured)
	$(RUN) mbx gitsync

# ─── Inspection ──────────────────────────────────────────────────────────────

status: ## Show service health and host URLs
	@$(COMPOSE) ps
	@set -a; . ./.env 2>/dev/null; set +a; \
	 echo; \
	 echo "  Metabase    http://localhost:$${METABASE_HOST_PORT:-3100}"; \
	 echo "  Airflow     http://localhost:$${AIRFLOW_HOST_PORT:-8080}"; \
	 echo "  Data docs   http://localhost:$${DATADOCS_HOST_PORT:-8081}"; \
	 echo "  Warehouse   postgres://localhost:$${WAREHOUSE_HOST_PORT:-5434}/$${WAREHOUSE_DB:-warehouse}"

logs: ## Tail logs: make logs [S=metabase]
	$(COMPOSE) logs -f --tail=100 $(S)

psql: ## Open a psql shell on the warehouse
	@set -a; . ./.env; set +a; \
	 $(COMPOSE) exec -e PGPASSWORD="$$WAREHOUSE_PASSWORD" warehouse-db \
	   psql -U "$$WAREHOUSE_USER" -d "$$WAREHOUSE_DB"

# ─── Tests ───────────────────────────────────────────────────────────────────

test: ## Run the offline test suite (mocked API, duckdb, no network)
	uv run pytest

test-postgres: ## Run tests that need the live warehouse
	uv run pytest -m postgres
