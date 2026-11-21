# Single entry point for the project. `make help` lists the targets.
.DEFAULT_GOAL := help
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

UV ?= uv
COMPOSE ?= docker compose
# The local stack: compose.yaml + compose.override.yaml with the RustFS profile on.
STACK := $(COMPOSE) --profile local-storage

# Linters run from pinned images or pinned PyPI wheels, the same versions as CI.
HADOLINT_IMAGE := hadolint/hadolint:v2.15.1@sha256:32dac94127fd60b7b7e3fbfc65e1383b9b5e25c9bfd7b8536de7a539fe68a12d
GITLEAKS_IMAGE := ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f
ACTIONLINT := $(UV) tool run --from 'actionlint-py==1.7.12.25' --with 'shellcheck-py==0.11.0.1' actionlint
SHELLCHECK := $(UV) tool run --from 'shellcheck-py==0.11.0.1' shellcheck
SHELL_SCRIPTS := scripts/*.sh deploy/*.sh
# In a git worktree .git is a file pointing at the main repository: gitleaks needs both mounted.
GIT_COMMON_DIR = $(shell git rev-parse --path-format=absolute --git-common-dir)

# Dummy values that only let `docker compose config` validate the files without a real .env.
COMPOSE_DUMMY_ENV := IBERGRID_S3_ACCESS_KEY=x IBERGRID_S3_SECRET_KEY=x \
	LAKEKEEPER_DB_PASSWORD=x LAKEKEEPER_ENCRYPTION_KEY=x DAGSTER_DB_PASSWORD=x \
	REDPANDA_ADMIN_PASSWORD=x REDPANDA_PRODUCER_PASSWORD=x REDPANDA_CONSUMER_PASSWORD=x
PROD_DUMMY_ENV := $(COMPOSE_DUMMY_ENV) \
	IBERGRID_S3_ENDPOINT_URL=https://account-id.r2.cloudflarestorage.com \
	IBERGRID_APP_DIGEST=sha256:1111111111111111111111111111111111111111111111111111111111111111 \
	IBERGRID_ORCHESTRATION_DIGEST=sha256:2222222222222222222222222222222222222222222222222222222222222222

.PHONY: help sync lock lint format typecheck test test-integration test-live contracts audit ci compose-config \
	hadolint shellcheck actionlint gitleaks env build up down down-volumes ps logs smoke \
	stream-up stream-down stream-status chaos-stream

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-17s\033[0m %s\n", $$1, $$2}'

# --- Python ----------------------------------------------------------------------------------

sync: ## Install the exact versions in uv.lock (runtime and dev group)
	$(UV) sync --locked

lock: ## Re-resolve uv.lock after editing pyproject.toml
	$(UV) lock

lint: ## uv.lock up to date, ruff check and ruff format --check
	$(UV) lock --check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Apply ruff fixes and formatting
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

typecheck: ## Strict mypy over src/ and tests/
	$(UV) run mypy

test: ## Unit tests with branch coverage (fails under 90 %)
	$(UV) run pytest --cov --cov-report=term-missing

test-integration: ## Integration tests against the running stack (make up first)
	$(UV) run pytest -m integration tests/integration

test-live: ## Opt-in smoke tests against REData and omie.es (a handful of requests)
	$(UV) run pytest -m live tests/live

contracts: ## Rewrite the committed JSON Schemas from the pydantic contracts (a unit test compares them)
	$(UV) run python -c "from ibergrid.stream.contract import json_schema; print(json_schema(), end='')" \
		> contracts/redata-demand-realtime.v1.json

audit: ## Known vulnerabilities in the locked dependencies (pip-audit)
	$(UV) export --frozen --all-groups --no-emit-project --format requirements-txt \
		| $(UV) run pip-audit --requirement /dev/stdin --disable-pip --progress-spinner off

# --- Repository checks -----------------------------------------------------------------------

ci: lint typecheck test compose-config hadolint shellcheck actionlint gitleaks ## Everything the CI Quality and Repository jobs check

compose-config: ## Validate the local and the production compose files (dummy values)
	$(COMPOSE_DUMMY_ENV) $(COMPOSE) --profile '*' config -q
	$(PROD_DUMMY_ENV) $(COMPOSE) -f compose.yaml -f compose.prod.yaml config -q

hadolint: ## Lint the Dockerfile (pinned hadolint image)
	docker run --rm -i $(HADOLINT_IMAGE) < Dockerfile

shellcheck: ## Lint the shell scripts
	$(SHELLCHECK) -x $(SHELL_SCRIPTS)

actionlint: ## Lint the GitHub workflows (with shellcheck on their run steps)
	$(ACTIONLINT) -no-color

gitleaks: ## Scan the whole history of the current branch for secrets (pinned gitleaks image)
	docker run --rm \
		-e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
		-v "$(CURDIR):$(CURDIR):ro" -v "$(GIT_COMMON_DIR):$(GIT_COMMON_DIR):ro" \
		$(GITLEAKS_IMAGE) git "$(CURDIR)" --log-opts=HEAD --redact --no-banner --no-color --exit-code 1

# --- Local stack -----------------------------------------------------------------------------

env: ## Create .env with random local secrets (never overwrites an existing one)
	./scripts/gen-env.sh

build: ## Build the app and orchestration images (ibergrid-app:local, ibergrid-orchestration:local)
	$(COMPOSE) build catalog-init dagster-code

up: ## Build the images and start the stack; waits until every service is ready (Dagster UI: http://127.0.0.1:28300)
	$(STACK) up -d --wait --build

down: ## Stop the stack, keeping its volumes
	$(COMPOSE) --profile '*' down --remove-orphans

down-volumes: ## Stop the stack and DELETE its volumes (bucket, catalog database, broker data)
	$(COMPOSE) --profile '*' down --remove-orphans --volumes

ps: ## Status of every service, one-shots included
	$(COMPOSE) --profile '*' ps -a

logs: ## Follow the logs; SERVICE=<name> for one service
	$(COMPOSE) --profile '*' logs -f --tail=100 $(SERVICE)

smoke: ## ibergrid doctor from inside the compose network
	$(COMPOSE) run --rm --no-deps doctor

# --- Streaming lane (ADR 0006) -------------------------------------------------------------------

stream-up: ## Create the lake's tables, then start the stream producer and consumer (they poll REData)
	$(COMPOSE) run --rm --no-deps doctor lake init
	$(STACK) --profile stream up -d --wait stream-producer stream-consumer

stream-down: ## Stop the stream producer and consumer (the consumer flushes first)
	$(COMPOSE) --profile stream stop stream-producer stream-consumer

stream-status: ## The stream table's checkpoint, the lag per partition and the dead letters
	$(COMPOSE) run --rm --no-deps stream-consumer stream status

chaos-stream: build ## SIGKILL the consumer mid-flush in a throwaway compose project, then prove exactly once
	$(UV) run python -u scripts/chaos_stream.py
