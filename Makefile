SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
UV := $(shell command -v uv 2>/dev/null)
ENV ?= dev
WORKFLOW ?= nyc_taxi_job
CATALOG ?=
WHEEL_FILE ?= $(shell ls -t dist/*.whl 2>/dev/null | sed 's#^.*/##' | head -n 1)
BUNDLE_VARS ?=
EFFECTIVE_BUNDLE_VARS = $(BUNDLE_VARS) $(if $(CATALOG),--var="catalog=$(CATALOG)",) $(if $(WHEEL_FILE),--var="wheel_file=$(WHEEL_FILE)",)

.PHONY: help install test lint format typecheck check clean uv-build dab-validate dab-deploy dab-run dab-destroy ensure-wheel-file

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; print "Available targets:"} /^[a-zA-Z_-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install project dependencies (prefers uv)
ifdef UV
	uv sync --dev
else
	$(PYTHON) -m pip install -e ".[dev]"
endif

test: ## Run unit tests
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -v

lint: ## Run Ruff lint checks
ifdef UV
	uv run ruff check .
else
	ruff check .
endif

format: ## Format code with Ruff formatter
ifdef UV
	uv run ruff format .
else
	ruff format .
endif

typecheck: ## Run mypy type checks
ifdef UV
	uv run mypy src tests
else
	mypy src tests
endif

check: lint typecheck test ## Run all quality gates

clean: ## Remove common local caches
	rm -rf .mypy_cache .pytest_cache .ruff_cache

uv-build: ## Build Python wheel with uv
	uv build --wheel

ensure-wheel-file:
	@if [[ -z "$(WHEEL_FILE)" ]]; then \
		echo "ERROR: wheel_file nao encontrado em dist/."; \
		echo "Execute 'make uv-build' primeiro ou passe WHEEL_FILE=<arquivo.whl>."; \
		exit 1; \
	fi

dab-validate: ensure-wheel-file ## Validate bundle (ENV=<target> [CATALOG=<name>] [WHEEL_FILE=<file.whl>] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle validate -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)

dab-deploy: uv-build ensure-wheel-file ## Deploy bundle (ENV=<target> [CATALOG=<name>] [WHEEL_FILE=<file.whl>] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle deploy -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)

dab-run: ensure-wheel-file ## Run workflow (ENV=<target> WORKFLOW=<job_name> [CATALOG=<name>] [WHEEL_FILE=<file.whl>] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle run -t $(ENV) $(WORKFLOW) $(EFFECTIVE_BUNDLE_VARS)

dab-destroy: ## Destroy bundle resources (ENV=<target> [CATALOG=<name>] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle destroy --auto-approve -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)
