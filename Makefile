SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
UV := $(shell command -v uv 2>/dev/null)
ENV ?= dev
WORKFLOW ?= nyc_taxi_job
CATALOG ?=
WHEEL_FILE ?= $(shell ls -t dist/*.whl 2>/dev/null | sed 's#^.*/##' | head -n 1)
TARGET_YEAR ?=
TARGET_MONTHS ?=
TAXI_TYPE ?=
DISCOVER_FROM ?=
BUNDLE_VARS ?=
EFFECTIVE_BUNDLE_VARS = $(BUNDLE_VARS) \
	$(if $(CATALOG),--var="catalog=$(CATALOG)",) \
	$(if $(WHEEL_FILE),--var="wheel_file=$(WHEEL_FILE)",) \
	$(if $(TARGET_YEAR),--var="target_year=$(TARGET_YEAR)",) \
	$(if $(TARGET_MONTHS),--var="target_months=$(TARGET_MONTHS)",) \
	$(if $(TAXI_TYPE),--var="taxi_type=$(TAXI_TYPE)",) \
	$(if $(DISCOVER_FROM),--var="discover_from=$(DISCOVER_FROM)",)

# Bundle --var: resolvido no deploy / validate; não muda argv de um job já no workspace.
#
# dab-run: TARGET_YEAR, TARGET_MONTHS e TAXI_TYPE viram `bundle run --params` (job
# parameters em resources/nyc_taxi_job.yml). Demais overrides continuam como --var
# (CATALOG, WHEEL_FILE, DISCOVER_FROM, BUNDLE_VARS). Requer job redeployado com
# bloco `parameters` ao menos uma vez após essa convenção.
empty :=
space := $(empty) $(empty)
comma := ,
RUN_JOB_PARAMS := $(subst $(space),$(comma),$(strip \
	$(if $(TARGET_YEAR),target_year=$(TARGET_YEAR)) \
	$(if $(TARGET_MONTHS),target_months=$(TARGET_MONTHS)) \
	$(if $(TAXI_TYPE),taxi_type=$(TAXI_TYPE)) \
))
EFFECTIVE_BUNDLE_VARS_RUN = $(BUNDLE_VARS) \
	$(if $(CATALOG),--var="catalog=$(CATALOG)",) \
	$(if $(WHEEL_FILE),--var="wheel_file=$(WHEEL_FILE)",) \
	$(if $(DISCOVER_FROM),--var="discover_from=$(DISCOVER_FROM)",)

.PHONY: help install test test-landing test-bronze test-silver test-gold lint format typecheck check clean uv-build dab-validate dab-deploy dab-run dab-destroy ensure-wheel-file

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; print "Available targets:"} /^[a-zA-Z_-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Common variables:"
	@echo "  ENV            Bundle target: dev | stg | prd (default: dev)"
	@echo "  CATALOG        Override the catalog variable"
	@echo "  WHEEL_FILE     Override the wheel filename (auto-detected from dist/)"
	@echo "Bundle vars (databricks.yml):"
	@echo "  TARGET_YEAR    Default do job parameter target_year (deploy: --var; dab-run: vira --params)"
	@echo "  TARGET_MONTHS Idem target_months, ex.: 6 ou 1,2,3"
	@echo "  TAXI_TYPE     Idem taxi_type: yellow | green | both"
	@echo "  (dab-validate / dab-deploy / dab-destroy usam TARGET_* como --var p/ defaults)"
	@echo "  (dab-run repassa TARGET_* e TAXI_TYPE só como --params no job, não como --var)"
	@echo "  DISCOVER_FROM  Initial month for landing discovery mode (not used by schedule)"
	@echo "  BUNDLE_VARS    Extra --var=\"k=v\" pairs to forward to databricks bundle"
	@echo ""
	@echo "Examples:"
	@echo "  make dab-deploy ENV=dev"
	@echo "  make dab-deploy ENV=prd CATALOG=nyc_taxi_prd"
	@echo "  make dab-deploy ENV=dev TARGET_YEAR=2024 TARGET_MONTHS=1,2,3"
	@echo "  make dab-deploy ENV=dev TAXI_TYPE=green   # ingere apenas green_taxi"
	@echo "  make dab-run    ENV=dev TARGET_YEAR=2023 TARGET_MONTHS=6 TAXI_TYPE=both"
	@echo "  # dab-run exige job com bloco parameters no workspace; se necessário: make dab-deploy ENV=dev"

install: ## Install project dependencies (prefers uv)
ifdef UV
	uv sync --dev
else
	$(PYTHON) -m pip install -e ".[dev]"
endif

test: ## Run unit tests
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -v

test-landing: ## Run landing layer unit tests
	$(PYTHON) -m unittest tests/test_landing_main.py -v

test-bronze: ## Run bronze layer unit tests
	$(PYTHON) -m unittest tests/test_bronze_main.py -v

test-silver: ## Run silver layer unit tests
	$(PYTHON) -m unittest tests/test_silver_main.py -v

test-gold: ## Run gold layer unit tests
	$(PYTHON) -m unittest tests/test_gold_main.py -v

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

dab-validate: ensure-wheel-file ## Validate bundle (ENV=<dev|stg|prd> [CATALOG=<name>] [WHEEL_FILE=<file.whl>] [TARGET_YEAR=YYYY] [TARGET_MONTHS=M1,M2,...] [TAXI_TYPE=yellow|green|both] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle validate -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)

dab-deploy: uv-build ensure-wheel-file ## Deploy bundle (ENV=<dev|stg|prd> [CATALOG=<name>] [WHEEL_FILE=<file.whl>] [TARGET_YEAR=YYYY] [TARGET_MONTHS=M1,M2,...] [TAXI_TYPE=yellow|green|both] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle deploy -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)

dab-run: ensure-wheel-file ## Roda o job: TARGET_YEAR, TARGET_MONTHS, TAXI_TYPE -> --params; catalog/wheel/discover -> --var
	databricks bundle run -t $(ENV) $(EFFECTIVE_BUNDLE_VARS_RUN) $(if $(strip $(RUN_JOB_PARAMS)),--params $(RUN_JOB_PARAMS)) $(WORKFLOW)

dab-destroy: ## Destroy bundle resources (ENV=<dev|stg|prd> [CATALOG=<name>] [BUNDLE_VARS='--var="k=v"'])
	databricks bundle destroy --auto-approve -t $(ENV) $(EFFECTIVE_BUNDLE_VARS)