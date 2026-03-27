.PHONY: help up down init seed run-pipeline test lint build logs clean

COMPOSE = docker compose
PROFILES = --profile core --profile orchestration --profile processing --profile serving --profile monitoring

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Infrastructure ───────────────────────────────────────────────────────────

build:  ## Build all custom Docker images
	$(COMPOSE) $(PROFILES) build

up: build  ## Start all services (builds images first)
	$(COMPOSE) --profile core up -d
	@echo "Waiting for core services to be healthy..."
	@sleep 5
	$(COMPOSE) --profile orchestration up -d
	$(COMPOSE) --profile processing up -d
	$(COMPOSE) --profile serving up -d
	$(COMPOSE) --profile monitoring up -d
	@echo ""
	@echo "Services running:"
	@echo "  Airflow UI    -> http://localhost:8080"
	@echo "  MinIO UI      -> http://localhost:9001"
	@echo "  Spark UI      -> http://localhost:8081"
	@echo "  Superset      -> http://localhost:8088"
	@echo "  Grafana       -> http://localhost:3000"
	@echo "  Prometheus    -> http://localhost:9090"

up-core:  ## Start only core services (MinIO + Postgres)
	$(COMPOSE) --profile core up -d

up-orchestration: up-core  ## Start core + Airflow
	$(COMPOSE) --profile orchestration up -d

up-processing: up-core  ## Start core + Spark
	$(COMPOSE) --profile processing up -d

up-serving: up-core  ## Start core + Superset
	$(COMPOSE) --profile serving up -d

up-monitoring:  ## Start monitoring stack (Prometheus + Grafana)
	$(COMPOSE) --profile monitoring up -d

down:  ## Stop and remove all containers
	$(COMPOSE) $(PROFILES) down

down-volumes:  ## Stop containers AND remove volumes (destructive!)
	$(COMPOSE) $(PROFILES) down -v

logs:  ## Follow logs from all running services
	$(COMPOSE) $(PROFILES) logs -f

logs-airflow:  ## Follow Airflow scheduler and webserver logs
	$(COMPOSE) logs -f airflow-webserver airflow-scheduler

logs-spark:  ## Follow Spark logs
	$(COMPOSE) logs -f spark-master spark-worker

# ─── Initialization ───────────────────────────────────────────────────────────

init: ## Initialize MinIO buckets and Airflow connections
	@echo "Initializing MinIO buckets..."
	bash scripts/init_minio.sh
	@echo "Initializing Superset..."
	bash scripts/init_superset.sh
	@echo "Initialization complete."

# ─── Data ─────────────────────────────────────────────────────────────────────

seed:  ## Download Jan-Feb 2024 NYC Taxi data for local development
	bash scripts/seed_data.sh

# ─── Pipeline ─────────────────────────────────────────────────────────────────

run-pipeline:  ## Trigger the full pipeline (Bronze -> Silver -> Gold)
	docker exec medallion-airflow-scheduler airflow dags trigger dag_bronze_ingest
	@echo "Pipeline triggered. Monitor progress at http://localhost:8080"

run-bronze:  ## Trigger only the Bronze ingestion DAG
	docker exec medallion-airflow-scheduler airflow dags trigger dag_bronze_ingest

run-silver:  ## Trigger only the Silver transformation DAG
	docker exec medallion-airflow-scheduler airflow dags trigger dag_silver_transform

run-gold:  ## Trigger only the Gold dbt build DAG
	docker exec medallion-airflow-scheduler airflow dags trigger dag_gold_build

backfill:  ## Backfill Bronze DAG for a date range (usage: make backfill START=2024-01-01 END=2024-03-01)
	docker exec medallion-airflow-scheduler airflow dags backfill dag_bronze_ingest \
		--start-date $(START) --end-date $(END)

# ─── Testing ──────────────────────────────────────────────────────────────────

test:  ## Run all unit and integration tests
	pytest tests/ -v --cov=spark_jobs --cov-report=term-missing --cov-fail-under=80

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-integration:  ## Run integration tests only (requires running Spark)
	pytest tests/integration/ -v

test-dbt:  ## Validate dbt models (parse + compile, no data required)
	docker exec medallion-airflow-scheduler bash -c \
		"cd /opt/airflow/dbt/nyc_taxi && dbt parse && dbt compile"

# ─── Code Quality ─────────────────────────────────────────────────────────────

lint:  ## Run ruff linter and black formatter check
	ruff check .
	black --check .

format:  ## Auto-format code with black and ruff --fix
	black .
	ruff check --fix .

# ─── Utilities ────────────────────────────────────────────────────────────────

dbt-docs:  ## Generate and serve dbt documentation catalog
	docker exec medallion-airflow-scheduler bash -c \
		"cd /opt/airflow/dbt/nyc_taxi && dbt docs generate && dbt docs serve --port 8085"

delta-history:  ## Show Delta Lake transaction history for Silver table
	docker exec medallion-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		spark_jobs/utils/show_delta_history.py

clean:  ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
