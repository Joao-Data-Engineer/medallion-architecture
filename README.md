# Medallion Architecture — NYC Taxi Analytics Platform

A production-grade **Medallion Architecture ETL pipeline** built from scratch with industry-standard tools.
Processes 3M+ rows/month of NYC taxi data through Bronze → Silver → Gold layers, fully containerized and reproducible locally.

---

## Architecture

```
NYC TLC Public S3 (Parquet, ~3M rows/month)
        │
        │  Airflow DAG (monthly)
        ▼
┌──────────────────────────────────────────────┐
│  BRONZE  s3://bronze/raw/yellow_taxi/        │
│  Format: Raw Parquet (immutable, versioned)  │
└──────────────────┬───────────────────────────┘
                   │  Great Expectations (schema, row count)
                   │  PySpark 3.5 (cast · dedupe · outliers · zone join)
                   ▼
┌──────────────────────────────────────────────┐
│  SILVER  s3://silver/yellow_taxi/            │
│  Format: Delta Lake (ACID · time travel)     │
│  Partitioned: year / month / pickup_borough  │
└──────────────────┬───────────────────────────┘
                   │  Great Expectations (nulls, ranges, referential integrity)
                   │  dbt 1.8 (staging → intermediate → marts + tests)
                   ▼
┌──────────────────────────────────────────────┐
│  GOLD    s3://gold/fct_hourly_zone_revenue/  │
│          s3://gold/fct_daily_borough_demand/ │
│          s3://gold/mart_payment_analysis/    │
│  Format: Delta Lake                          │
└──────────────────┬───────────────────────────┘
                   │  DuckDB (reads Delta from MinIO via S3 endpoint)
                   ▼
         Apache Superset Dashboards
```

**Orchestration:** Three Airflow DAGs with `ExternalTaskSensor` coupling:
`dag_bronze_ingest` → `dag_silver_transform` → `dag_gold_build`

---

## Tech Stack

| Layer | Technology |
|---|---|
| Object Storage | MinIO (S3-compatible, local) |
| Bronze | Raw Parquet (immutable) |
| Silver | Delta Lake 3.1.0 (ACID, time travel, idempotent MERGE) |
| Gold | Delta Lake + dbt 1.8 |
| Processing | Apache Spark 3.5 (PySpark) |
| Gold Transforms | dbt-spark (SQL business logic, testable, self-documenting) |
| Orchestration | Apache Airflow 2.9 (TaskFlow API) |
| Data Quality | Great Expectations 0.18 (checkpoint per layer) |
| Serving | Apache Superset 3.1 + DuckDB 0.10 |
| Infrastructure | Docker Compose with profiles |
| CI/CD | GitHub Actions (lint + unit tests + dbt parse) |
| Monitoring | Prometheus + Grafana (Airflow StatsD + MinIO metrics) |

---

## Quick Start

> Prerequisites: Docker Desktop, Docker Compose v2, ~8 GB RAM available

```bash
# 1. Clone and configure
git clone https://github.com/joaovictoria/medallion-architecture.git
cd medallion-architecture
cp .env.example .env

# 2. Start all services (~2 min first time, downloads images)
make up

# 3. Initialize MinIO buckets + Superset
make init

# 4. Download Jan-Feb 2024 NYC Taxi data (~120 MB)
make seed

# 5. Run the full pipeline
make run-pipeline
```

**Access the services:**

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark UI | http://localhost:8081 | — |
| Superset | http://localhost:8088 | admin / admin123 |
| Grafana | http://localhost:3000 | admin / admin123 |

---

## Project Structure

```
medallion-architecture/
├── .github/workflows/       # CI: lint + unit tests + dbt parse
├── docker/                  # Custom Dockerfiles (Airflow, Spark, Superset)
├── dags/
│   ├── dag_bronze_ingest.py
│   ├── dag_silver_transform.py
│   ├── dag_gold_build.py
│   ├── operators/           # TLCIngestOperator (multipart S3 upload)
│   └── sensors/             # MinioKeySensor
├── spark_jobs/
│   ├── bronze_to_silver/    # yellow_taxi_transformer.py, schema_definitions.py
│   └── utils/               # delta_utils.py, metrics.py, minio_client.py
├── dbt/nyc_taxi/
│   └── models/
│       ├── staging/         # stg_yellow_taxi, stg_taxi_zones
│       ├── intermediate/    # int_trips_enriched (ephemeral)
│       └── marts/           # fct_hourly_zone_revenue, fct_daily_borough_demand,
│                            # mart_payment_analysis, dim_taxi_zones
├── great_expectations/      # Bronze / Silver / Gold expectation suites
├── monitoring/              # Prometheus + Grafana dashboards
├── tests/
│   ├── unit/                # PySpark unit tests (no I/O, synthetic data)
│   └── integration/         # Full Bronze→Silver pipeline test
├── config/pipeline_config.yml  # Centralized thresholds, paths, Spark conf
├── docs/
│   ├── data_dictionary.md
│   ├── runbook.md
│   └── decisions/           # ADR-001, ADR-002, ADR-003
├── scripts/                 # init_minio.sh, seed_data.sh
├── docker-compose.yml
└── Makefile
```

---

## Design Decisions

### Idempotent Pipeline
The Silver layer uses Delta Lake `MERGE` on a surrogate key `(pickup_datetime, PULocationID, DOLocationID, fare_amount)`. Running the same month twice produces the same row count. The Bronze operator checks file existence before downloading.

### Schema-First Development
`schema_definitions.py` defines explicit `StructType` schemas for Bronze source and Silver target. If TLC changes a column type, the Spark job fails at read time — not silently downstream.

### Spark / dbt Boundary
PySpark handles volume and schema complexity (Bronze→Silver). dbt handles business logic in SQL (Silver→Gold). Business analysts can extend Gold models without touching Python code. See [ADR-002](docs/decisions/ADR-002-dbt-for-gold-layer.md).

### Data Quality as a Gate
Great Expectations checkpoints run at every layer transition with fail-halt semantics. A Bronze checkpoint failure aborts the run; a Silver failure quarantines the batch to `s3://quarantine/`.

### Delta Time Travel
All Silver tables retain full transaction history. Any Gold model can be rebuilt against a previous Silver snapshot: `delta_scan('s3://silver/yellow_taxi/', version=3)`.

### Structured Observability
`metrics.py` uses `structlog` to emit JSON log lines with consistent fields: `pipeline_layer`, `rows_in`, `rows_out`, `duration_seconds`. Machine-parseable by Grafana Loki.

---

## Testing

```bash
make test           # All tests with coverage (>80% required)
make test-unit      # Unit tests only (no services needed)
make test-dbt       # dbt parse + compile validation
```

---

## Key Commands

```bash
make up             # Start all services
make down           # Stop all services
make seed           # Download Jan-Feb 2024 data to Bronze
make run-pipeline   # Trigger full Bronze→Silver→Gold pipeline
make dbt-docs       # Open dbt data catalog at http://localhost:8085
make lint           # Run ruff + black
make delta-history  # Show Silver Delta transaction log
```

---

## Acknowledgements

Data source: [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
