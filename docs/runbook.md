# Runbook — Medallion Architecture Operations

## Backfill Missing Months

If a monthly run failed and you need to re-ingest a past month:

```bash
# Option 1: Airflow backfill via CLI
bash scripts/run_backfill.sh 2024-01-01 2024-04-01

# Option 2: Makefile shortcut
make backfill START=2024-01-01 END=2024-04-01

# Option 3: Trigger manually via Airflow UI
# DAGs -> dag_bronze_ingest -> Trigger w/ Config:
# {"year": 2024, "month": 3}
```

## Re-run a Failed DAG

```bash
# Clear and re-run a specific DAG run
docker exec medallion-airflow-scheduler \
  airflow tasks clear dag_silver_transform \
  --dag-run-id <run_id> --yes

# Or from the Airflow UI: Grid View -> failed task -> Clear
```

## Check Silver Delta Table Health

```bash
# View transaction history
make delta-history

# Or directly via spark-shell
docker exec medallion-spark-master \
  /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -e "DESCRIBE HISTORY delta.\`s3a://silver/yellow_taxi\`"
```

## Run VACUUM to Free Storage

```bash
# Vacuum Silver table (removes files older than 7 days)
docker exec medallion-spark-master \
  /opt/spark/bin/spark-sql \
  -e "VACUUM delta.\`s3a://silver/yellow_taxi\` RETAIN 168 HOURS"
```

## Add Green Taxi Data (Extending the Pipeline)

1. Add `green` to `source.taxi_types` in `config/pipeline_config.yml`
2. Add `GreenTaxiSchema` to `spark_jobs/bronze_to_silver/schema_definitions.py`
   (Green Taxi has slightly different column names: `lpep_pickup_datetime`, etc.)
3. Create `spark_jobs/bronze_to_silver/green_taxi_transformer.py`
   (copy `yellow_taxi_transformer.py`, adjust column names)
4. Add a new task to `dag_bronze_ingest.py` for Green Taxi files
5. Add `stg_green_taxi.sql` to `dbt/nyc_taxi/models/staging/`
6. Update marts to UNION yellow + green via a `stg_all_trips.sql` intermediate model

## Check Data Quality Results

```bash
# Browse GX validation results in MinIO
mc ls medallion/data-quality/gx-results/validations/ --recursive

# Or open the GX Data Docs (if generated)
docker exec medallion-airflow-webserver \
  bash -c "cd /opt/airflow/great_expectations && python -m http.server 8090 \
  --directory uncommitted/data_docs/local_site/"
# Then open http://localhost:8090
```

## View dbt Catalog

```bash
make dbt-docs
# Opens at http://localhost:8085
```

## Reset Everything (Clean Slate)

```bash
make down-volumes   # DESTRUCTIVE: removes all data volumes
make up
make init
make seed
make run-pipeline
```
