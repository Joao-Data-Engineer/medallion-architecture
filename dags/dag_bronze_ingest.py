"""
dag_bronze_ingest.py

Bronze layer ingestion DAG.
Runs monthly to download NYC TLC Yellow Taxi data and upload it to
the MinIO Bronze bucket. Uses TaskFlow API for clean task definition.

Pipeline:
  check_source → download_to_bronze → validate_bronze → trigger_silver

Schedule: @monthly (1st of each month at 02:00 UTC)
Idempotent: yes (TLCIngestOperator skips if file already exists)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import yaml
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from operators.tlc_ingest_operator import TLCIngestOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _load_config() -> dict:
    config_path = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/airflow/config/pipeline_config.yml")
    with open(config_path) as f:
        return yaml.safe_load(f)


@dag(
    dag_id="dag_bronze_ingest",
    description="Ingest NYC TLC Yellow Taxi monthly Parquet files into Bronze bucket",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "ingestion", "nyc-taxi"],
)
def bronze_ingest_dag():
    cfg = _load_config()

    @task()
    def check_source_availability(**context) -> dict:
        """Verifies the TLC source URL is reachable before attempting download."""
        import requests

        execution_date = context["logical_date"]
        year = execution_date.year
        month = execution_date.month
        month_str = f"{month:02d}"
        source_cfg = cfg["source"]
        filename = f"yellow_tripdata_{year}-{month_str}.parquet"
        url = f"{source_cfg['tlc_base_url']}/{filename}"

        response = requests.head(url, timeout=30)
        if response.status_code not in (200, 206):
            raise ValueError(f"Source file not available: {url} (HTTP {response.status_code})")

        content_length = response.headers.get("Content-Length", "unknown")
        return {
            "url": url,
            "content_length_bytes": content_length,
            "year": year,
            "month": month,
        }

    @task()
    def validate_bronze(s3_path: str, **context) -> dict:
        """
        Runs Great Expectations Bronze checkpoint against the uploaded file.
        Raises on validation failure, halting the pipeline.
        """
        import great_expectations as gx
        from great_expectations.core.batch import BatchRequest

        gx_context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
        batch_request = BatchRequest(
            datasource_name="bronze_s3_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="yellow_taxi",
            batch_spec_passthrough={
                "reader_method": "read_parquet",
                "reader_options": {},
            },
        )
        result = gx_context.run_checkpoint(
            checkpoint_name="bronze_checkpoint",
            batch_request=batch_request,
            run_name=context["run_id"],
        )
        if not result["success"]:
            stats = result["statistics"]
            raise ValueError(
                f"Bronze GX validation failed: "
                f"{stats['unsuccessful_expectations']} expectations failed "
                f"out of {stats['evaluated_expectations']}"
            )
        return {"validation_status": "passed", "s3_path": s3_path}

    source_info = check_source_availability()

    download = TLCIngestOperator(
        task_id="download_to_bronze",
        year="{{ logical_date.year }}",
        month="{{ logical_date.month }}",
    )

    validation = validate_bronze(download.output)

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="dag_silver_transform",
        conf={
            "year": "{{ logical_date.year }}",
            "month": "{{ logical_date.month }}",
            "bronze_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
    )

    source_info >> download >> validation >> trigger_silver


bronze_ingest_dag()
