"""
dag_silver_transform.py

Silver layer transformation DAG.
Triggered by dag_bronze_ingest upon successful ingestion.
Submits a PySpark job that reads Bronze Parquet, transforms it,
and writes to Silver Delta Lake.

Pipeline:
  sensor (bronze exists) → spark_submit → validate_silver → trigger_gold
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from sensors.minio_key_sensor import MinioKeySensor

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=60),
}

# S3A configuration passed to SparkSubmitOperator
SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "{{ var.value.minio_access_key }}",
    "spark.hadoop.fs.s3a.secret.key": "{{ var.value.minio_secret_key }}",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
}


@dag(
    dag_id="dag_silver_transform",
    description="Transform Bronze Parquet → Silver Delta Lake using PySpark",
    schedule=None,  # Triggered by Bronze DAG
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["silver", "spark", "delta-lake"],
)
def silver_transform_dag():

    # ── Sense Bronze file exists before starting Spark ────────────────────────
    bronze_sensor = MinioKeySensor(
        task_id="wait_for_bronze_file",
        bucket="bronze",
        key=(
            "raw/yellow_taxi/"
            "year={{ dag_run.conf.get('year', logical_date.year) }}/"
            "month={{ '%02d' % dag_run.conf.get('month', logical_date.month) | int }}/"
        ),
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    # ── Submit PySpark job ────────────────────────────────────────────────────
    spark_transform = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/spark_jobs/bronze_to_silver/yellow_taxi_transformer.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        application_args=[
            "--year", "{{ dag_run.conf.get('year', logical_date.year) }}",
            "--month", "{{ dag_run.conf.get('month', logical_date.month) }}",
        ],
        name="medallion-bronze-to-silver",
        verbose=False,
        env_vars={
            "PIPELINE_CONFIG_PATH": "/opt/spark/work-dir/config/pipeline_config.yml",
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        },
    )

    @task()
    def validate_silver(**context) -> dict:
        """Runs Great Expectations Silver checkpoint."""
        import great_expectations as gx

        gx_context = gx.get_context(
            context_root_dir="/opt/airflow/great_expectations"
        )
        result = gx_context.run_checkpoint(
            checkpoint_name="silver_checkpoint",
            run_name=context["run_id"],
        )
        if not result["success"]:
            stats = result["statistics"]
            raise ValueError(
                f"Silver GX validation failed: "
                f"{stats['unsuccessful_expectations']} expectations failed"
            )
        return {"validation_status": "passed"}

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="dag_gold_build",
        conf={
            "year": "{{ dag_run.conf.get('year', logical_date.year) }}",
            "month": "{{ dag_run.conf.get('month', logical_date.month) }}",
            "silver_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
    )

    bronze_sensor >> spark_transform >> validate_silver() >> trigger_gold


silver_transform_dag()
