"""
dag_gold_build.py

Gold layer DAG. Triggered by dag_silver_transform after Silver validation passes.
Runs dbt build (models + tests) to produce Gold aggregation tables,
then validates with Great Expectations.

Pipeline:
  sensor (silver ready) → dbt_build → dbt_test → validate_gold
"""

from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from sensors.minio_key_sensor import MinioKeySensor

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt/nyc_taxi"
DBT_PROFILES_DIR = "/opt/airflow/dbt/nyc_taxi"


@dag(
    dag_id="dag_gold_build",
    description="Build Gold Delta Lake tables with dbt and validate with Great Expectations",
    schedule=None,  # Triggered by Silver DAG
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt", "delta-lake"],
)
def gold_build_dag():

    # ── Sense Silver Delta table has data ─────────────────────────────────────
    silver_sensor = MinioKeySensor(
        task_id="wait_for_silver_data",
        bucket="silver",
        key="yellow_taxi/_delta_log/",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    # ── dbt build: runs models + tests in dependency order ────────────────────
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --profiles-dir {DBT_PROFILES_DIR} --target dev --fail-fast"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        },
    )

    # ── Generate dbt docs (artifacts only, no serve) ──────────────────────────
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && " f"dbt docs generate --profiles-dir {DBT_PROFILES_DIR}"
        ),
        trigger_rule="all_success",
    )

    @task()
    def validate_gold(**context) -> dict:
        """Runs Great Expectations Gold checkpoint."""
        import great_expectations as gx

        gx_context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
        result = gx_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            run_name=context["run_id"],
        )
        if not result["success"]:
            stats = result["statistics"]
            raise ValueError(
                f"Gold GX validation failed: "
                f"{stats['unsuccessful_expectations']} expectations failed"
            )
        return {"validation_status": "passed"}

    silver_sensor >> dbt_build >> dbt_docs >> validate_gold()


gold_build_dag()
