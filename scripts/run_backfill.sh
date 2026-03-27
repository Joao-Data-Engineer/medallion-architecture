#!/usr/bin/env bash
# run_backfill.sh - Triggers Airflow backfill for a date range
# Usage: bash scripts/run_backfill.sh 2024-01-01 2024-06-01
set -euo pipefail

START_DATE="${1:-2024-01-01}"
END_DATE="${2:-2024-03-01}"

echo "==> Running Bronze DAG backfill from ${START_DATE} to ${END_DATE}..."
docker exec medallion-airflow-scheduler \
  airflow dags backfill dag_bronze_ingest \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}" \
    --reset-dagruns

echo "==> Backfill complete. Monitor at http://localhost:8080"
