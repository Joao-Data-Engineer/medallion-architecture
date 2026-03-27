#!/usr/bin/env bash
# init_superset.sh - Initializes Apache Superset with DuckDB datasource
set -euo pipefail

CONTAINER="medallion-superset"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-admin123}"

echo "==> Waiting for Superset to be ready..."
for i in $(seq 1 30); do
  if docker exec "${CONTAINER}" curl -sf http://localhost:8088/health > /dev/null 2>&1; then
    echo "==> Superset is ready."
    break
  fi
  echo "    Attempt ${i}/30 - waiting 3s..."
  sleep 3
done

echo "==> Initializing Superset database..."
docker exec "${CONTAINER}" superset db upgrade

echo "==> Creating admin user..."
docker exec "${CONTAINER}" superset fab create-admin \
  --username "${ADMIN_USER}" \
  --firstname Admin \
  --lastname User \
  --email admin@medallion.local \
  --password "${ADMIN_PASSWORD}" 2>/dev/null || echo "    Admin user already exists."

echo "==> Running superset init..."
docker exec "${CONTAINER}" superset init

echo ""
echo "==> Superset initialization complete."
echo "    URL: http://localhost:8088"
echo "    Login: ${ADMIN_USER} / ${ADMIN_PASSWORD}"
echo ""
echo "    NOTE: After the pipeline runs, add a DuckDB database connection:"
echo "    Settings -> Database Connections -> + Database -> DuckDB"
echo "    SQLAlchemy URI: duckdb:///:memory:?s3_endpoint=minio:9000"
