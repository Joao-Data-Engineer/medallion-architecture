#!/usr/bin/env bash
# seed_data.sh - Downloads sample NYC Taxi data for local development
# Downloads Yellow Taxi data for Jan and Feb 2024 directly to MinIO Bronze bucket
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
MC_ALIAS="medallion"

TLC_BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

MONTHS=("2024-01" "2024-02")

echo "==> Configuring mc alias..."
mc alias set "${MC_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --quiet

# ─── Download Taxi Zone lookup (reference data) ───────────────────────────────
echo "==> Downloading Taxi Zone lookup..."
ZONE_KEY="bronze/raw/taxi_zones/taxi_zone_lookup.csv"

if mc ls "${MC_ALIAS}/${ZONE_KEY}" > /dev/null 2>&1; then
  echo "    Zone lookup already exists, skipping."
else
  curl -fsSL "${ZONE_LOOKUP_URL}" | \
    mc pipe "${MC_ALIAS}/${ZONE_KEY}"
  echo "    Uploaded: ${ZONE_KEY}"
fi

# ─── Download Yellow Taxi monthly files ───────────────────────────────────────
for MONTH in "${MONTHS[@]}"; do
  YEAR="${MONTH%-*}"
  MON="${MONTH#*-}"
  FILENAME="yellow_tripdata_${MONTH}.parquet"
  URL="${TLC_BASE_URL}/${FILENAME}"
  KEY="bronze/raw/yellow_taxi/year=${YEAR}/month=${MON}/${FILENAME}"

  echo "==> Processing ${FILENAME}..."

  if mc ls "${MC_ALIAS}/${KEY}" > /dev/null 2>&1; then
    echo "    Already exists in MinIO, skipping."
    continue
  fi

  echo "    Downloading from ${URL}..."
  TMP_FILE="/tmp/${FILENAME}"
  curl -fL --progress-bar "${URL}" -o "${TMP_FILE}"

  echo "    Uploading to s3://bronze/raw/yellow_taxi/year=${YEAR}/month=${MON}/..."
  mc cp "${TMP_FILE}" "${MC_ALIAS}/${KEY}"

  # Show row count estimate from file size
  SIZE=$(du -sh "${TMP_FILE}" | cut -f1)
  echo "    Done. File size: ${SIZE}"
  rm -f "${TMP_FILE}"
done

echo ""
echo "==> Seed data complete. Bronze bucket contents:"
mc ls --recursive "${MC_ALIAS}/bronze/"
