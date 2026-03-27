#!/usr/bin/env bash
# init_minio.sh - Creates MinIO buckets and policies for the Medallion pipeline
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
MC_ALIAS="medallion"

echo "==> Waiting for MinIO to be ready at ${MINIO_ENDPOINT}..."
for i in $(seq 1 30); do
  if curl -sf "${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
    echo "==> MinIO is ready."
    break
  fi
  echo "    Attempt ${i}/30 - waiting 2s..."
  sleep 2
done

# Install mc (MinIO Client) if not present
if ! command -v mc &> /dev/null; then
  echo "==> Installing MinIO Client (mc)..."
  curl -fsSL "https://dl.min.io/client/mc/release/linux-amd64/mc" -o /usr/local/bin/mc
  chmod +x /usr/local/bin/mc
fi

echo "==> Configuring mc alias..."
mc alias set "${MC_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

# ─── Create buckets ───────────────────────────────────────────────────────────
echo "==> Creating buckets..."

BUCKETS=("bronze" "silver" "gold" "data-quality" "quarantine")

for BUCKET in "${BUCKETS[@]}"; do
  if mc ls "${MC_ALIAS}/${BUCKET}" > /dev/null 2>&1; then
    echo "    Bucket '${BUCKET}' already exists, skipping."
  else
    mc mb "${MC_ALIAS}/${BUCKET}"
    echo "    Created bucket: ${BUCKET}"
  fi
done

# ─── Set versioning on bronze (immutable audit trail) ─────────────────────────
echo "==> Enabling versioning on bronze bucket..."
mc version enable "${MC_ALIAS}/bronze"

# ─── Set lifecycle policy on quarantine (auto-delete after 30 days) ──────────
echo "==> Setting lifecycle policy on quarantine bucket..."
mc ilm rule add --expire-days 30 "${MC_ALIAS}/quarantine"

# ─── Create read-write policy for pipeline user ───────────────────────────────
echo "==> Creating pipeline IAM policy..."
cat > /tmp/pipeline-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::bronze/*",
        "arn:aws:s3:::bronze",
        "arn:aws:s3:::silver/*",
        "arn:aws:s3:::silver",
        "arn:aws:s3:::gold/*",
        "arn:aws:s3:::gold",
        "arn:aws:s3:::data-quality/*",
        "arn:aws:s3:::data-quality",
        "arn:aws:s3:::quarantine/*",
        "arn:aws:s3:::quarantine"
      ]
    }
  ]
}
EOF

mc admin policy create "${MC_ALIAS}" pipeline-rw /tmp/pipeline-policy.json 2>/dev/null || \
  echo "    Policy 'pipeline-rw' already exists, skipping."

echo ""
echo "==> MinIO initialization complete."
echo "    Buckets: ${BUCKETS[*]}"
echo "    Console: ${MINIO_ENDPOINT/9000/9001} (or http://localhost:9001)"
