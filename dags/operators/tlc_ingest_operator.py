"""
TLCIngestOperator
Downloads NYC TLC Parquet files from the public CloudFront distribution
and uploads them to the MinIO Bronze bucket using multipart streaming.

Key design decisions:
- Stream directly to MinIO (no local disk needed)
- Idempotent: checks if file already exists before downloading
- Retry-safe: partial uploads are cleaned up on failure
- Emits structured log lines for observability
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING

import boto3
import requests
import structlog
import yaml
from airflow.models import BaseOperator
from botocore.config import Config

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = structlog.get_logger(__name__)


def _load_config() -> dict:
    config_path = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/airflow/config/pipeline_config.yml")
    with open(config_path) as f:
        return yaml.safe_load(f)


class TLCIngestOperator(BaseOperator):
    """
    Downloads a single month of NYC Yellow Taxi Parquet data from the TLC
    public dataset and uploads it to MinIO Bronze bucket.

    Args:
        year: 4-digit year as int (e.g. 2024)
        month: 1-12 month as int (e.g. 1 for January)
    """

    template_fields = ("year", "month")

    def __init__(self, year: int, month: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.year = year
        self.month = month

    def execute(self, context: Context) -> str:
        cfg = _load_config()
        storage = cfg["storage"]
        source = cfg["source"]

        month_str = f"{int(self.month):02d}"
        year_str = str(int(self.year))
        filename = f"yellow_tripdata_{year_str}-{month_str}.parquet"
        url = f"{source['tlc_base_url']}/{filename}"
        s3_key = f"{storage['paths']['bronze_yellow_taxi']}/year={year_str}/month={month_str}/{filename}"
        bucket = storage["buckets"]["bronze"]

        log = logger.bind(
            pipeline_layer="bronze",
            dag_run_id=context["run_id"],
            year=year_str,
            month=month_str,
            s3_key=s3_key,
        )

        s3_client = self._get_s3_client(storage["endpoint"])

        # ── Idempotency check ────────────────────────────────────────────────
        if self._object_exists(s3_client, bucket, s3_key):
            log.info("bronze_file_already_exists", action="skip")
            return f"s3://{bucket}/{s3_key}"

        log.info("bronze_download_start", url=url)

        # ── Stream download → MinIO multipart upload ─────────────────────────
        start_ts = datetime.utcnow()
        bytes_uploaded = self._stream_to_minio(s3_client, url, bucket, s3_key, log)
        duration = (datetime.utcnow() - start_ts).total_seconds()

        log.info(
            "bronze_download_complete",
            bytes_uploaded=bytes_uploaded,
            duration_seconds=round(duration, 2),
        )

        return f"s3://{bucket}/{s3_key}"

    def _get_s3_client(self, endpoint: str):
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )

    @staticmethod
    def _object_exists(s3_client, bucket: str, key: str) -> bool:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False
        except Exception:
            return False

    def _stream_to_minio(
        self, s3_client, url: str, bucket: str, key: str, log
    ) -> int:
        """Streams HTTP response body directly into a MinIO multipart upload."""
        chunk_size = 64 * 1024 * 1024  # 64 MB parts

        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu["UploadId"]
        parts = []
        part_number = 1
        buffer = b""
        total_bytes = 0

        try:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB read chunks
                buffer += chunk
                total_bytes += len(chunk)

                if len(buffer) >= chunk_size:
                    part = s3_client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer,
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    log.debug("multipart_part_uploaded", part_number=part_number, buffer_mb=len(buffer) // 1024 // 1024)
                    part_number += 1
                    buffer = b""

            # Upload final part
            if buffer:
                part = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise

        return total_bytes
