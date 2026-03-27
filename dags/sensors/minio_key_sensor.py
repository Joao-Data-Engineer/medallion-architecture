"""
MinioKeySensor
Polls MinIO for the existence of a specific S3 key.
Used between Bronze ingestion and Silver transformation to ensure
data is present before starting the Spark job.
"""

from __future__ import annotations

import os

import boto3
import structlog
from airflow.sensors.base import BaseSensorOperator
from botocore.config import Config

logger = structlog.get_logger(__name__)


class MinioKeySensor(BaseSensorOperator):
    """
    Waits until a given key (or key prefix) exists in a MinIO bucket.

    Args:
        bucket: MinIO bucket name
        key: Full object key or prefix to check for existence
        endpoint: MinIO endpoint URL (defaults to MINIO_ENDPOINT env var)
    """

    template_fields = ("bucket", "key")

    def __init__(self, bucket: str, key: str, endpoint: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key
        self.endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

    def poke(self, context) -> bool:
        s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4"),
        )

        log = logger.bind(bucket=self.bucket, key=self.key)

        try:
            # Try exact key first
            s3.head_object(Bucket=self.bucket, Key=self.key)
            log.info("minio_key_found")
            return True
        except Exception:
            pass

        # Try prefix listing (key might be a directory prefix)
        try:
            response = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.key, MaxKeys=1)
            found = bool(response.get("Contents"))
            if found:
                log.info("minio_prefix_found", object_count=len(response.get("Contents", [])))
            else:
                log.debug("minio_key_not_found_yet")
            return found
        except Exception as e:
            log.warning("minio_sensor_error", error=str(e))
            return False
