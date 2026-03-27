"""
minio_client.py

Boto3 S3 client factory for MinIO, and helper utilities for
listing/checking objects in the medallion buckets.
"""

from __future__ import annotations

import os

import boto3
from botocore.config import Config


def get_minio_client(endpoint: str | None = None):  # pragma: no cover
    """Returns a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint or os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def object_exists(bucket: str, key: str, endpoint: str | None = None) -> bool:  # pragma: no cover
    """Returns True if the given S3 key exists in MinIO."""
    client = get_minio_client(endpoint)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def list_objects(
    bucket: str, prefix: str, endpoint: str | None = None
) -> list[str]:  # pragma: no cover
    """Returns all object keys in a bucket under the given prefix."""
    client = get_minio_client(endpoint)
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def get_s3a_conf(endpoint: str | None = None) -> dict[str, str]:  # pragma: no cover
    """
    Returns a dict of Hadoop S3A configuration for use with SparkConf.
    Useful for testing or dynamic session creation.
    """
    ep = endpoint or os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return {
        "spark.hadoop.fs.s3a.endpoint": ep,
        "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
