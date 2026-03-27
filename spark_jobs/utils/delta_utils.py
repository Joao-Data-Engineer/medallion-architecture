"""
delta_utils.py

Helpers for Delta Lake table lifecycle management.
Centralizes create/optimize/vacuum operations so callers don't
need to know the Delta API details.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_or_create_delta_table(
    spark: SparkSession,
    schema: StructType,
    path: str,
    partition_cols: list[str],
):
    """
    Returns a DeltaTable at `path`. If it doesn't exist, creates it
    with the given schema and partition columns.

    Args:
        spark: Active SparkSession
        schema: Target schema for the Delta table
        path: S3A path (e.g. s3a://silver/yellow_taxi)
        partition_cols: List of column names to partition by
    """
    from delta import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        return DeltaTable.forPath(spark, path)

    # Create empty Delta table with schema and partition spec
    (
        spark.createDataFrame([], schema)
        .write.format("delta")
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .save(path)
    )
    return DeltaTable.forPath(spark, path)


def optimize_delta_table(
    spark: SparkSession, path: str, z_order_cols: list[str] | None = None
) -> None:
    """
    Runs OPTIMIZE on a Delta table to compact small files.
    Optionally applies ZORDER to co-locate data for common query filters.

    Args:
        spark: Active SparkSession
        path: S3A path to the Delta table
        z_order_cols: Columns to ZORDER by (e.g. ["PULocationID", "DOLocationID"])
    """
    if z_order_cols:
        cols = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Removes old Delta Lake files beyond the retention window.
    Default: 7 days (168 hours).

    Args:
        spark: Active SparkSession
        path: S3A path to the Delta table
        retention_hours: Files older than this are deleted (default 168h = 7 days)
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def get_delta_history(spark: SparkSession, path: str, limit: int = 10) -> None:
    """Prints the last N transactions in a Delta table's transaction log."""
    from delta import DeltaTable

    history = DeltaTable.forPath(spark, path).history(limit)
    history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)


def time_travel_read(spark, path: str, version: int | None = None, timestamp: str | None = None):
    """
    Reads a Delta table at a specific version or timestamp (time travel).

    Args:
        spark: Active SparkSession
        path: S3A path to the Delta table
        version: Delta version number (int)
        timestamp: ISO timestamp string (e.g. "2024-02-01T00:00:00")
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(path)
