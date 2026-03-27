"""
yellow_taxi_transformer.py

PySpark Bronze → Silver transformation for NYC Yellow Taxi data.

Pipeline:
  1. Read Bronze Parquet with explicit schema (fail-fast on schema drift)
  2. Cast and validate columns
  3. Remove outliers (configurable thresholds from pipeline_config.yml)
  4. Join with Taxi Zone lookup (broadcast join - zones CSV is tiny)
  5. Compute derived columns (duration, speed, fare_per_mile, etc.)
  6. Write to Silver as Delta Lake using MERGE for idempotency

Usage:
  spark-submit yellow_taxi_transformer.py --year 2024 --month 1
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import yaml
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Add parent directories to path when running as spark-submit job
sys.path.insert(0, "/opt/spark/work-dir")

from spark_jobs.bronze_to_silver.schema_definitions import (
    AIRPORT_LOCATION_IDS,
    BRONZE_YELLOW_TAXI_SCHEMA,
    TAXI_ZONE_SCHEMA,
)
from spark_jobs.utils.delta_utils import get_or_create_delta_table, optimize_delta_table
from spark_jobs.utils.metrics import PipelineMetrics
from spark_jobs.utils.minio_client import get_s3a_conf


def load_config() -> dict:
    config_path = os.environ.get("PIPELINE_CONFIG_PATH", "/opt/spark/work-dir/config/pipeline_config.yml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_spark_session(cfg: dict) -> SparkSession:
    spark_cfg = cfg["spark"]
    s3a = spark_cfg["s3a"]

    builder = (
        SparkSession.builder.appName(spark_cfg["app_name"])
        .config("spark.sql.extensions", spark_cfg["delta"]["extensions"])
        .config("spark.sql.catalog.spark_catalog", spark_cfg["delta"]["catalog_impl"])
        # MinIO S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", s3a["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", str(s3a["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3a["connection_ssl_enabled"]).lower())
        .config("spark.hadoop.fs.s3a.impl", s3a["impl"])
        .config("spark.hadoop.fs.s3a.fast.upload", str(s3a["fast_upload"]).lower())
        # Delta Lake commit protocol
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ─── Step 1: Read Bronze ──────────────────────────────────────────────────────

def read_bronze(spark: SparkSession, cfg: dict, year: int, month: int) -> DataFrame:
    storage = cfg["storage"]
    month_str = f"{month:02d}"
    path = (
        f"s3a://{storage['buckets']['bronze']}/"
        f"{storage['paths']['bronze_yellow_taxi']}/"
        f"year={year}/month={month_str}/"
    )
    return spark.read.schema(BRONZE_YELLOW_TAXI_SCHEMA).parquet(path)


# ─── Step 2: Cast & Validate ──────────────────────────────────────────────────

def cast_and_validate(df: DataFrame) -> DataFrame:
    return (
        df
        # Rename to snake_case
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("Airport_fee", "airport_fee")
        # Cast to correct types
        .withColumn("vendor_id", F.col("vendor_id").cast(IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("rate_code_id", F.col("rate_code_id").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        # Drop rows where core columns are null (non-recoverable)
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
            & F.col("PULocationID").isNotNull()
            & F.col("DOLocationID").isNotNull()
            & F.col("fare_amount").isNotNull()
        )
        # Ensure dropoff > pickup (data quality issue in raw data)
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
    )


# ─── Step 3: Remove Outliers ──────────────────────────────────────────────────

def remove_outliers(df: DataFrame, cfg: dict) -> DataFrame:
    outliers = cfg["silver"]["outliers"]
    return (
        df
        .filter(F.col("fare_amount").between(
            outliers["min_fare_amount"], outliers["max_fare_amount"]
        ))
        .filter(F.col("trip_distance").between(0.0, outliers["max_trip_distance_miles"]))
        .filter(
            F.col("passenger_count").isNull()
            | F.col("passenger_count").between(
                outliers["min_passenger_count"], outliers["max_passenger_count"]
            )
        )
    )


# ─── Step 4: Zone Lookup Join ─────────────────────────────────────────────────

def load_zone_lookup(spark: SparkSession, cfg: dict) -> DataFrame:
    storage = cfg["storage"]
    path = f"s3a://{storage['buckets']['bronze']}/{storage['paths']['bronze_taxi_zones']}"
    return spark.read.schema(TAXI_ZONE_SCHEMA).option("header", "true").csv(path)


def enrich_with_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    # Broadcast the zones DataFrame (it's only ~265 rows)
    zones_broadcast = F.broadcast(zones)

    pickup_zones = zones_broadcast.select(
        F.col("LocationID").alias("pu_loc_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    dropoff_zones = zones_broadcast.select(
        F.col("LocationID").alias("do_loc_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )

    return (
        df
        .join(pickup_zones, df["PULocationID"] == pickup_zones["pu_loc_id"], "left")
        .join(dropoff_zones, df["DOLocationID"] == dropoff_zones["do_loc_id"], "left")
        .drop("pu_loc_id", "do_loc_id")
        # Fill unknown zones (location IDs not in lookup)
        .withColumn("pickup_borough", F.coalesce(F.col("pickup_borough"), F.lit("Unknown")))
        .withColumn("pickup_zone", F.coalesce(F.col("pickup_zone"), F.lit("Unknown")))
        .withColumn("dropoff_borough", F.coalesce(F.col("dropoff_borough"), F.lit("Unknown")))
        .withColumn("dropoff_zone", F.coalesce(F.col("dropoff_zone"), F.lit("Unknown")))
    )


# ─── Step 5: Derived Columns ──────────────────────────────────────────────────

def add_derived_columns(df: DataFrame, year: int, month: int) -> DataFrame:
    airport_ids = list(AIRPORT_LOCATION_IDS)

    duration_col = (
        F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")
    ) / 60.0  # seconds → minutes

    return (
        df
        # Trip duration in minutes
        .withColumn("trip_duration_minutes", F.round(duration_col, 2))
        # Filter after deriving (needs duration > 0 after zone join)
        .filter(F.col("trip_duration_minutes").between(1.0, 300.0))
        # Speed (miles per hour)
        .withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0), 2),
            ).otherwise(F.lit(None)),
        )
        # Fare per mile
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 2),
            ).otherwise(F.lit(None)),
        )
        # Is airport trip (pickup or dropoff at JFK, LGA, EWR)
        .withColumn(
            "is_airport_trip",
            F.when(
                F.col("PULocationID").isin(airport_ids) | F.col("DOLocationID").isin(airport_ids),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        # Tip percentage of fare
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2),
            ).otherwise(F.lit(None)),
        )
        # Partition columns
        .withColumn("year", F.lit(year).cast(IntegerType()))
        .withColumn("month", F.lit(month).cast(IntegerType()))
    )


# ─── Step 6: Delta MERGE (idempotent write) ───────────────────────────────────

def write_to_silver(spark: SparkSession, df: DataFrame, cfg: dict) -> int:
    storage = cfg["storage"]
    silver_path = (
        f"s3a://{storage['buckets']['silver']}/"
        f"{storage['paths']['silver_yellow_taxi']}"
    )
    partition_cols = cfg["silver"]["partition_columns"]

    delta_table = get_or_create_delta_table(spark, df.schema, silver_path, partition_cols)

    # Surrogate merge key: combination of datetime + location IDs + fare
    # This covers the uniqueness of a trip record without a natural PK
    merge_condition = """
        target.pickup_datetime = source.pickup_datetime
        AND target.PULocationID = source.PULocationID
        AND target.DOLocationID = source.DOLocationID
        AND target.fare_amount = source.fare_amount
        AND target.year = source.year
        AND target.month = source.month
    """

    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Run OPTIMIZE + ZORDER after every write to keep file sizes healthy
    z_order_cols = cfg["silver"]["z_order_columns"]
    optimize_delta_table(spark, silver_path, z_order_cols)

    return df.count()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(year: int, month: int) -> None:
    cfg = load_config()
    metrics = PipelineMetrics(layer="silver", year=year, month=month)

    spark = build_spark_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    start_ts = datetime.utcnow()

    # Read
    metrics.log("silver_read_start")
    raw_df = read_bronze(spark, cfg, year, month)
    rows_in = raw_df.count()
    metrics.log("silver_read_complete", rows_in=rows_in)

    # Transform
    validated_df = cast_and_validate(raw_df)
    clean_df = remove_outliers(validated_df, cfg)

    zones_df = load_zone_lookup(spark, cfg)
    enriched_df = enrich_with_zones(clean_df, zones_df)
    final_df = add_derived_columns(enriched_df, year, month)

    # Write
    metrics.log("silver_write_start")
    rows_out = write_to_silver(spark, final_df, cfg)

    duration = (datetime.utcnow() - start_ts).total_seconds()
    rows_dropped = rows_in - rows_out

    metrics.log(
        "silver_transform_complete",
        rows_in=rows_in,
        rows_out=rows_out,
        rows_dropped=rows_dropped,
        duration_seconds=round(duration, 2),
        drop_rate_pct=round((rows_dropped / rows_in * 100) if rows_in > 0 else 0, 2),
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver transformer for NYC Yellow Taxi")
    parser.add_argument("--year", type=int, required=True, help="4-digit year (e.g. 2024)")
    parser.add_argument("--month", type=int, required=True, help="Month 1-12")
    args = parser.parse_args()
    main(args.year, args.month)
