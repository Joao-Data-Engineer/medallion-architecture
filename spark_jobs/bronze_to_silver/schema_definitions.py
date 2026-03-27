"""
schema_definitions.py

Explicit schema definitions for Bronze source and Silver target.
Using StructType instead of schema inference prevents silent data corruption
when the TLC source changes column types across years.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Bronze Source Schema (NYC TLC Yellow Taxi 2022+) ─────────────────────────
# This is the schema as published by TLC. If TLC changes the schema,
# the job fails at read time with a clear error — not silently downstream.
BRONZE_YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), nullable=True),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=False),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("passenger_count", DoubleType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", DoubleType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=False),
    StructField("DOLocationID", IntegerType(), nullable=False),
    StructField("payment_type", LongType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=False),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("Airport_fee", DoubleType(), nullable=True),
])

# ─── Taxi Zone Lookup Schema ──────────────────────────────────────────────────
TAXI_ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), nullable=False),
    StructField("Borough", StringType(), nullable=True),
    StructField("Zone", StringType(), nullable=True),
    StructField("service_zone", StringType(), nullable=True),
])

# ─── Silver Target Schema ─────────────────────────────────────────────────────
# Cleaned, enriched, renamed to snake_case with derived columns added.
SILVER_YELLOW_TAXI_SCHEMA = StructType([
    # Original identifiers (renamed)
    StructField("vendor_id", IntegerType(), nullable=True),
    StructField("pickup_datetime", TimestampType(), nullable=False),
    StructField("dropoff_datetime", TimestampType(), nullable=False),
    StructField("passenger_count", IntegerType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=False),
    StructField("rate_code_id", IntegerType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=False),
    StructField("DOLocationID", IntegerType(), nullable=False),
    StructField("payment_type", IntegerType(), nullable=True),
    # Fare breakdown
    StructField("fare_amount", DoubleType(), nullable=False),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True),
    # Zone enrichment (from lookup join)
    StructField("pickup_borough", StringType(), nullable=False),
    StructField("pickup_zone", StringType(), nullable=True),
    StructField("dropoff_borough", StringType(), nullable=False),
    StructField("dropoff_zone", StringType(), nullable=True),
    # Derived columns
    StructField("trip_duration_minutes", DoubleType(), nullable=False),
    StructField("speed_mph", DoubleType(), nullable=True),
    StructField("fare_per_mile", DoubleType(), nullable=True),
    StructField("is_airport_trip", IntegerType(), nullable=False),
    StructField("tip_percentage", DoubleType(), nullable=True),
    # Partition columns
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
])

# Airport zone IDs in the NYC TLC dataset
AIRPORT_LOCATION_IDS = {
    1,    # EWR (Newark)
    132,  # JFK Airport
    138,  # LaGuardia Airport
}
