"""
conftest.py - Shared pytest fixtures for the Medallion Architecture test suite.
"""

import os
from datetime import datetime

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Local SparkSession for unit and integration tests.
    Uses local[2] master — no cluster needed.
    Delta Lake configured in local mode.
    """
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("medallion-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")  # Fast for small test data
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_bronze_rows():
    """Minimal valid Bronze Yellow Taxi rows for testing transformations."""
    return [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 30, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 55, 0),
            "passenger_count": 2.0,
            "trip_distance": 5.3,
            "RatecodeID": 1.0,
            "store_and_fwd_flag": "N",
            "PULocationID": 161,   # Midtown Center (Manhattan)
            "DOLocationID": 236,   # Upper East Side South
            "payment_type": 1,
            "fare_amount": 18.50,
            "extra": 1.0,
            "mta_tax": 0.5,
            "tip_amount": 4.00,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": 25.00,
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        },
        {
            "VendorID": 2,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 18, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 18, 45, 0),
            "passenger_count": 1.0,
            "trip_distance": 12.1,
            "RatecodeID": 2.0,
            "store_and_fwd_flag": "N",
            "PULocationID": 132,   # JFK Airport
            "DOLocationID": 161,   # Midtown Center
            "payment_type": 1,
            "fare_amount": 70.00,
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 15.00,
            "tolls_amount": 6.55,
            "improvement_surcharge": 1.0,
            "total_amount": 93.05,
            "congestion_surcharge": 2.5,
            "Airport_fee": 1.75,
        },
    ]


@pytest.fixture(scope="session")
def sample_outlier_rows():
    """Rows that should be filtered out as outliers."""
    return [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 5, 0),
            "passenger_count": 1.0,
            "trip_distance": 0.1,
            "RatecodeID": 1.0,
            "store_and_fwd_flag": "N",
            "PULocationID": 161,
            "DOLocationID": 236,
            "payment_type": 2,
            "fare_amount": -50.0,  # NEGATIVE FARE - should be filtered
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": -49.0,
            "congestion_surcharge": 0.0,
            "Airport_fee": 0.0,
        },
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 6, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 12, 0, 0),  # 6 HOURS - should be filtered
            "passenger_count": 1.0,
            "trip_distance": 200.0,  # 200 miles - should be filtered
            "RatecodeID": 1.0,
            "store_and_fwd_flag": "N",
            "PULocationID": 161,
            "DOLocationID": 236,
            "payment_type": 1,
            "fare_amount": 600.0,  # > 500 - should be filtered
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": 601.5,
            "congestion_surcharge": 0.0,
            "Airport_fee": 0.0,
        },
    ]
