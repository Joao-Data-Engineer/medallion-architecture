"""
test_bronze_to_silver_pipeline.py

Integration test for the full Bronze → Silver transformation.
Writes a synthetic Bronze Parquet fixture to a local temp path,
runs the full transformer pipeline end-to-end, and asserts the
Silver Delta output is correct.

Requires: local PySpark (no MinIO or cluster needed).
Mark: integration (skipped in unit-only CI runs).
"""

import pytest
from pyspark.sql import Row

from spark_jobs.bronze_to_silver.schema_definitions import BRONZE_YELLOW_TAXI_SCHEMA
from spark_jobs.bronze_to_silver.yellow_taxi_transformer import (
    add_derived_columns,
    cast_and_validate,
    enrich_with_zones,
    remove_outliers,
)

MOCK_CONFIG = {
    "silver": {
        "outliers": {
            "min_fare_amount": 0.0,
            "max_fare_amount": 500.0,
            "max_trip_distance_miles": 150.0,
            "min_passenger_count": 1,
            "max_passenger_count": 8,
            "min_trip_duration_minutes": 1.0,
            "max_trip_duration_minutes": 300.0,
        },
        "partition_columns": ["year", "month", "pickup_borough"],
        "z_order_columns": ["PULocationID", "DOLocationID"],
    }
}


@pytest.mark.integration
class TestBronzeToSilverPipeline:
    def test_full_pipeline_produces_silver_delta_table(self, spark, tmp_path, sample_bronze_rows):
        """
        End-to-end test: Bronze rows → full transform pipeline → Silver Delta table.
        Verifies row count, schema, and partitioning.
        """

        # Create zones fixture
        zones_data = [
            Row(
                LocationID=161,
                Borough="Manhattan",
                Zone="Midtown Center",
                service_zone="Yellow Zone",
            ),
            Row(LocationID=132, Borough="Queens", Zone="JFK Airport", service_zone="Airports"),
            Row(
                LocationID=236,
                Borough="Manhattan",
                Zone="Upper East Side South",
                service_zone="Yellow Zone",
            ),
        ]
        zones_df = spark.createDataFrame(zones_data)

        # Run full pipeline
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        cleaned = remove_outliers(validated, MOCK_CONFIG)
        enriched = enrich_with_zones(cleaned, zones_df)
        final = add_derived_columns(enriched, year=2024, month=1)

        # Write to local Delta table
        silver_path = f"file://{tmp_path}/silver/yellow_taxi"
        final.write.format("delta").mode("overwrite").save(silver_path)

        # Read back and assert
        result = spark.read.format("delta").load(silver_path)
        assert result.count() == len(sample_bronze_rows)
        assert "pickup_borough" in result.columns
        assert "trip_duration_minutes" in result.columns
        assert "is_airport_trip" in result.columns
        assert "year" in result.columns
        assert result.filter(result["year"] != 2024).count() == 0

    def test_pipeline_is_idempotent(self, spark, tmp_path, sample_bronze_rows):
        """Running the pipeline twice produces the same number of rows (Delta MERGE)."""
        from spark_jobs.utils.delta_utils import get_or_create_delta_table

        zones_data = [
            Row(
                LocationID=161,
                Borough="Manhattan",
                Zone="Midtown Center",
                service_zone="Yellow Zone",
            ),
            Row(LocationID=132, Borough="Queens", Zone="JFK Airport", service_zone="Airports"),
            Row(
                LocationID=236,
                Borough="Manhattan",
                Zone="Upper East Side South",
                service_zone="Yellow Zone",
            ),
        ]
        zones_df = spark.createDataFrame(zones_data)

        silver_path = f"file://{tmp_path}/silver/idempotent_test"

        def run_pipeline():
            df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
            validated = cast_and_validate(df)
            cleaned = remove_outliers(validated, MOCK_CONFIG)
            enriched = enrich_with_zones(cleaned, zones_df)
            final = add_derived_columns(enriched, year=2024, month=1)

            delta_table = get_or_create_delta_table(
                spark, final.schema, silver_path, partition_cols=["year", "month", "pickup_borough"]
            )
            merge_condition = """
                target.pickup_datetime = source.pickup_datetime
                AND target.PULocationID = source.PULocationID
                AND target.DOLocationID = source.DOLocationID
                AND target.fare_amount = source.fare_amount
            """
            (
                delta_table.alias("target")
                .merge(final.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return spark.read.format("delta").load(silver_path).count()

        count_run1 = run_pipeline()
        count_run2 = run_pipeline()
        assert (
            count_run1 == count_run2
        ), f"Pipeline is not idempotent: run 1 = {count_run1}, run 2 = {count_run2}"

    def test_outlier_rows_not_in_silver(
        self, spark, tmp_path, sample_bronze_rows, sample_outlier_rows
    ):
        """Outlier rows from Bronze must not appear in Silver."""
        zones_data = [
            Row(
                LocationID=161,
                Borough="Manhattan",
                Zone="Midtown Center",
                service_zone="Yellow Zone",
            ),
            Row(LocationID=132, Borough="Queens", Zone="JFK Airport", service_zone="Airports"),
            Row(
                LocationID=236,
                Borough="Manhattan",
                Zone="Upper East Side South",
                service_zone="Yellow Zone",
            ),
        ]
        zones_df = spark.createDataFrame(zones_data)

        all_rows = sample_bronze_rows + sample_outlier_rows
        df = spark.createDataFrame(all_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        cleaned = remove_outliers(validated, MOCK_CONFIG)
        enriched = enrich_with_zones(cleaned, zones_df)
        final = add_derived_columns(enriched, year=2024, month=1)

        # Only valid rows should survive
        assert final.count() == len(sample_bronze_rows)
        assert final.filter(final["fare_amount"] < 0).count() == 0
        assert final.filter(final["fare_amount"] > 500).count() == 0
