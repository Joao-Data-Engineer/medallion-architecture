"""
test_yellow_taxi_transformer.py

Unit tests for the Bronze → Silver transformation functions.
Uses a local Spark session with synthetic data — no MinIO or external services.
"""

import pytest
from pyspark.sql import Row

from spark_jobs.bronze_to_silver.schema_definitions import (
    BRONZE_YELLOW_TAXI_SCHEMA,
)
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
        }
    }
}


@pytest.mark.unit
class TestCastAndValidate:
    def test_renames_columns_to_snake_case(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        result = cast_and_validate(df)
        assert "pickup_datetime" in result.columns
        assert "vendor_id" in result.columns
        assert "tpep_pickup_datetime" not in result.columns
        assert "VendorID" not in result.columns

    def test_drops_null_pickup_datetime(self, spark, sample_bronze_rows):
        rows = sample_bronze_rows.copy()
        rows.append({**rows[0], "tpep_pickup_datetime": None})
        df = spark.createDataFrame(rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        result = cast_and_validate(df)
        assert result.filter(result["pickup_datetime"].isNull()).count() == 0

    def test_drops_rows_where_dropoff_before_pickup(self, spark, sample_bronze_rows):
        from datetime import datetime

        rows = [
            {
                **sample_bronze_rows[0],
                "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
                "tpep_dropoff_datetime": datetime(2024, 1, 15, 9, 0, 0),  # before pickup
            }
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_valid_rows_are_retained(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        result = cast_and_validate(df)
        assert result.count() == len(sample_bronze_rows)


@pytest.mark.unit
class TestRemoveOutliers:
    def test_removes_negative_fares(self, spark, sample_bronze_rows, sample_outlier_rows):
        all_rows = sample_bronze_rows + sample_outlier_rows
        df = spark.createDataFrame(all_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        result = remove_outliers(validated, MOCK_CONFIG)
        # All rows with fare_amount < 0 or > 500 should be removed
        assert result.filter(result["fare_amount"] < 0).count() == 0
        assert result.filter(result["fare_amount"] > 500).count() == 0

    def test_removes_extreme_trip_distance(self, spark, sample_bronze_rows, sample_outlier_rows):
        all_rows = sample_bronze_rows + sample_outlier_rows
        df = spark.createDataFrame(all_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        result = remove_outliers(validated, MOCK_CONFIG)
        assert result.filter(result["trip_distance"] > 150.0).count() == 0

    def test_valid_rows_survive_outlier_removal(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        result = remove_outliers(validated, MOCK_CONFIG)
        assert result.count() == len(sample_bronze_rows)


@pytest.mark.unit
class TestEnrichWithZones:
    def test_adds_borough_columns(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)

        # Create a minimal zones DataFrame
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

        result = enrich_with_zones(validated, zones_df)
        assert "pickup_borough" in result.columns
        assert "dropoff_borough" in result.columns
        assert "pickup_zone" in result.columns
        assert "dropoff_zone" in result.columns

    def test_unknown_zone_gets_unknown_label(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        # Empty zones DataFrame — all locations are unknown
        zones_df = spark.createDataFrame(
            [], "LocationID INT, Borough STRING, Zone STRING, service_zone STRING"
        )

        result = enrich_with_zones(validated, zones_df)
        unknowns = result.filter(result["pickup_borough"] == "Unknown").count()
        assert unknowns == len(sample_bronze_rows)


@pytest.mark.unit
class TestAddDerivedColumns:
    def _get_enriched_df(self, spark, sample_bronze_rows):
        df = spark.createDataFrame(sample_bronze_rows, schema=BRONZE_YELLOW_TAXI_SCHEMA)
        validated = cast_and_validate(df)
        cleaned = remove_outliers(validated, MOCK_CONFIG)
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
        return enrich_with_zones(cleaned, zones_df)

    def test_trip_duration_is_positive(self, spark, sample_bronze_rows):
        enriched = self._get_enriched_df(spark, sample_bronze_rows)
        result = add_derived_columns(enriched, year=2024, month=1)
        assert result.filter(result["trip_duration_minutes"] <= 0).count() == 0

    def test_airport_trip_flag_set_correctly(self, spark, sample_bronze_rows):
        enriched = self._get_enriched_df(spark, sample_bronze_rows)
        result = add_derived_columns(enriched, year=2024, month=1)
        # Row with PULocationID=132 (JFK) must have is_airport_trip=1
        jfk_trips = result.filter(result["PULocationID"] == 132)
        assert jfk_trips.first()["is_airport_trip"] == 1

    def test_non_airport_trip_flag_is_zero(self, spark, sample_bronze_rows):
        enriched = self._get_enriched_df(spark, sample_bronze_rows)
        result = add_derived_columns(enriched, year=2024, month=1)
        # Row with PULocationID=161 (Midtown) is not airport
        non_airport = result.filter(result["PULocationID"] == 161)
        assert non_airport.first()["is_airport_trip"] == 0

    def test_year_month_partition_columns_added(self, spark, sample_bronze_rows):
        enriched = self._get_enriched_df(spark, sample_bronze_rows)
        result = add_derived_columns(enriched, year=2024, month=1)
        assert "year" in result.columns
        assert "month" in result.columns
        assert result.filter(result["year"] != 2024).count() == 0
        assert result.filter(result["month"] != 1).count() == 0

    def test_fare_per_mile_computed_correctly(self, spark, sample_bronze_rows):
        enriched = self._get_enriched_df(spark, sample_bronze_rows)
        result = add_derived_columns(enriched, year=2024, month=1)
        first_row = result.filter(result["PULocationID"] == 161).first()
        expected = round(first_row["fare_amount"] / first_row["trip_distance"], 2)
        assert abs(first_row["fare_per_mile"] - expected) < 0.01
