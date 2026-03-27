"""
test_delta_utils.py

Unit tests for Delta Lake utility functions.
Uses a local temp directory to test table creation and idempotency.
"""

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.mark.unit
class TestGetOrCreateDeltaTable:
    SCHEMA = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )

    def test_creates_delta_table_when_not_exists(self, spark, tmp_path):
        from spark_jobs.utils.delta_utils import get_or_create_delta_table

        path = f"file://{tmp_path}/test_table"
        table = get_or_create_delta_table(spark, self.SCHEMA, path, partition_cols=[])
        assert table is not None

    def test_returns_existing_delta_table(self, spark, tmp_path):
        from spark_jobs.utils.delta_utils import get_or_create_delta_table

        path = f"file://{tmp_path}/test_table_existing"
        # Create once
        get_or_create_delta_table(spark, self.SCHEMA, path, partition_cols=[])
        # Call again — should not raise, should return existing
        table = get_or_create_delta_table(spark, self.SCHEMA, path, partition_cols=[])
        assert table is not None

    def test_created_table_is_readable(self, spark, tmp_path):
        from spark_jobs.utils.delta_utils import get_or_create_delta_table

        path = f"file://{tmp_path}/test_readable"
        get_or_create_delta_table(spark, self.SCHEMA, path, partition_cols=[])
        df = spark.read.format("delta").load(path)
        # Delta Lake may adjust nullable flags; compare column names and types only
        assert {f.name: f.dataType for f in df.schema} == {f.name: f.dataType for f in self.SCHEMA}


@pytest.mark.unit
class TestTimeTravelRead:
    def test_reads_at_version_zero(self, spark, tmp_path):
        from spark_jobs.utils.delta_utils import time_travel_read

        path = f"file://{tmp_path}/tt_table"
        schema = StructType([StructField("val", IntegerType(), True)])

        # Write version 0
        spark.createDataFrame([(1,), (2,)], schema=schema).write.format("delta").save(path)
        # Write version 1
        spark.createDataFrame([(3,), (4,)], schema=schema).write.format("delta").mode(
            "append"
        ).save(path)

        v0 = time_travel_read(spark, path, version=0)
        assert v0.count() == 2
