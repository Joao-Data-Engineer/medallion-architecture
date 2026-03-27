"""
superset_config.py
Apache Superset configuration for the Medallion Architecture project.
DuckDB reads Gold Delta tables directly from MinIO via S3 endpoint.
"""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "your-secret-key-here")

# Allow DuckDB as a database engine
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# DuckDB connection for Gold Delta tables
# Superset users should add this as a database connection:
#   Name: Gold Delta (DuckDB)
#   SQLAlchemy URI: duckdb:///:memory:
# Then run the INSTALL/LOAD extensions via the SQL editor:
#   INSTALL delta; LOAD delta;
#   INSTALL httpfs; LOAD httpfs;
#   SET s3_endpoint='minio:9000';
#   SET s3_access_key_id='minioadmin';
#   SET s3_secret_access_key='minioadmin123';
#   SET s3_use_ssl=false;
#   SET s3_url_style='path';

# Example queries for dashboards:
EXAMPLE_QUERIES = {
    "revenue_heatmap": """
        SELECT
            pickup_zone,
            pickup_borough,
            hour_of_day,
            SUM(total_revenue) as revenue,
            SUM(trip_count) as trips
        FROM delta_scan('s3://gold/fct_hourly_zone_revenue/')
        GROUP BY pickup_zone, pickup_borough, hour_of_day
        ORDER BY revenue DESC
    """,
    "daily_demand": """
        SELECT
            pickup_borough,
            pickup_date,
            trip_count,
            trip_count_7d_avg,
            total_revenue
        FROM delta_scan('s3://gold/fct_daily_borough_demand/')
        ORDER BY pickup_date DESC, pickup_borough
    """,
    "payment_analysis": """
        SELECT
            payment_type_label,
            pickup_borough,
            time_of_day_bucket,
            avg_tip_pct,
            trip_count,
            total_revenue
        FROM delta_scan('s3://gold/mart_payment_analysis/')
        ORDER BY trip_count DESC
    """,
}
