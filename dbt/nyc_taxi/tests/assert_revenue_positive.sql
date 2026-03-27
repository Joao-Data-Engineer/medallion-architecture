-- Custom test: total_revenue must be >= 0 for all rows in fct_hourly_zone_revenue
-- Returns rows that FAIL the assertion (dbt test passes when this query returns 0 rows)

select *
from {{ ref('fct_hourly_zone_revenue') }}
where total_revenue < 0
