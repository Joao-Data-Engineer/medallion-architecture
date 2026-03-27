-- stg_taxi_zones.sql
-- Staging model for the Taxi Zone lookup reference table.

with source as (
    select * from {{ source('silver', 'taxi_zones') }}
)

select
    LocationID    as location_id,
    Borough       as borough,
    Zone          as zone_name,
    service_zone
from source
where LocationID is not null
