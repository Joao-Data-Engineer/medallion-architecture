-- dim_taxi_zones.sql
-- Dimension table: cleaned Taxi Zone reference data.
-- 265 NYC TLC zones with borough and service zone classification.

with zones as (
    select * from {{ ref('stg_taxi_zones') }}
)

select
    location_id,
    borough,
    zone_name,
    service_zone,

    -- Flag for airport zones (used in fact table filters)
    case
        when location_id in (1, 132, 138) then true
        else false
    end as is_airport_zone,

    -- Borough grouping for high-level analysis
    case
        when borough = 'Manhattan' then 'Inner'
        when borough in ('Brooklyn', 'Queens') then 'Outer Core'
        when borough in ('Bronx', 'Staten Island') then 'Outer'
        when borough = 'EWR' then 'Airport'
        else 'Unknown'
    end as borough_group

from zones
order by location_id
