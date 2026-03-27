-- fct_hourly_zone_revenue.sql
-- Business question: Which pickup zones generate the most revenue per hour of day?
-- Grain: one row per (pickup_zone, hour_of_day, year, month)

with trips as (
    select * from {{ ref('int_trips_enriched') }}
)

select
    pickup_zone,
    pickup_borough,
    pickup_location_id,
    pickup_hour                                     as hour_of_day,
    time_of_day_bucket,
    year,
    month,

    -- Volume
    count(*)                                        as trip_count,
    sum(passenger_count)                            as total_passengers,

    -- Revenue
    round(sum(total_revenue), 2)                    as total_revenue,
    round(sum(base_revenue), 2)                     as base_revenue,
    round(avg(total_revenue), 2)                    as avg_revenue_per_trip,
    round(sum(tip_amount), 2)                       as total_tips,

    -- Trip characteristics
    round(avg(trip_distance), 2)                    as avg_trip_distance_miles,
    round(avg(trip_duration_minutes), 2)            as avg_trip_duration_minutes,
    round(avg(fare_per_mile), 2)                    as avg_fare_per_mile,

    -- Airport trips
    sum(is_airport_trip)                            as airport_trip_count,
    round(
        sum(is_airport_trip) * 100.0 / count(*), 2
    )                                               as airport_trip_pct

from trips
where pickup_zone is not null
group by
    pickup_zone,
    pickup_borough,
    pickup_location_id,
    pickup_hour,
    time_of_day_bucket,
    year,
    month
