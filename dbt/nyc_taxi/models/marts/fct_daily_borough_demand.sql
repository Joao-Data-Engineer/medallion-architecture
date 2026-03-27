-- fct_daily_borough_demand.sql
-- Business question: How does daily demand vary by borough over time?
-- Grain: one row per (pickup_borough, pickup_date)
-- Includes 7-day rolling average for trend analysis.

with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

daily_agg as (
    select
        pickup_borough,
        pickup_date,
        year,
        month,

        count(*)                                    as trip_count,
        round(sum(total_revenue), 2)                as total_revenue,
        round(avg(trip_duration_minutes), 2)        as avg_trip_duration_minutes,
        round(avg(trip_distance), 2)                as avg_trip_distance_miles,
        round(sum(tip_amount), 2)                   as total_tips,
        sum(passenger_count)                        as total_passengers

    from trips
    group by pickup_borough, pickup_date, year, month
),

with_rolling_avg as (
    select
        *,
        round(
            avg(trip_count) over (
                partition by pickup_borough
                order by pickup_date
                rows between 6 preceding and current row
            ), 2
        ) as trip_count_7d_avg,

        round(
            avg(total_revenue) over (
                partition by pickup_borough
                order by pickup_date
                rows between 6 preceding and current row
            ), 2
        ) as revenue_7d_avg

    from daily_agg
)

select * from with_rolling_avg
order by pickup_date desc, pickup_borough
