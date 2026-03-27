-- mart_payment_analysis.sql
-- Business question: How do tip rates and revenue vary by payment type, borough, and time of day?
-- Grain: one row per (payment_type, pickup_borough, time_of_day_bucket, year, month)

with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

-- Only include credit card trips for tip analysis
-- (cash tips are not recorded in the dataset)
cc_trips as (
    select * from trips where is_credit_card = true
),

payment_agg as (
    select
        payment_type,
        payment_type_label,
        pickup_borough,
        time_of_day_bucket,
        year,
        month,

        count(*)                                        as trip_count,
        round(sum(total_revenue), 2)                    as total_revenue,
        round(avg(total_revenue), 2)                    as avg_revenue_per_trip,
        round(avg(fare_amount), 2)                      as avg_fare,
        round(sum(tip_amount), 2)                       as total_tips,
        round(avg(tip_percentage), 2)                   as avg_tip_pct,
        round(
            percentile_approx(tip_percentage, 0.5), 2
        )                                               as median_tip_pct,
        round(avg(trip_distance), 2)                    as avg_trip_distance_miles,
        sum(is_airport_trip)                            as airport_trip_count

    from trips
    group by
        payment_type,
        payment_type_label,
        pickup_borough,
        time_of_day_bucket,
        year,
        month
)

select * from payment_agg
order by year desc, month desc, pickup_borough, payment_type
