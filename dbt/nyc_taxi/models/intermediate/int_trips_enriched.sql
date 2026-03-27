-- int_trips_enriched.sql
-- Intermediate model: applies final business logic before mart aggregations.
-- Materialized as ephemeral (compiled inline, no table created).

with trips as (
    select * from {{ ref('stg_yellow_taxi') }}
),

-- Payment type labels (TLC codebook)
payment_labeled as (
    select
        *,
        case payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided Trip'
            else 'Other'
        end as payment_type_label,

        -- Revenue = fare + tolls + surcharges (excludes tip — driver-specific)
        fare_amount + coalesce(tolls_amount, 0) +
        coalesce(mta_tax, 0) + coalesce(improvement_surcharge, 0) +
        coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0) as base_revenue,

        -- Total revenue including tip
        fare_amount + coalesce(tip_amount, 0) + coalesce(tolls_amount, 0) +
        coalesce(mta_tax, 0) + coalesce(improvement_surcharge, 0) +
        coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0) as total_revenue,

        -- Is a credit card trip (tip data is only reliable for CC payments)
        case when payment_type = 1 then true else false end as is_credit_card

    from trips
)

select * from payment_labeled
