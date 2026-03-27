-- stg_yellow_taxi.sql
-- Thin staging model over Silver yellow_taxi.
-- Only renames and light casts — no business logic here.

with source as (
    select * from {{ source('silver', 'yellow_taxi') }}
),

renamed as (
    select
        -- Timestamps
        pickup_datetime,
        dropoff_datetime,

        -- Location IDs (TLC zone integers)
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,

        -- Zone enrichment
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,

        -- Fare breakdown (USD)
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,

        -- Trip metrics
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        speed_mph,
        fare_per_mile,
        tip_percentage,

        -- Flags
        is_airport_trip,
        payment_type,
        vendor_id,

        -- Partition keys
        year,
        month,

        -- Derived time parts (used in downstream models)
        hour(pickup_datetime)                       as pickup_hour,
        dayofweek(pickup_datetime)                  as pickup_day_of_week,
        date(pickup_datetime)                       as pickup_date,
        {{ time_bucket('hour(pickup_datetime)') }}  as time_of_day_bucket

    from source
)

select * from renamed
