# Data Dictionary — NYC Taxi Medallion Architecture

## Silver Layer: `yellow_taxi`

Partitioned by: `year` / `month` / `pickup_borough`

| Column | Type | Description | Source |
|---|---|---|---|
| `vendor_id` | INT | Taxi vendor (1=Creative Mobile Tech, 2=VeriFone) | TLC raw |
| `pickup_datetime` | TIMESTAMP | Trip start time | TLC raw (renamed) |
| `dropoff_datetime` | TIMESTAMP | Trip end time | TLC raw (renamed) |
| `passenger_count` | INT | Number of passengers | TLC raw |
| `trip_distance` | DOUBLE | Trip distance in miles | TLC raw |
| `rate_code_id` | INT | Rate type (1=Standard, 2=JFK, 3=Newark, etc.) | TLC raw (renamed) |
| `PULocationID` | INT | TLC pickup zone ID (1-265) | TLC raw |
| `DOLocationID` | INT | TLC dropoff zone ID (1-265) | TLC raw |
| `payment_type` | INT | 1=CC, 2=Cash, 3=No Charge, 4=Dispute, 5=Unknown | TLC raw |
| `fare_amount` | DOUBLE | Meter fare in USD | TLC raw |
| `extra` | DOUBLE | Extras and surcharges (rush hour, overnight) | TLC raw |
| `mta_tax` | DOUBLE | MTA tax ($0.50) | TLC raw |
| `tip_amount` | DOUBLE | Tip (credit card only; cash tips not recorded) | TLC raw |
| `tolls_amount` | DOUBLE | Toll charges | TLC raw |
| `improvement_surcharge` | DOUBLE | Improvement surcharge ($1.00) | TLC raw |
| `total_amount` | DOUBLE | Total charged amount | TLC raw |
| `congestion_surcharge` | DOUBLE | Congestion surcharge ($2.50 in Manhattan core) | TLC raw |
| `airport_fee` | DOUBLE | Airport fee ($1.75 for JFK/LGA pickups) | TLC raw (renamed) |
| `pickup_borough` | STRING | NYC borough name for pickup zone | Zone lookup join |
| `pickup_zone` | STRING | TLC zone name for pickup location | Zone lookup join |
| `dropoff_borough` | STRING | NYC borough name for dropoff zone | Zone lookup join |
| `dropoff_zone` | STRING | TLC zone name for dropoff location | Zone lookup join |
| `trip_duration_minutes` | DOUBLE | `(dropoff - pickup)` in minutes | Derived |
| `speed_mph` | DOUBLE | `trip_distance / (trip_duration_minutes / 60)` | Derived |
| `fare_per_mile` | DOUBLE | `fare_amount / trip_distance` | Derived |
| `is_airport_trip` | INT | 1 if pickup or dropoff at JFK/LGA/EWR, else 0 | Derived |
| `tip_percentage` | DOUBLE | `(tip_amount / fare_amount) * 100` | Derived |
| `year` | INT | Pickup year (partition key) | Derived |
| `month` | INT | Pickup month 1-12 (partition key) | Derived |

---

## Gold Layer: `fct_hourly_zone_revenue`

Grain: one row per (pickup_zone, hour_of_day, year, month)

| Column | Type | Description |
|---|---|---|
| `pickup_zone` | STRING | TLC zone name |
| `pickup_borough` | STRING | NYC borough |
| `pickup_location_id` | INT | TLC zone ID |
| `hour_of_day` | INT | 0-23 |
| `time_of_day_bucket` | STRING | morning_rush / midday / evening_rush / night / overnight |
| `year` | INT | Year |
| `month` | INT | Month |
| `trip_count` | BIGINT | Number of trips |
| `total_passengers` | BIGINT | Sum of passenger counts |
| `total_revenue` | DOUBLE | Sum of total_revenue (fare + tip + surcharges) |
| `base_revenue` | DOUBLE | Sum of revenue excluding tip |
| `avg_revenue_per_trip` | DOUBLE | Average revenue per trip |
| `total_tips` | DOUBLE | Sum of tips |
| `avg_trip_distance_miles` | DOUBLE | Mean trip distance |
| `avg_trip_duration_minutes` | DOUBLE | Mean trip duration |
| `avg_fare_per_mile` | DOUBLE | Mean fare-per-mile ratio |
| `airport_trip_count` | BIGINT | Airport trip count |
| `airport_trip_pct` | DOUBLE | Airport trips as % of total |

---

## Gold Layer: `fct_daily_borough_demand`

Grain: one row per (pickup_borough, pickup_date)

| Column | Type | Description |
|---|---|---|
| `pickup_borough` | STRING | NYC borough |
| `pickup_date` | DATE | Calendar date |
| `year` | INT | Year |
| `month` | INT | Month |
| `trip_count` | BIGINT | Daily trip count |
| `total_revenue` | DOUBLE | Daily revenue |
| `avg_trip_duration_minutes` | DOUBLE | Mean trip duration |
| `avg_trip_distance_miles` | DOUBLE | Mean trip distance |
| `total_tips` | DOUBLE | Daily tip total |
| `total_passengers` | BIGINT | Daily passenger count |
| `trip_count_7d_avg` | DOUBLE | 7-day rolling average of trip_count |
| `revenue_7d_avg` | DOUBLE | 7-day rolling average of revenue |

---

## Gold Layer: `mart_payment_analysis`

Grain: one row per (payment_type, pickup_borough, time_of_day_bucket, year, month)

| Column | Type | Description |
|---|---|---|
| `payment_type` | INT | TLC payment code |
| `payment_type_label` | STRING | Credit Card / Cash / No Charge / etc. |
| `pickup_borough` | STRING | NYC borough |
| `time_of_day_bucket` | STRING | morning_rush / midday / evening_rush / night / overnight |
| `year` | INT | Year |
| `month` | INT | Month |
| `trip_count` | BIGINT | Trip count |
| `total_revenue` | DOUBLE | Total revenue |
| `avg_revenue_per_trip` | DOUBLE | Average revenue |
| `avg_fare` | DOUBLE | Average fare amount |
| `total_tips` | DOUBLE | Total tips (CC only) |
| `avg_tip_pct` | DOUBLE | Average tip as % of fare (CC only) |
| `median_tip_pct` | DOUBLE | Median tip % (CC only) |
| `avg_trip_distance_miles` | DOUBLE | Mean distance |
| `airport_trip_count` | BIGINT | Airport trip count |

---

## Reference: `dim_taxi_zones`

| Column | Type | Description |
|---|---|---|
| `location_id` | INT | TLC zone ID (1-265), primary key |
| `borough` | STRING | Manhattan / Brooklyn / Queens / Bronx / Staten Island / EWR |
| `zone_name` | STRING | Zone label (e.g. "Midtown Center") |
| `service_zone` | STRING | Yellow Zone / Boro Zone / Airports |
| `is_airport_zone` | BOOLEAN | True for JFK (132), LGA (138), EWR (1) |
| `borough_group` | STRING | Inner / Outer Core / Outer / Airport |
