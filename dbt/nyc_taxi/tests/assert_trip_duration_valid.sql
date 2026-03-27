-- Custom test: Silver trip_duration_minutes must be within valid range (1-300 min)
-- Validates Silver source data before Gold models consume it

select *
from {{ source('silver', 'yellow_taxi') }}
where trip_duration_minutes < 1
   or trip_duration_minutes > 300
