with trips as (
    select * from {{ ref('int_trip_metrics') }}
)

select
    date(tpep_pickup_datetime) as trip_date,
    pickup_year,
    pickup_month,
    pickup_day,
    day_type,

    -- Métriques de volume
    count(*) as total_trips,
    count(distinct PULocationID) as distinct_pickup_zones,
    count(distinct DOLocationID) as distinct_dropoff_zones,

    -- Métriques de distance
    avg(trip_distance) as avg_distance,
    sum(trip_distance) as total_distance,

    -- Métriques financières
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_revenue_per_trip,
    sum(tip_amount) as total_tips,
    avg(tip_percentage) as avg_tip_percentage,

    -- Métriques opérationnelles
    avg(trip_duration_minutes) as avg_duration,
    avg(avg_speed_mph) as avg_speed,
    avg(passenger_count) as avg_passengers

from trips
group by trip_date, pickup_year, pickup_month, pickup_day, day_type
order by trip_date
