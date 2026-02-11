with trips as (
    select * from {{ ref('int_trip_metrics') }}
)

select
    pickup_hour,
    time_period,
    day_type,

    -- Volume
    count(*) as total_trips,
    avg(passenger_count) as avg_passengers,

    -- Métriques de performance
    avg(trip_distance) as avg_distance,
    avg(trip_duration_minutes) as avg_duration,
    avg(avg_speed_mph) as avg_speed,

    -- Métriques financières
    avg(total_amount) as avg_revenue,
    sum(total_amount) as total_revenue,
    avg(tip_percentage) as avg_tip_pct,

    -- Distribution par catégorie de distance
    sum(case when distance_category = 'Court (≤1 mile)' then 1 else 0 end) as short_trips,
    sum(case when distance_category = 'Moyen (1-5 miles)' then 1 else 0 end) as medium_trips,
    sum(case when distance_category = 'Long (5-10 miles)' then 1 else 0 end) as long_trips,
    sum(case when distance_category = 'Très long (>10 miles)' then 1 else 0 end) as very_long_trips

from trips
group by pickup_hour, time_period, day_type
order by pickup_hour, day_type
