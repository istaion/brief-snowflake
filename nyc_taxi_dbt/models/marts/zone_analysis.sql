with trips as (
    select * from {{ ref('int_trip_metrics') }}
)

select
    PULocationID as pickup_zone,

    -- Volume
    count(*) as total_trips,
    count(*) * 100.0 / sum(count(*)) over () as pct_of_total_trips,

    -- Métriques moyennes
    avg(trip_distance) as avg_distance,
    avg(trip_duration_minutes) as avg_duration,
    avg(total_amount) as avg_revenue,
    avg(tip_percentage) as avg_tip_pct,

    -- Totaux
    sum(total_amount) as total_revenue,
    sum(tip_amount) as total_tips,

    -- Distribution par période
    sum(case when time_period = 'Rush Matinal' then 1 else 0 end) as trips_rush_morning,
    sum(case when time_period = 'Rush Soir' then 1 else 0 end) as trips_rush_evening,
    sum(case when time_period = 'Nuit' then 1 else 0 end) as trips_night,

    -- Destination la plus fréquente
    mode(DOLocationID) as most_common_destination

from trips
group by PULocationID
order by total_trips desc
