with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)

select
    *,

    -- Durée du trajet en minutes
    datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,

    -- Dimensions temporelles
    extract(hour from tpep_pickup_datetime) as pickup_hour,
    extract(day from tpep_pickup_datetime) as pickup_day,
    extract(month from tpep_pickup_datetime) as pickup_month,
    extract(year from tpep_pickup_datetime) as pickup_year,
    dayname(tpep_pickup_datetime) as pickup_day_name,

    case
        when dayofweek(tpep_pickup_datetime) in (0, 6) then 'Weekend'
        else 'Weekday'
    end as day_type,

    -- Période de la journée
    case
        when extract(hour from tpep_pickup_datetime) between 6 and 9 then 'Rush Matinal'
        when extract(hour from tpep_pickup_datetime) between 10 and 15 then 'Journée'
        when extract(hour from tpep_pickup_datetime) between 16 and 19 then 'Rush Soir'
        when extract(hour from tpep_pickup_datetime) between 20 and 23 then 'Soirée'
        else 'Nuit'
    end as time_period,

    -- Catégorie de distance
    case
        when trip_distance <= 1 then 'Court (≤1 mile)'
        when trip_distance <= 5 then 'Moyen (1-5 miles)'
        when trip_distance <= 10 then 'Long (5-10 miles)'
        else 'Très long (>10 miles)'
    end as distance_category,

    -- Vitesse moyenne en mph
    case
        when datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 0
        then (trip_distance / datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)) * 60
        else null
    end as avg_speed_mph,

    -- Pourcentage de pourboire
    case
        when fare_amount > 0 then (tip_amount / fare_amount) * 100
        else 0
    end as tip_percentage

from trips
