with source as (
    select * from {{ source('raw', 'yellow_taxi_trips_clean') }}
),

filtered as (
    select
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee

    from source
    where
        -- Montants positifs
        fare_amount >= 0
        and total_amount >= 0
        and tip_amount >= 0
        -- Dates cohérentes (pickup avant dropoff)
        and tpep_dropoff_datetime > tpep_pickup_datetime
        -- Distances valides (entre 0.1 et 100 miles)
        and trip_distance between 0.1 and 100
        -- Zones non nulles
        and PULocationID is not null
        and DOLocationID is not null
        -- Durée raisonnable (entre 1 minute et 3 heures)
        and datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) between 1 and 180
        -- Nombre de passagers raisonnable
        and (passenger_count is null or passenger_count between 0 and 6)
)

select * from filtered
