{{ config(materialized='view') }}

select
    -- identifiers
    generate_uuid() as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as integer) AS sr_flag
from
    {{ source('staging', 'fhv_2019') }}
where
    PUlocationID IN (select locationid from {{ ref('taxi_zone_lookup') }}) and
    DOlocationID IN (select locationid from {{ ref('taxi_zone_lookup') }})

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}