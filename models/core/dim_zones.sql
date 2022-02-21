{{ config(materialized='table') }}

select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') AS service_zone
from
    {{ ref('taxi_zone_lookup') }}