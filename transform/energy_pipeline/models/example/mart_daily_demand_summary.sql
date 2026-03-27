{{ config(materialized='table') }}

select 
    city_name,
    country,
    cast(time as date) as 'activity_date',
    cast(avg(temperature) as decimal(10,2)) as avg_temperature,
    cast(min(temperature) as decimal(10,2)) as min_temperature,
    cast(max(temperature) as decimal(10,2)) as max_temperature,
    cast(avg(humidity) as decimal(10,2)) as avg_humidity,
    cast(min(humidity) as decimal(10,2)) as min_humidity,
    cast(max(humidity) as decimal(10,2)) as max_humidity,
    cast(avg(wind_speed) as decimal(10,2)) as avg_wind_speed,
    cast(avg(precipitation) as decimal(10,2)) as avg_precipitation,
    cast(min(load_mw) as decimal(10,2)) as min_load_mw,
    cast(max(load_mw) as decimal(10,2)) as max_load_mw,
    cast(avg(load_mw) as decimal(10,2)) as avg_load_mw,
    cast(sum(load_mw) as decimal(10,2)) as total_load_mw
from {{ref('int_weather_energy_joined') }} as joined
group by city_name, country, cast(time as date)