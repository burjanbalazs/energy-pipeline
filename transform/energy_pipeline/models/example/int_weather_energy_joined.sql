

{{ config(materialized='table') }}

select 
    weather.city_name,
    weather.country,
    weather.time,
    weather.temperature,
    weather.humidity,
    weather.wind_speed,
    weather.precipitation,
    weather.weather_code,
    energy.load_mw
from {{ref('stg_weather') }} as weather
join {{ref('stg_energy') }} as energy
on 
    weather.day = energy.day and 
    weather.year = energy.year and 
    weather.month = energy.month and 
    datepart('hour', weather.time) = datepart('hour', energy.time) and
    weather.city_name = energy.city_name