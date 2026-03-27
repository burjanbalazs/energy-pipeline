{{ config(materialized='table') }}

select * from read_parquet('az://silver/weather/**/*.parquet')