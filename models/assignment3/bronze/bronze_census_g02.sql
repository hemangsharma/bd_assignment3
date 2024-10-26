-- models/bronze/bronze_census_g02.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'census_g02') }}