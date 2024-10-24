-- models/bronze/bronze_census_g01.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'census_g01') }}