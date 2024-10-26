-- models/bronze/bronze_lga_suburb.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'lga_suburb') }}