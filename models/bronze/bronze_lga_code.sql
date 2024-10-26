-- models/bronze/bronze_lga_code.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'lga_code') }}