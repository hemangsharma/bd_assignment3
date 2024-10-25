-- models/dim_suburb.sql

{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        s."lga_code",
        s.listing_neighbourhood,
        s.lga_name
    FROM {{ ref('silver_lga_neighbourhood') }} s
)

SELECT
    lga_code,
    listing_neighbourhood,
    lga_name,
    CURRENT_TIMESTAMP AS effective_date
FROM source_data