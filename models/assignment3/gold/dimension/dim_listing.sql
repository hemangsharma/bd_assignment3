-- models/gold/dimension/dim_listing.sql

{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        l."LISTING_ID" AS listing_id,
        l.listing_neighbourhood,
        l.property_type,
        l.room_type,
        l."ACCOMMODATES",
        l.price,
        l."SCRAPED_DATE"
    FROM {{ ref('silver_airbnb_listings') }} l
)

SELECT
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    "ACCOMMODATES",
    price,
    "SCRAPED_DATE",
    CURRENT_TIMESTAMP AS effective_date
FROM source_data
