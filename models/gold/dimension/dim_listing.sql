-- models/gold/dim_listing.sql
{{ config(
    materialized='table'
) }}
SELECT DISTINCT
    "LISTING_ID",
    LISTING_NEIGHBOURHOOD,
    property_type,
    room_type,
    "ACCOMMODATES"
FROM {{ ref('silver_airbnb_listings') }}