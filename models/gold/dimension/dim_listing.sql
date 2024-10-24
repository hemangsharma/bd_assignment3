-- models/gold/dim_listing.sql
{{ config(
    materialized='table'
) }}
SELECT DISTINCT
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates
FROM {{ ref('silver_airbnb_listings') }}