-- models/gold/dim_property.sql
{{ config(
    materialized='table'
) }}
SELECT DISTINCT
    property_type,
    room_type,
    "ACCOMMODATES"
FROM {{ ref('silver_airbnb_listings') }}