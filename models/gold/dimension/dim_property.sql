-- models/gold/dim_property.sql
{{ config(
    materialized='table'
) }}
SELECT DISTINCT
    property_type,
    room_type,
    accommodates
FROM {{ ref('silver_airbnb_listings') }}