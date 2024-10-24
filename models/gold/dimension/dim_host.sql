-- models/gold/dim_host.sql
{{ config(
    materialized='table'
) }}
SELECT DISTINCT
    host_id,
    host_neighbourhood,
    host_is_superhost
FROM {{ ref('silver_airbnb_listings') }}