-- models/silver/hosts_cleaned.sql
{{ config(
    schema="silver",
    materialized="table"
) }}

SELECT 
    "HOST_ID" AS host_id,
    host_neighbourhood,
    "HOST_IS_SUPERHOST",
    COUNT(DISTINCT "LISTING_ID") AS num_listings
FROM {{ ref('silver_airbnb_listings') }}
GROUP BY host_id, host_neighbourhood, "HOST_IS_SUPERHOST"