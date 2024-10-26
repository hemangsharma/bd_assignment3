-- models/gold/dimension/dim_host.sql

{{ config(
    materialized='table'
) }}

SELECT 
    host_id,
    host_neighbourhood,
    "HOST_IS_SUPERHOST",
    num_listings
FROM {{ ref('hosts_cleaned') }}

