-- models/gold/dim_host.sql

{{ config(
    materialized='table'
) }}

SELECT 
    "HOST_ID",
    host_neighbourhood,
    "HOST_IS_SUPERHOST",
    num_listings
FROM {{ ref('hosts_cleaned') }};

