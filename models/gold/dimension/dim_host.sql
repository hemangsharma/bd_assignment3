-- models/gold/dim_host.sql
{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    "HOST_ID",
    HOST_NEIGHBOURHOOD,
    "HOST_IS_SUPERHOST"
FROM {{ ref('silver_airbnb_listings') }}