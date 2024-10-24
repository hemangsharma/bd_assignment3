-- models/datamart/dm_host_neighbourhood.sql
{{ config(materialized='table') }}

WITH host_metrics AS (
    SELECT
        HOST_NEIGHBOURHOOD,
        COUNT(DISTINCT "HOST_ID") AS distinct_hosts,
        SUM(PRICE * (30 - "AVAILABILITY_30")) AS estimated_revenue,
        (SUM(PRICE * (30 - "AVAILABILITY_30")) / COUNT(DISTINCT "HOST_ID")) AS estimated_revenue_per_host
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY HOST_NEIGHBOURHOOD
)
SELECT *
FROM host_metrics