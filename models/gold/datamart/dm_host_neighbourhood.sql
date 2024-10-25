-- models/datamart/dm_host_neighbourhood.sql
{{ config(materialized='table') }}

WITH lga_mapping AS (
    SELECT 
        ln.listing_neighbourhood AS listing_neighbourhood,  -- Normalize suburb names for case-insensitive comparison
        ln.lga_name AS host_neighbourhood_lga  -- LGA names to use for mapping
    FROM {{ ref('silver_lga_neighbourhood') }} ln
),
host_metrics AS (
    SELECT
        lm.host_neighbourhood_lga,  -- Mapped LGA name
        date_trunc('month', CAST(l."SCRAPED_DATE" AS DATE)) AS month_year,  -- Extracting month/year from SCRAPED_DATE
        COUNT(DISTINCT l."HOST_ID") AS distinct_hosts,  -- Number of distinct hosts
        SUM(l.PRICE * (30 - l.availability_30)) AS estimated_revenue,  -- Calculating estimated revenue
        (SUM(l.PRICE * (30 - l.availability_30)) / NULLIF(COUNT(DISTINCT l."HOST_ID"), 0)) AS estimated_revenue_per_host  -- Revenue per distinct host
    FROM {{ ref('silver_airbnb_listings') }} l
    LEFT JOIN lga_mapping lm
        ON LOWER(l.listing_neighbourhood) = lm.listing_neighbourhood  -- Join for LGA mapping
    WHERE l.has_availability = 't'  -- Filter for listings with availability
    GROUP BY lm.host_neighbourhood_lga, month_year  -- Grouping by LGA and month/year
)
SELECT *
FROM host_metrics
WHERE host_neighbourhood_lga IS NOT NULL  -- Ensure only valid LGA names are included
ORDER BY host_neighbourhood_lga, month_year  -- Order by LGA and month/year