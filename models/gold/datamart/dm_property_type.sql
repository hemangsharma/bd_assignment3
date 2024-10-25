-- models/datamart/dm_property_type.sql
{{ config(materialized='table') }}

WITH property_metrics AS (
    SELECT
        l.PROPERTY_TYPE,
        l.ROOM_TYPE,
        l."ACCOMMODATES",
        date_trunc('month', CAST(l."SCRAPED_DATE" AS DATE)) AS month_year,  -- Extracting month/year from SCRAPED_DATE
        "SCRAPED_MONTH" as month,
        "SCRAPED_YEAR" as year,
        COUNT(l."LISTING_ID") AS active_listings,  -- Count of active listings
        MIN(l.PRICE) AS min_price,  -- Minimum price
        MAX(l.PRICE) AS max_price,  -- Maximum price
        AVG(l.PRICE) AS avg_price,  -- Average price
        COUNT(DISTINCT l."HOST_ID") AS distinct_hosts,  -- Number of distinct hosts
        AVG(l.review_scores_rating) AS avg_review_score,  -- Average review score
        SUM(30 - l.availability_30) AS total_stays,  -- Total number of stays
        SUM(l.PRICE * (30 - l.availability_30)) / NULLIF(COUNT(l."LISTING_ID"), 0) AS estimated_revenue_per_listing,  -- Average estimated revenue per active listing
        COUNT(CASE WHEN l."HOST_IS_SUPERHOST" = 't' THEN 1 END) * 1.0 / NULLIF(COUNT(DISTINCT l."HOST_ID"), 0) AS superhost_rate,  -- Superhost rate as decimal
        COUNT(CASE WHEN l.has_availability = 't' THEN 1 END) AS total_active_listings,  -- Count of active listings
        COUNT(CASE WHEN l.has_availability = 'f' THEN 1 END) AS total_inactive_listings  -- Count of inactive listings
    FROM 
        {{ ref('silver_airbnb_listings') }} l
    WHERE 
        l.has_availability = 't'  -- Only count active listings
    GROUP BY 
        l.PROPERTY_TYPE, l.ROOM_TYPE, l."ACCOMMODATES", month_year, month, year  -- Grouping by necessary columns
),

-- Second CTE to calculate percentage changes for active and inactive listings
property_changes AS (
    SELECT 
        *,
        100.0 * (active_listings - LAG(active_listings) OVER (PARTITION BY PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES" ORDER BY month_year)) / NULLIF(LAG(active_listings) OVER (PARTITION BY PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES" ORDER BY month_year), 0) AS pct_change_active_listings,  -- Percentage change for active listings
        100.0 * (total_inactive_listings - LAG(total_inactive_listings) OVER (PARTITION BY PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES" ORDER BY month_year)) / NULLIF(LAG(total_inactive_listings) OVER (PARTITION BY PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES" ORDER BY month_year), 0) AS pct_change_inactive_listings  -- Percentage change for inactive listings
    FROM 
        property_metrics
)

SELECT 
    PROPERTY_TYPE,
    ROOM_TYPE,
    "ACCOMMODATES",
    month_year,
    month,
    year,
    active_listings,
    min_price,
    max_price,
    avg_price,
    distinct_hosts,
    avg_review_score,
    total_stays,
    estimated_revenue_per_listing,
    superhost_rate,
    total_active_listings,
    total_inactive_listings,
    pct_change_active_listings,
    pct_change_inactive_listings
FROM 
    property_changes
ORDER BY 
    PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES", month_year  -- Order by property_type, room_type, accommodates, and month/year
