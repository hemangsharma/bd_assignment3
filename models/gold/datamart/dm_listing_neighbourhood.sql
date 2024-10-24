-- models/datamart/dm_listing_neighbourhood.sql
WITH neighbourhood_metrics AS (
    SELECT
        listing_neighbourhood,
        COUNT("LISTING_ID") AS active_listings,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(PRICE) AS avg_price,
        COUNT(DISTINCT "HOST_ID") AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - "AVAILABILITY_30") AS total_stays,
        SUM(price * (30 - "AVAILABILITY_30")) AS estimated_revenue,
        (COUNT(DISTINCT "HOST_ID") FILTER(WHERE "HOST_IS_SUPERHOST" = 't') * 100.0 / COUNT(DISTINCT "HOST_ID")) AS superhost_rate
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY listing_neighbourhood
)
SELECT *
FROM neighbourhood_metrics