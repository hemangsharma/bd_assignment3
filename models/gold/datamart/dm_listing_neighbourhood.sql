-- models/datamart/dm_listing_neighbourhood.sql
WITH neighbourhood_metrics AS (
    SELECT
        listing_neighbourhood,
        COUNT(listing_id) AS active_listings,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - availability_30) AS total_stays,
        SUM(price * (30 - availability_30)) AS estimated_revenue,
        (COUNT(DISTINCT host_id) FILTER(WHERE host_is_superhost = 't') * 100.0 / COUNT(DISTINCT host_id)) AS superhost_rate
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY listing_neighbourhood
)
SELECT *
FROM neighbourhood_metrics