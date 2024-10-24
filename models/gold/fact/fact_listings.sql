-- models/gold/fact_listings.sql
WITH listings AS (
    SELECT
        listing_id,
        COUNT(listing_id) AS total_active_listings,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(review_scores_rating) AS avg_review_score,
        COUNT(CASE WHEN has_availability = 't' THEN 1 END) AS active_listings
    FROM {{ ref('silver_airbnb_listings') }}
    GROUP BY listing_id
)
SELECT *
FROM listings