-- models/gold/fact_listings.sql
WITH listings AS (
    SELECT
        "LISTING_ID",
        COUNT("LISTING_ID") AS total_active_listings,
        AVG(PRICE) AS avg_price,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(review_scores_rating) AS avg_review_score,
        COUNT(CASE WHEN has_availability = 't' THEN 1 END) AS active_listings
    FROM {{ ref('silver_airbnb_listings') }}
    GROUP BY "LISTING_ID"
)
SELECT *
FROM listings