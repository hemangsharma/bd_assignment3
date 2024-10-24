-- models/datamart/dm_property_type.sql
WITH property_metrics AS (
    SELECT
        PROPERTY_TYPE,
        ROOM_TYPE,
        "ACCOMMODATES",
        COUNT("LISTING_ID") AS active_listings,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(PRICE) AS avg_price,
        COUNT(DISTINCT "HOST_ID") AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - "AVAILABILITY_30") AS total_stays,
        SUM(PRICE * (30 - "AVAILABILITY_30")) AS estimated_revenue
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY PROPERTY_TYPE, ROOM_TYPE, "ACCOMMODATES"
)
SELECT *
FROM property_metrics