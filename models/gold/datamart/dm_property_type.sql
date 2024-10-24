-- models/datamart/dm_property_type.sql
WITH property_metrics AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        COUNT(listing_id) AS active_listings,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(price) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - availability_30) AS total_stays,
        SUM(price * (30 - availability_30)) AS estimated_revenue
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY property_type, room_type, accommodates
)
SELECT *
FROM property_metrics