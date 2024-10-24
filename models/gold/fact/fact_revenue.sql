-- models/gold/fact_revenue.sql
WITH revenue AS (
    SELECT
        listing_id,
        AVG(price * (30 - availability_30)) AS estimated_revenue,
        (30 - availability_30) AS number_of_stays
    FROM {{ ref('silver_airbnb_listings') }}
    GROUP BY listing_id, price, availability_30
)
SELECT *
FROM revenue