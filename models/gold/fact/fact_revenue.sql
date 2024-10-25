-- models/gold/fact/fact_revenue.sql
WITH revenue AS (
    SELECT
        "LISTING_ID",
        AVG(PRICE * (30 - availability_30)) AS estimated_revenue,
        (30 - availability_30) AS number_of_stays
    FROM {{ ref('silver_airbnb_listings') }}
    GROUP BY "LISTING_ID", PRICE, availability_30
)
SELECT *
FROM revenue
WHERE estimated_revenue > 0