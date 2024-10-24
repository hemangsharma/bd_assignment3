-- models/gold/fact_revenue.sql
WITH revenue AS (
    SELECT
        "LISTING_ID",
        AVG(PRICE * (30 - "AVAILABILITY_30")) AS estimated_revenue,
        (30 - "AVAILABILITY_30") AS number_of_stays
    FROM {{ ref('silver_airbnb_listings') }}
    GROUP BY "LISTING_ID", PRICE, "AVAILABILITY_30"
)
SELECT *
FROM revenue