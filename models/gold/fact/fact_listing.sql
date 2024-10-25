-- models/gold/fact_listing.sql
SELECT 
    "LISTING_ID",
    "HOST_ID",
    listing_neighbourhood,
    COUNT(*) AS total_stays,
    SUM(price * (30 - availability_30)) AS estimated_revenue
FROM {{ ref('silver_airbnb_listings') }}
GROUP BY "LISTING_ID", "HOST_ID", listing_neighbourhood