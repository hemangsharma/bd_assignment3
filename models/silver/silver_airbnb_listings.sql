WITH listings_clean AS (
    SELECT 
        "listing_id",
        "host_id"
    FROM {{ ref('bronze_airbnb_listings') }}
)
SELECT *
FROM listings_clean