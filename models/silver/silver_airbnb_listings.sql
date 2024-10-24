-- models/silver/silver_airbnb_listings.sql
WITH listings_clean AS (
    SELECT 
        listing_id,
        host_id,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price::NUMERIC AS price,
        has_availability,
        availability_30::INT,
        number_of_reviews::INT,
        review_scores_rating::NUMERIC,
        review_scores_accuracy::NUMERIC,
        review_scores_cleanliness::NUMERIC,
        review_scores_checkin::NUMERIC,
        review_scores_communication::NUMERIC,
        review_scores_value::NUMERIC
    FROM {{ ref('bronze_airbnb_listings') }}
    WHERE has_availability = 't'
)
SELECT *
FROM listings_clean