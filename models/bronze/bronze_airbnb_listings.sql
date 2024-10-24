-- models/bronze/bronze_airbnb_listings.sql
{{ config(
    materialized='table'
) }}

SELECT 
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
FROM {{ source('raw', 'airbnb_listings') }}