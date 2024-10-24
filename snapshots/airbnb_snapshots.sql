{% snapshot airbnb_listings_snapshot %}

{{ config(
    target_schema='silver',  -- The schema where the snapshot will be stored
    unique_key='listing_id',  -- Each row is identified uniquely by listing_id
    strategy='timestamp',     -- Using timestamp strategy
    updated_at='scraped_date',  -- The field that tracks the last update timestamp
    check_cols=['price', 'availability_30', 'number_of_reviews', 'review_scores_rating', 'host_is_superhost']
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
    review_scores_value,
    CURRENT_TIMESTAMP() AS updated_at  -- Create a timestamp for this snapshot
FROM {{ source('bronze', 'airbnb_listings') }}

{% endsnapshot %}
