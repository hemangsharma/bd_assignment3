--snapshots/airbnb_snapshots.sql
{% snapshot airbnb_snapshots %}

{{ config(
    target_schema='dbt_assignment_silver',
    unique_key= 'LISTING_ID',
    strategy='timestamp',
    updated_at="SCRAPE_ID"
) }}

SELECT 
    "LISTING_ID",
    "SCRAPE_ID",
    "SCRAPED_DATE",
    "HOST_ID",
    "HOST_NAME",
    "HOST_SINCE",
    "HOST_IS_SUPERHOST",
    "HOST_NEIGHBOURHOOD",
    "LISTING_NEIGHBOURHOOD",
    "PROPERTY_TYPE",
    "ROOM_TYPE",
    "ACCOMMODATES",
    "PRICE",
    "HAS_AVAILABILITY",
    "AVAILABILITY_30",
    "NUMBER_OF_REVIEWS",
    "REVIEW_SCORES_RATING",
    "REVIEW_SCORES_ACCURACY",
    "REVIEW_SCORES_CLEANLINESS",
    "REVIEW_SCORES_CHECKIN",
    "REVIEW_SCORES_COMMUNICATION",
    "REVIEW_SCORES_VALUE"
FROM {{ source('raw', 'airbnb_listings') }}

{% endsnapshot %}