{% snapshot silver_airbnb_listings_snapshot %}

{{ config(
    target_schema='dbt_assignment_silver',
    unique_key='HOST_NAME',
    strategy='timestamp',
    updated_at='PRICE'
) }}


SELECT 
    "LISTING_ID",
    "SCRAPED_DATE",
    "HOST_ID",
    "HOST_IS_SUPERHOST",
    HOST_NEIGHBOURHOOD,
    PROPERTY_TYPE,
    ROOM_TYPE,
    "ACCOMMODATES",
    PRICE,
    HAS_AVAILABILITY,
    availability_30,
    REVIEW_SCORES_RATING,
    REVIEW_SCORES_CLEANLINESS,
    REVIEW_SCORES_VALUE
FROM {{ ref('silver_airbnb_listings') }}

{% endsnapshot %}