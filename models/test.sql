SELECT *
FROM {{ ref('bronze_airbnb_listings') }}
WHERE HAS_AVAILABILITY = 'f'