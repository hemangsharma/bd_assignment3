{{ config(
    materialized='table'
) }}

WITH cleaned_airbnb_listings AS (
    SELECT
        "LISTING_ID",
        "SCRAPE_ID",
        CASE
            -- Handling date formats for SCRAPED_DATE
            WHEN "SCRAPED_DATE" ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
                TO_CHAR(TO_DATE("SCRAPED_DATE", 'DD/MM/YYYY'), 'YYYY-MM-DD')
            WHEN "SCRAPED_DATE" ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' THEN
                "SCRAPED_DATE"
            ELSE
                NULL  -- Handle unexpected formats
        END AS "SCRAPED_DATE",
        "HOST_ID",
        INITCAP(TRIM("HOST_NAME")) AS HOST_NAME,
        CASE
            -- Handling date formats for HOST_SINCE
            WHEN "HOST_SINCE" ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
                TO_CHAR(TO_DATE("HOST_SINCE", 'DD/MM/YYYY'), 'YYYY-MM-DD')
            WHEN "HOST_SINCE" ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' THEN
                "HOST_SINCE"
            ELSE
                NULL  -- Handle unexpected formats
        END AS "HOST_SINCE",
        CASE
            WHEN TRIM("HOST_IS_SUPERHOST") = 't' THEN TRUE
            ELSE FALSE
        END AS "HOST_IS_SUPERHOST",
        INITCAP(TRIM("HOST_NEIGHBOURHOOD")) AS HOST_NEIGHBOURHOOD,
        INITCAP(TRIM("LISTING_NEIGHBOURHOOD")) AS LISTING_NEIGHBOURHOOD,
        INITCAP(TRIM("PROPERTY_TYPE")) AS PROPERTY_TYPE,
        INITCAP(TRIM("ROOM_TYPE")) AS ROOM_TYPE,
        "ACCOMMODATES",
        CASE 
            WHEN "PRICE" IS NULL THEN 0
            ELSE "PRICE"
        END AS PRICE,
        CASE
            WHEN TRIM("HAS_AVAILABILITY") = 't' THEN TRUE
            ELSE FALSE
        END AS HAS_AVAILABILITY,
        "AVAILABILITY_30",
        "NUMBER_OF_REVIEWS",
        CASE 
            WHEN "REVIEW_SCORES_RATING" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_RATING"
        END AS REVIEW_SCORES_RATING,
        CASE 
            WHEN "REVIEW_SCORES_ACCURACY" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_ACCURACY"
        END AS REVIEW_SCORES_ACCURACY,
        CASE 
            WHEN "REVIEW_SCORES_CLEANLINESS" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_CLEANLINESS"
        END AS REVIEW_SCORES_CLEANLINESS,
        CASE 
            WHEN "REVIEW_SCORES_CHECKIN" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_CHECKIN"
        END AS REVIEW_SCORES_CHECKIN,
        CASE 
            WHEN "REVIEW_SCORES_COMMUNICATION" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_COMMUNICATION"
        END AS REVIEW_SCORES_COMMUNICATION,
        CASE 
            WHEN "REVIEW_SCORES_VALUE" IS NULL THEN 0.0
            ELSE "REVIEW_SCORES_VALUE"
        END AS REVIEW_SCORES_VALUE,
        CURRENT_TIMESTAMP AS snapshot_timestamp  -- Capture the snapshot time
    FROM {{ ref ('bronze_airbnb_listings') }}
    WHERE "LISTING_ID" IS NOT NULL  -- Exclude rows without a listing_id
)

SELECT DISTINCT * 
FROM cleaned_airbnb_listings