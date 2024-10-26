-- models/silver/silver_airbnb_listings.sql

{{ config(
    schema="silver",
    materialized="table"
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
        
        -- Split SCRAPED_DATE into year, month, and day without decimals
        EXTRACT(YEAR FROM TO_DATE("SCRAPED_DATE", 'YYYY-MM-DD'))::INTEGER AS "SCRAPED_YEAR",
        EXTRACT(MONTH FROM TO_DATE("SCRAPED_DATE", 'YYYY-MM-DD'))::INTEGER AS "SCRAPED_MONTH",
        EXTRACT(DAY FROM TO_DATE("SCRAPED_DATE", 'YYYY-MM-DD'))::INTEGER AS "SCRAPED_DAY",

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
        
        -- Replace HOST_NEIGHBOURHOOD with LISTING_NEIGHBOURHOOD if HOST_NEIGHBOURHOOD is null
        COALESCE(INITCAP(TRIM("HOST_NEIGHBOURHOOD")), INITCAP(TRIM("LISTING_NEIGHBOURHOOD"))) AS HOST_NEIGHBOURHOOD,
        
        INITCAP(TRIM("LISTING_NEIGHBOURHOOD")) AS LISTING_NEIGHBOURHOOD,
        INITCAP(TRIM("PROPERTY_TYPE")) AS PROPERTY_TYPE,
        INITCAP(TRIM("ROOM_TYPE")) AS ROOM_TYPE,
        "ACCOMMODATES",
        
        -- Set PRICE to 0 if it is null
        COALESCE("PRICE", 0) AS PRICE,
        
        CASE
            WHEN TRIM("HAS_AVAILABILITY") = 't' THEN TRUE
            ELSE FALSE
        END AS HAS_AVAILABILITY,
        
        COALESCE("AVAILABILITY_30", 0) AS AVAILABILITY_30,
        COALESCE("NUMBER_OF_REVIEWS", 0) AS NUMBER_OF_REVIEWS,
        COALESCE("REVIEW_SCORES_RATING", 0.0) AS REVIEW_SCORES_RATING,
        COALESCE("REVIEW_SCORES_ACCURACY", 0.0) AS REVIEW_SCORES_ACCURACY,
        COALESCE("REVIEW_SCORES_CLEANLINESS", 0.0) AS REVIEW_SCORES_CLEANLINESS,
        COALESCE("REVIEW_SCORES_CHECKIN", 0.0) AS REVIEW_SCORES_CHECKIN,
        COALESCE("REVIEW_SCORES_COMMUNICATION", 0.0) AS REVIEW_SCORES_COMMUNICATION,
        COALESCE("REVIEW_SCORES_VALUE", 0.0) AS REVIEW_SCORES_VALUE,
        
        CURRENT_TIMESTAMP AS snapshot_timestamp  -- Capture the snapshot time
    FROM {{ ref ('bronze_airbnb_listings') }}
    
    -- Remove rows where critical columns like LISTING_ID or PRICE are null
    WHERE 
        "LISTING_ID" IS NOT NULL
        AND "HOST_ID" IS NOT NULL  -- Exclude rows without a host_id
        AND "SCRAPED_DATE" IS NOT NULL
)

SELECT DISTINCT * 
FROM cleaned_airbnb_listings