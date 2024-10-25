-- models/gold/fact_airbnb_listings.sql

{{ config(
    materialized='table'
) }}

WITH airbnb_facts AS (
    SELECT
        al."LISTING_ID",
        al."SCRAPED_DATE",
        al."HOST_ID",
        al."HOST_IS_SUPERHOST",
        al."ACCOMMODATES",
        al.PRICE,
        al."NUMBER_OF_REVIEWS",
        al.REVIEW_SCORES_RATING,
        al.REVIEW_SCORES_ACCURACY,
        al.REVIEW_SCORES_CLEANLINESS,
        al.REVIEW_SCORES_CHECKIN,
        al.REVIEW_SCORES_COMMUNICATION,
        al.REVIEW_SCORES_VALUE,
        -- Extracting numeric LGA code from silver census
        SUBSTRING(c.lga_code_2016 FROM 4) AS lga_code_numeric,  -- Discarding the 'LGA' prefix
        c.total_population,
        c.age_0_4_total,
        c.age_5_14_total,
        c.age_15_19_total,
        c.age_20_24_total,
        c.age_25_34_total,
        c.age_35_44_total,
        c.age_45_54_total,
        c.age_55_64_total,
        c.age_65_74_total,
        c.age_75_84_total,
        c.age_85_plus_total,
        c.indigenous_population_total,
        c.average_household_size
    FROM {{ ref('silver_airbnb_listings') }} AS al
    JOIN {{ ref('silver_census') }} AS c
    ON SUBSTRING(c.lga_code_2016 FROM 4) = (SELECT DISTINCT "LGA_CODE" FROM {{ ref('bronze_lga_code') }} WHERE "LGA_CODE" = SUBSTRING(c.lga_code_2016 FROM 4))
)

SELECT 
    *,CURRENT_TIMESTAMP AS snapshot_timestamp  
FROM airbnb_facts
