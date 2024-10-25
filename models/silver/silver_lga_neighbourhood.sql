-- models/silver/silver_lga_neighbourhood.sql

WITH lga_neighbourhood AS (
    SELECT 
        ls."LGA_CODE" AS lga_code,                          -- Adding LGA_CODE from bronze_lga_code
        LOWER(ls."LGA_NAME") AS lga_name,                          -- LGA Name from bronze_lga_code
        LOWER(s."SUBURB_NAME") AS listing_neighbourhood           -- Suburb Name from bronze_lga_suburb
    FROM {{ ref('bronze_lga_code') }} ls
    LEFT JOIN {{ ref('bronze_lga_suburb') }} s
    ON LOWER(ls."LGA_NAME") = LOWER(s."LGA_NAME")                -- Join on LGA_NAME to match suburbs in lowercase
)
SELECT *
FROM lga_neighbourhood