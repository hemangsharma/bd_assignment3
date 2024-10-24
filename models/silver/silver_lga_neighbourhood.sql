-- models/silver/silver_lga_neighbourhood.sql
WITH lga_neighbourhood AS (
    SELECT 
        ls."LGA_NAME",
        s."SUBURB_NAME" AS listing_neighbourhood
    FROM {{ ref('bronze_lga_code') }} ls
    LEFT JOIN {{ ref('bronze_lga_suburb') }} s
    ON ls."LGA_NAME" = s."LGA_NAME"
)
SELECT *
FROM lga_neighbourhood