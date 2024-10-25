-- models/gold/dimension/dim_neighbourhoods.sql
SELECT 
    listing_neighbourhood
FROM {{ ref('silver_lga_neighbourhood') }}