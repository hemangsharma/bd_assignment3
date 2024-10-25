-- models/gold/dim_lga.sql
SELECT 
    lga_code,
    median_rent_weekly,
    total_population,
    median_age_persons,
    average_household_size
FROM {{ ref('silver_census') }}
WHERE median_rent_weekly > 0