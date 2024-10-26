WITH single_listing_hosts AS (
    SELECT 
        "HOST_ID",
        lga_code::text,  -- Cast to text to match the lga_code in the lga_mortgage CTE
        AVG(estimated_revenue) AS avg_revenue
    FROM 
        {{ ref('fact_listing_revenue') }}  -- Adjust to your gold fact table
    WHERE 
        "HOST_ID" IN (
            SELECT "HOST_ID" 
            FROM {{ ref('silver_airbnb_listings') }} 
            GROUP BY "HOST_ID" 
            HAVING COUNT("LISTING_ID") = 1
        )
    GROUP BY 
        "HOST_ID", lga_code
),
lga_mortgage AS (
    SELECT 
        lga_code,
        median_mortgage_repay_monthly  -- Assuming this column exists in your census data
    FROM 
        {{ ref('silver_census') }}
)
SELECT 
    s.lga_code,
    COUNT(s."HOST_ID") AS single_listing_hosts,
    SUM(CASE WHEN avg_revenue >= m.median_mortgage_repay_monthly THEN 1 ELSE 0 END) AS can_cover_mortgage,
    (SUM(CASE WHEN avg_revenue >= m.median_mortgage_repay_monthly THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(s."HOST_ID"), 0)) AS percentage_cover
FROM 
    single_listing_hosts s
JOIN 
    lga_mortgage m ON s.lga_code = m.lga_code
GROUP BY 
    s.lga_code
ORDER BY 
    percentage_cover DESC