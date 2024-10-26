WITH top_neighbourhoods AS (
    SELECT 
        listing_neighbourhood,
        AVG(estimated_revenue) AS avg_revenue_per_listing
    FROM 
        {{ ref('fact_listing_revenue') }}  -- Your gold fact table
    GROUP BY 
        listing_neighbourhood
    ORDER BY 
        avg_revenue_per_listing DESC
    LIMIT 5
),
neighbourhood_listing_revenue AS (
    SELECT 
        l.listing_neighbourhood,
        l.property_type,
        l.room_type,
        l."ACCOMMODATES",
        SUM(fr.estimated_revenue) AS total_revenue  -- Total revenue per listing type
    FROM 
        {{ ref('silver_airbnb_listings') }} l
    JOIN 
        {{ ref('fact_listing_revenue') }} fr ON l."LISTING_ID" = fr."LISTING_ID"
    JOIN 
        top_neighbourhoods t ON l.listing_neighbourhood = t.listing_neighbourhood
    GROUP BY 
        l.listing_neighbourhood, l.property_type, l.room_type, l."ACCOMMODATES"
),
best_listing_per_neighbourhood AS (
    SELECT 
        listing_neighbourhood,
        property_type,
        room_type,
        "ACCOMMODATES",
        total_revenue,
        ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY total_revenue DESC) AS revenue_rank
    FROM 
        neighbourhood_listing_revenue
)
SELECT 
    listing_neighbourhood,
    property_type,
    room_type,
    "ACCOMMODATES",
    total_revenue
FROM 
    best_listing_per_neighbourhood
WHERE 
    revenue_rank = 1  -- Selects the top listing type in each neighborhood
ORDER BY 
    total_revenue DESC