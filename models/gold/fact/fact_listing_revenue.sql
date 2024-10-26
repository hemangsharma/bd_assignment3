WITH revenue_data AS (
    SELECT
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        a.PRICE,
        a."SCRAPED_DATE",
        l.lga_name,
        l.lga_code,
        SUM(a.PRICE * (30 - a.availability_30)) AS estimated_revenue,  -- Revenue based on price and availability
        COUNT(DISTINCT a."LISTING_ID") AS active_listings         
    FROM
        {{ ref('silver_airbnb_listings') }} AS a
    JOIN
        {{ ref('silver_lga_neighbourhood') }} AS l
    ON
        LOWER(a.listing_neighbourhood) = LOWER(l.listing_neighbourhood)
    WHERE
        a.has_availability = 'true'
    GROUP BY
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        l.lga_name,
        a.PRICE,
        l.lga_code,
        a."SCRAPED_DATE"
)

SELECT
    "LISTING_ID",
    "HOST_ID",
    LISTING_NEIGHBOURHOOD,
    lga_code,
    estimated_revenue,
    active_listings,
    "SCRAPED_DATE"
FROM
    revenue_data
