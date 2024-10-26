-- models/gold/fact/fact_listing_revenue.sql

WITH revenue_data AS (
    SELECT
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        a.PRICE,
        a.NUMBER_OF_REVIEWS,
        a.REVIEW_SCORES_RATING,
        a."SCRAPED_DATE",
        a."SCRAPED_MONTH",
        l.lga_name,
        l.lga_code,
        SUM(a.PRICE * a.NUMBER_OF_REVIEWS) AS estimated_revenue,  
        COUNT(DISTINCT a."LISTING_ID") AS active_listings         
    FROM
        {{ ref('silver_airbnb_listings') }} AS a
    JOIN
        {{ ref('silver_lga_neighbourhood') }} AS l
    ON
        a.listing_neighbourhood = l.listing_neighbourhood
    GROUP BY
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        a.PRICE,
        a.NUMBER_OF_REVIEWS,
        a.REVIEW_SCORES_RATING,
        a."SCRAPED_DATE",
        a."SCRAPED_MONTH",
        l.lga_name,
        l.lga_code
)

SELECT
    "LISTING_ID",
    "HOST_ID",
    listing_neighbourhood,
    lga_code,
    estimated_revenue,
    active_listings,
    "SCRAPED_DATE",
    "SCRAPED_MONTH"
FROM
    revenue_data