-- models/gold/fact_listing_revenue.sql

WITH revenue_data AS (
    SELECT
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        a.PRICE,
        a."NUMBER_OF_REVIEWS",
        a.REVIEW_SCORES_RATING,
        a."SCRAPED_DATE",
        l."LGA_NAME",  -- Assuming LGA_CODE is in the silver_lga_neighbourhood
        SUM(a.PRICE) AS estimated_revenue,
        COUNT(a."LISTING_ID") AS active_listings
    FROM
        {{ ref('silver_airbnb_listings') }} AS a
    JOIN
        {{ ref('silver_lga_neighbourhood') }} AS l
    ON
        a.LISTING_NEIGHBOURHOOD = l.listing_neighbourhood

    WHERE
        a."SCRAPED_DATE" >= DATEADD(MONTH, -12, CURRENT_DATE)  -- Last 12 months
    GROUP BY
        a."LISTING_ID",
        a."HOST_ID",
        a.LISTING_NEIGHBOURHOOD,
        a.PRICE,
        a."NUMBER_OF_REVIEWS",
        a.REVIEW_SCORES_RATING,
        a."SCRAPED_DATE"
)

SELECT
    listing_id,
    host_id,
    listing_neighbourhood,
    lga_code,
    estimated_revenue,
    active_listings,
    scraped_date
FROM
    revenue_data