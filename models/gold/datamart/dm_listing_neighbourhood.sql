WITH neighbourhood_metrics AS (
    SELECT
        listing_neighbourhood,
        date_trunc('month', CAST("SCRAPED_DATE" AS DATE)) AS month_year,
        COUNT("LISTING_ID") AS active_listings,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(PRICE) AS avg_price,
        COUNT(DISTINCT "HOST_ID") AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - "AVAILABILITY_30") AS total_stays,
        SUM(price * (30 - "AVAILABILITY_30")) AS estimated_revenue,
        (COUNT(DISTINCT "HOST_ID") FILTER(WHERE "HOST_IS_SUPERHOST" = 't') * 100.0 / COUNT(DISTINCT "HOST_ID")) AS superhost_rate
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY listing_neighbourhood, month_year
),
active_change AS (
    SELECT
        listing_neighbourhood,
        month_year,
        active_listings,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_active_listings,
        ((active_listings - LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 /
        NULLIF(LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0)) AS pct_change_active
    FROM neighbourhood_metrics
),
inactive_change AS (
    SELECT
        listing_neighbourhood,
        date_trunc('month', CAST("SCRAPED_DATE" AS DATE)) AS month_year,
        COUNT("LISTING_ID") AS inactive_listings,
        LAG(COUNT("LISTING_ID")) OVER (PARTITION BY listing_neighbourhood ORDER BY date_trunc('month', CAST("SCRAPED_DATE" AS DATE))) AS prev_inactive_listings,
        ((COUNT("LISTING_ID") - LAG(COUNT("LISTING_ID")) OVER (PARTITION BY listing_neighbourhood ORDER BY date_trunc('month', CAST("SCRAPED_DATE" AS DATE))) ) * 100.0 /
        NULLIF(LAG(COUNT("LISTING_ID")) OVER (PARTITION BY listing_neighbourhood ORDER BY date_trunc('month', CAST("SCRAPED_DATE" AS DATE))), 0)) AS pct_change_inactive
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 'f'
    GROUP BY listing_neighbourhood, month_year
)
SELECT
    nm.listing_neighbourhood,
    nm.month_year,
    nm.active_listings,
    nm.min_price,
    nm.max_price,
    nm.avg_price,
    nm.distinct_hosts,
    nm.superhost_rate,
    nm.avg_review_score,
    nm.total_stays,
    nm.estimated_revenue,
    ac.pct_change_active,
    ic.pct_change_inactive
FROM neighbourhood_metrics nm
LEFT JOIN active_change ac
    ON nm.listing_neighbourhood = ac.listing_neighbourhood
    AND nm.month_year = ac.month_year
LEFT JOIN inactive_change ic
    ON nm.listing_neighbourhood = ic.listing_neighbourhood
    AND nm.month_year = ic.month_year
ORDER BY nm.listing_neighbourhood, nm.month_year