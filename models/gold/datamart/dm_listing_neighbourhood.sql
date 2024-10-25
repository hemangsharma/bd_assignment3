WITH neighbourhood_metrics AS (
    SELECT
        listing_neighbourhood,
        date_trunc('month', CAST("SCRAPED_DATE" AS DATE)) AS month_year,
        COUNT("LISTING_ID") FILTER(WHERE has_availability = 't') AS active_listings,
        COUNT("LISTING_ID") FILTER(WHERE has_availability = 'f') AS inactive_listings,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(PRICE) AS avg_price,
        COUNT(DISTINCT "HOST_ID") AS distinct_hosts,
        AVG(review_scores_rating) AS avg_review_score,
        SUM(30 - availability_30) AS total_stays,
        SUM(PRICE * (30 - availability_30)) AS estimated_revenue,
        (COUNT(DISTINCT "HOST_ID") FILTER(WHERE "HOST_IS_SUPERHOST" = 't') * 100.0 / NULLIF(COUNT(DISTINCT "HOST_ID"), 0)) AS superhost_rate
    FROM 
        {{ ref('silver_airbnb_listings') }}
    GROUP BY 
        listing_neighbourhood, month_year
),
change_metrics AS (
    SELECT
        listing_neighbourhood,
        month_year,
        active_listings,
        inactive_listings,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_active_listings,
        LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_inactive_listings,
        ((active_listings - LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 /
        NULLIF(LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0)) AS pct_change_active,
        ((inactive_listings - LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year)) * 100.0 /
        NULLIF(LAG(inactive_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year), 0)) AS pct_change_inactive
    FROM 
        neighbourhood_metrics
)
SELECT
    cm.listing_neighbourhood,
    cm.month_year,
    cm.active_listings,
    cm.inactive_listings,
    nm.min_price,
    nm.max_price,
    nm.avg_price,
    nm.distinct_hosts,
    nm.superhost_rate,
    nm.avg_review_score,
    nm.total_stays,
    nm.estimated_revenue,
    cm.pct_change_active,
    cm.pct_change_inactive
FROM 
    change_metrics cm
JOIN 
    neighbourhood_metrics nm
    ON cm.listing_neighbourhood = nm.listing_neighbourhood
    AND cm.month_year = nm.month_year
ORDER BY 
    cm.listing_neighbourhood, cm.month_year