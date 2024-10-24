-- models/datamart/dm_host_neighbourhood.sql
WITH host_metrics AS (
    SELECT
        host_neighbourhood,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(price * (30 - availability_30)) AS estimated_revenue,
        (SUM(price * (30 - availability_30)) / COUNT(DISTINCT host_id)) AS estimated_revenue_per_host
    FROM {{ ref('silver_airbnb_listings') }}
    WHERE has_availability = 't'
    GROUP BY host_neighbourhood
)
SELECT *
FROM host_metrics