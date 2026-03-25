-- ============================================================================
-- SQL Alert: Cost Anomaly Detection (Z-Score Based)
-- ============================================================================
-- Purpose: Detect when today's SQL warehouse spend exceeds 2 standard deviations
--          above the 30-day rolling average.
-- Schedule: Every 60 minutes
-- Alert Condition: is_anomaly = 1
-- ============================================================================

WITH daily_costs AS (
    SELECT
        usage_date,
        SUM(usage_quantity) AS daily_dbu
    FROM system.billing.usage
    WHERE billing_origin_product = 'SQL'
        AND usage_unit = 'DBU'
        AND usage_date >= current_date() - INTERVAL 30 DAYS
    GROUP BY usage_date
),
stats AS (
    SELECT
        AVG(daily_dbu) AS avg_dbu,
        STDDEV(daily_dbu) AS stddev_dbu
    FROM daily_costs
    WHERE usage_date < current_date()
)
SELECT
    ROUND(dc.daily_dbu, 2) AS today_dbu,
    ROUND(s.avg_dbu, 2) AS avg_daily_dbu,
    ROUND(s.stddev_dbu, 2) AS stddev_dbu,
    ROUND((dc.daily_dbu - s.avg_dbu) / NULLIF(s.stddev_dbu, 0), 2) AS z_score,
    CASE WHEN dc.daily_dbu > s.avg_dbu + 2 * s.stddev_dbu THEN 1 ELSE 0 END AS is_anomaly
FROM daily_costs dc
CROSS JOIN stats s
WHERE dc.usage_date = current_date()
