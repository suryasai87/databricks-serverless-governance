-- ============================================================================
-- SQL Alert: Idle Warehouse Monitor
-- ============================================================================
-- Purpose: Detect warehouses running with no queries for 30+ minutes
-- Schedule: Every 15 minutes
-- Alert Condition: idle_warehouse_count > 0
-- ============================================================================

WITH latest_warehouse_events AS (
    SELECT
        warehouse_id,
        event_type,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY event_time DESC) AS rn
    FROM system.compute.warehouse_events
    WHERE event_time > current_timestamp() - INTERVAL 1 DAY
),
running_warehouses AS (
    SELECT warehouse_id, event_time AS running_since
    FROM latest_warehouse_events
    WHERE rn = 1 AND event_type IN ('RUNNING', 'SCALED_UP')
),
warehouse_activity AS (
    SELECT
        compute.warehouse_id AS warehouse_id,
        MAX(start_time) AS last_query_time,
        COUNT(*) AS queries_1h
    FROM system.query.history
    WHERE start_time > current_timestamp() - INTERVAL 1 HOUR
    GROUP BY compute.warehouse_id
)
SELECT
    COUNT(*) AS idle_warehouse_count,
    COLLECT_LIST(
        CONCAT(
            rw.warehouse_id,
            ' (idle ',
            CAST(COALESCE(
                TIMESTAMPDIFF(MINUTE, wa.last_query_time, current_timestamp()),
                999
            ) AS STRING),
            ' min)'
        )
    ) AS idle_warehouse_details
FROM running_warehouses rw
LEFT JOIN warehouse_activity wa ON rw.warehouse_id = wa.warehouse_id
WHERE wa.last_query_time IS NULL
    OR TIMESTAMPDIFF(MINUTE, wa.last_query_time, current_timestamp()) > 30
