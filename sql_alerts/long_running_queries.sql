-- ============================================================================
-- SQL Alert: Long-Running Queries Monitor
-- ============================================================================
-- Purpose: Detect queries running longer than 30 minutes
-- Schedule: Every 5 minutes
-- Alert Condition: long_running_count > 0
-- ============================================================================

SELECT
    COUNT(*) AS long_running_count,
    MAX(total_duration_ms / 60000) AS max_duration_min,
    COLLECT_LIST(
        CONCAT(
            executed_by,
            ' | ',
            statement_type,
            ' | ',
            CAST(ROUND(total_duration_ms / 60000, 1) AS STRING),
            ' min | ',
            COALESCE(compute.warehouse_id, 'serverless')
        )
    ) AS violators
FROM system.query.history
WHERE execution_status = 'RUNNING'
    AND total_duration_ms > 1800000  -- 30 minutes
    AND start_time > current_timestamp() - INTERVAL 2 DAYS
