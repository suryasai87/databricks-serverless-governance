-- ============================================================================
-- SQL Alert: High Data Scan Detection
-- ============================================================================
-- Purpose: Detect running queries scanning excessive data (> 10 GB or 1B rows)
-- Schedule: Every 5 minutes
-- Alert Condition: excessive_scan_count > 0
-- ============================================================================

SELECT
    COUNT(*) AS excessive_scan_count,
    MAX(read_bytes / 1024 / 1024 / 1024) AS max_read_gb,
    MAX(read_rows) AS max_read_rows,
    COLLECT_LIST(
        CONCAT(
            executed_by,
            ' | ',
            CAST(ROUND(read_bytes / 1024 / 1024 / 1024, 2) AS STRING),
            ' GB | ',
            CAST(read_rows AS STRING),
            ' rows | ',
            CAST(ROUND(total_duration_ms / 60000, 1) AS STRING),
            ' min'
        )
    ) AS violators
FROM system.query.history
WHERE execution_status = 'RUNNING'
    AND (read_bytes > 10737418240 OR read_rows > 1000000000)  -- 10 GB or 1B rows
    AND start_time > current_timestamp() - INTERVAL 2 DAYS
