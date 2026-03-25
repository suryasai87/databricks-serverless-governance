# Databricks notebook source
# MAGIC %md
# MAGIC # Serverless SQL Governance - Setup
# MAGIC
# MAGIC This notebook creates the required infrastructure:
# MAGIC 1. Governance catalog/schema
# MAGIC 2. Audit log Delta table
# MAGIC 3. Monitoring views
# MAGIC
# MAGIC **Run this once** before deploying the governance jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "hls_amer_catalog"
SCHEMA = "serverless_governance"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.governance_audit_log"
QUERY_STATS_VIEW = f"{CATALOG}.{SCHEMA}.v_query_performance_stats"
WAREHOUSE_STATS_VIEW = f"{CATALOG}.{SCHEMA}.v_warehouse_activity_stats"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Audit Log Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    event_id STRING,
    event_time TIMESTAMP,
    event_type STRING COMMENT 'QUERY_CANCELLED | WAREHOUSE_STOPPED | COST_ALERT',
    rule_name STRING,
    severity STRING COMMENT 'info | warning | critical',
    target_id STRING COMMENT 'statement_id or warehouse_id',
    target_name STRING COMMENT 'warehouse name or query preview',
    target_user STRING,
    details STRING COMMENT 'JSON with full context',
    action_taken STRING COMMENT 'cancelled | stopped | alert_only | dry_run',
    dry_run BOOLEAN
)
USING DELTA
COMMENT 'Audit log for all serverless governance actions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")
print(f"Audit table {AUDIT_TABLE} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Monitoring Views

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {QUERY_STATS_VIEW} AS
SELECT
    date_trunc('hour', start_time) AS hour,
    execution_status,
    statement_type,
    compute.warehouse_id AS warehouse_id,
    COUNT(*) AS query_count,
    AVG(total_duration_ms) / 1000 AS avg_duration_sec,
    MAX(total_duration_ms) / 1000 AS max_duration_sec,
    SUM(read_bytes) / 1024 / 1024 / 1024 AS total_read_gb,
    SUM(read_rows) AS total_read_rows,
    COUNT(CASE WHEN total_duration_ms > 1800000 THEN 1 END) AS queries_over_30min,
    COUNT(CASE WHEN total_duration_ms > 3600000 THEN 1 END) AS queries_over_1hr
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 7 DAYS
GROUP BY 1, 2, 3, 4
""")
print(f"View {QUERY_STATS_VIEW} ready")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {WAREHOUSE_STATS_VIEW} AS
WITH warehouse_events AS (
    SELECT
        warehouse_id,
        event_type,
        event_time,
        cluster_count,
        LAG(event_time) OVER (PARTITION BY warehouse_id ORDER BY event_time) AS prev_event_time,
        LAG(event_type) OVER (PARTITION BY warehouse_id ORDER BY event_time) AS prev_event_type
    FROM system.compute.warehouse_events
    WHERE event_time > current_timestamp() - INTERVAL 7 DAYS
),
warehouse_activity AS (
    SELECT
        compute.warehouse_id AS warehouse_id,
        MAX(start_time) AS last_query_time,
        COUNT(*) AS query_count_24h,
        AVG(total_duration_ms) / 1000 AS avg_query_duration_sec
    FROM system.query.history
    WHERE start_time > current_timestamp() - INTERVAL 1 DAY
    GROUP BY compute.warehouse_id
)
SELECT
    e.warehouse_id,
    e.event_type AS latest_event,
    e.event_time AS latest_event_time,
    e.cluster_count,
    a.last_query_time,
    a.query_count_24h,
    a.avg_query_duration_sec,
    TIMESTAMPDIFF(MINUTE, a.last_query_time, current_timestamp()) AS idle_minutes
FROM warehouse_events e
LEFT JOIN warehouse_activity a ON e.warehouse_id = a.warehouse_id
WHERE e.event_time = (
    SELECT MAX(event_time)
    FROM warehouse_events e2
    WHERE e2.warehouse_id = e.warehouse_id
)
""")
print(f"View {WAREHOUSE_STATS_VIEW} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Governance Actions Summary View

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_governance_actions_summary AS
SELECT
    date_trunc('day', event_time) AS day,
    event_type,
    severity,
    action_taken,
    COUNT(*) AS action_count,
    COUNT(DISTINCT target_user) AS affected_users
FROM {AUDIT_TABLE}
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2
""")
print(f"Summary view ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

print("=== Setup Complete ===")
print(f"Catalog:          {CATALOG}")
print(f"Schema:           {SCHEMA}")
print(f"Audit Table:      {AUDIT_TABLE}")
print(f"Query Stats View: {QUERY_STATS_VIEW}")
print(f"Warehouse View:   {WAREHOUSE_STATS_VIEW}")
print(f"\nNext steps:")
print(f"  1. Run notebook 01_query_watchdog to test query killing")
print(f"  2. Run notebook 02_warehouse_auditor to test idle detection")
print(f"  3. Run notebook 03_cost_monitor to test cost-based rules")
print(f"  4. Run notebook 04_deploy_jobs to create scheduled Databricks Jobs")
