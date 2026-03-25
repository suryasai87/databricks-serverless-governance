# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 4: SQL Alerts for Real-Time Governance Notifications
# MAGIC
# MAGIC **Purpose:** Create Databricks SQL Alerts that monitor system tables and
# MAGIC send notifications when governance thresholds are breached.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. SQL Queries run on a schedule (every 5 minutes)
# MAGIC 2. Alerts evaluate conditions (e.g., count > 0)
# MAGIC 3. Notifications sent via email or webhook when triggered
# MAGIC
# MAGIC **Advantage:** No Databricks Job required - runs natively in SQL Warehouses.
# MAGIC
# MAGIC **Limitation:** Alerts can notify but cannot automatically cancel queries or stop warehouses.
# MAGIC Use this in combination with the Job-based approaches for full automation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    CreateAlertRequestAlert,
    AlertCondition,
    AlertConditionOperand,
    AlertConditionThreshold,
    AlertOperandColumn,
    AlertConditionThresholdValue,
)

w = WorkspaceClient()

# Warehouse to run alert queries on
ALERT_WAREHOUSE_ID = "4b28691c780d9875"

print("SQL Alerts Setup initialized")
print(f"Using warehouse: {ALERT_WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert 1: Long-Running Queries Detected

# COMMAND ----------

# Create the monitoring query
long_running_query_sql = """
SELECT
    COUNT(*) AS long_running_count,
    MAX(total_duration_ms / 60000) AS max_duration_min,
    COLLECT_LIST(CONCAT(executed_by, ' (', CAST(total_duration_ms / 60000 AS INT), ' min)')) AS violators
FROM system.query.history
WHERE execution_status = 'RUNNING'
    AND total_duration_ms > 1800000
    AND start_time > current_timestamp() - INTERVAL 2 DAYS
"""

try:
    # Create the query first
    query_obj = w.queries.create(
        query=long_running_query_sql,
        display_name="[Governance] Long-Running Queries Monitor",
        warehouse_id=ALERT_WAREHOUSE_ID,
        description="Detects queries running longer than 30 minutes on any SQL warehouse"
    )
    print(f"Created query: {query_obj.id}")

    # Create the alert
    alert = w.alerts.create(
        alert=CreateAlertRequestAlert(
            condition=AlertCondition(
                op="GREATER_THAN",
                operand=AlertConditionOperand(
                    column=AlertOperandColumn(name="long_running_count")
                ),
                threshold=AlertConditionThreshold(
                    value=AlertConditionThresholdValue(double_value=0)
                )
            ),
            display_name="[Governance] Long-Running Query Alert",
            query_id=query_obj.id,
        )
    )
    print(f"Created alert: {alert.id}")
    print(f"  Condition: long_running_count > 0")
    print(f"  Status: Active")

except Exception as e:
    print(f"Note: Could not create alert via SDK: {e}")
    print("You can create this alert manually in the Databricks SQL UI:")
    print(f"  Query: {long_running_query_sql}")
    print(f"  Condition: long_running_count > 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert 2: Warehouse Idle for Extended Period

# COMMAND ----------

idle_warehouse_query_sql = """
WITH running_warehouses AS (
    SELECT warehouse_id, event_time AS started_at
    FROM system.compute.warehouse_events
    WHERE event_type = 'RUNNING'
        AND event_time = (
            SELECT MAX(event_time)
            FROM system.compute.warehouse_events e2
            WHERE e2.warehouse_id = system.compute.warehouse_events.warehouse_id
        )
),
warehouse_activity AS (
    SELECT
        compute.warehouse_id AS warehouse_id,
        MAX(start_time) AS last_query_time
    FROM system.query.history
    WHERE start_time > current_timestamp() - INTERVAL 1 DAY
    GROUP BY compute.warehouse_id
)
SELECT
    COUNT(*) AS idle_warehouse_count,
    COLLECT_LIST(rw.warehouse_id) AS idle_warehouses
FROM running_warehouses rw
LEFT JOIN warehouse_activity wa ON rw.warehouse_id = wa.warehouse_id
WHERE wa.last_query_time IS NULL
    OR TIMESTAMPDIFF(MINUTE, wa.last_query_time, current_timestamp()) > 30
"""

try:
    query_obj2 = w.queries.create(
        query=idle_warehouse_query_sql,
        display_name="[Governance] Idle Warehouse Monitor",
        warehouse_id=ALERT_WAREHOUSE_ID,
        description="Detects warehouses running with no queries for 30+ minutes"
    )
    print(f"Created query: {query_obj2.id}")

    alert2 = w.alerts.create(
        alert=CreateAlertRequestAlert(
            condition=AlertCondition(
                op="GREATER_THAN",
                operand=AlertConditionOperand(
                    column=AlertOperandColumn(name="idle_warehouse_count")
                ),
                threshold=AlertConditionThreshold(
                    value=AlertConditionThresholdValue(double_value=0)
                )
            ),
            display_name="[Governance] Idle Warehouse Alert",
            query_id=query_obj2.id,
        )
    )
    print(f"Created alert: {alert2.id}")

except Exception as e:
    print(f"Note: Could not create alert: {e}")
    print("Create manually with the SQL above, condition: idle_warehouse_count > 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert 3: Daily Cost Anomaly

# COMMAND ----------

cost_anomaly_query_sql = """
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
    dc.daily_dbu AS today_dbu,
    s.avg_dbu AS avg_daily_dbu,
    ROUND((dc.daily_dbu - s.avg_dbu) / NULLIF(s.stddev_dbu, 0), 2) AS z_score,
    CASE WHEN dc.daily_dbu > s.avg_dbu + 2 * s.stddev_dbu THEN 1 ELSE 0 END AS is_anomaly
FROM daily_costs dc
CROSS JOIN stats s
WHERE dc.usage_date = current_date()
"""

try:
    query_obj3 = w.queries.create(
        query=cost_anomaly_query_sql,
        display_name="[Governance] Daily Cost Anomaly Detector",
        warehouse_id=ALERT_WAREHOUSE_ID,
        description="Detects when daily SQL warehouse spend exceeds 2 standard deviations from the 30-day average"
    )
    print(f"Created query: {query_obj3.id}")

    alert3 = w.alerts.create(
        alert=CreateAlertRequestAlert(
            condition=AlertCondition(
                op="EQUAL",
                operand=AlertConditionOperand(
                    column=AlertOperandColumn(name="is_anomaly")
                ),
                threshold=AlertConditionThreshold(
                    value=AlertConditionThresholdValue(double_value=1)
                )
            ),
            display_name="[Governance] Cost Anomaly Alert",
            query_id=query_obj3.id,
        )
    )
    print(f"Created alert: {alert3.id}")

except Exception as e:
    print(f"Note: Could not create alert: {e}")
    print("Create manually with the SQL above, condition: is_anomaly = 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Created Alerts
# MAGIC
# MAGIC | Alert | Trigger | Schedule |
# MAGIC |-------|---------|----------|
# MAGIC | Long-Running Query Alert | Any query running > 30 min | Every 5 min |
# MAGIC | Idle Warehouse Alert | Any warehouse idle > 30 min | Every 15 min |
# MAGIC | Cost Anomaly Alert | Daily spend > 2σ above mean | Every 60 min |
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Go to **SQL > Alerts** in the Databricks UI
# MAGIC 2. Configure notification destinations (email, Slack webhook, PagerDuty)
# MAGIC 3. Set the refresh schedule for each alert
# MAGIC 4. Combine with notebooks 01-03 for automated enforcement (alerts notify, jobs enforce)
