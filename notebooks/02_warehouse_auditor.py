# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 2: Idle Warehouse Auditor
# MAGIC
# MAGIC **Purpose:** Detect and shut down SQL warehouses that are idle (no active queries)
# MAGIC beyond a configurable threshold.
# MAGIC
# MAGIC **Why this matters:** Even with auto-stop configured, warehouses may remain running if:
# MAGIC - A stale JDBC/ODBC connection holds the warehouse open
# MAGIC - Auto-stop is set too high (e.g., 45 minutes on Pro/Classic)
# MAGIC - Someone manually started a warehouse and forgot about it
# MAGIC
# MAGIC **Schedule:** Every 15 minutes via Databricks Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import uuid
from datetime import datetime

CATALOG = "hls_amer_catalog"
SCHEMA = "serverless_governance"
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.governance_audit_log"
DRY_RUN = False

# Warehouse rules
WAREHOUSE_RULES = [
    {
        "name": "Stop warehouses idle > 10 minutes",
        "enabled": True,
        "idle_threshold_minutes": 10,
        "exempt_warehouses": [],  # Add warehouse names to exempt
        "exempt_warehouse_ids": [],
        "warehouse_type_filter": None  # None = all types
    },
    {
        "name": "Stop non-serverless warehouses idle > 30 minutes",
        "enabled": True,
        "idle_threshold_minutes": 30,
        "exempt_warehouses": [],
        "exempt_warehouse_ids": [],
        "warehouse_type_filter": "PRO"
    }
]

print(f"Warehouse Auditor initialized | Dry Run: {DRY_RUN}")
print(f"Rules loaded: {len(WAREHOUSE_RULES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: Warehouse Idle Detection Engine

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def get_running_warehouses():
    """Get all currently running warehouses via the Warehouses API."""
    warehouses = list(w.warehouses.list())
    running = [wh for wh in warehouses if wh.state and wh.state.value == "RUNNING"]
    print(f"Found {len(running)} running warehouses out of {len(warehouses)} total")
    return running


def get_warehouse_last_query_time(warehouse_id):
    """Query system.query.history to find when the last query ran on this warehouse."""
    query = f"""
    SELECT
        MAX(start_time) AS last_query_time,
        COUNT(*) AS recent_query_count
    FROM system.query.history
    WHERE compute.warehouse_id = '{warehouse_id}'
        AND start_time > current_timestamp() - INTERVAL 1 DAY
    """
    try:
        result = spark.sql(query).collect()
        if result and result[0]["last_query_time"]:
            return result[0]["last_query_time"], result[0]["recent_query_count"]
    except Exception as e:
        print(f"  Warning: Could not query history for {warehouse_id}: {e}")
    return None, 0


def get_active_query_count(warehouse_id):
    """Check if there are currently running queries on this warehouse."""
    query = f"""
    SELECT COUNT(*) AS running_count
    FROM system.query.history
    WHERE compute.warehouse_id = '{warehouse_id}'
        AND execution_status = 'RUNNING'
        AND start_time > current_timestamp() - INTERVAL 2 DAYS
    """
    try:
        result = spark.sql(query).collect()
        return result[0]["running_count"] if result else 0
    except Exception:
        return -1  # Unknown


def stop_warehouse(warehouse_id, warehouse_name, rule_name, idle_minutes, dry_run=False):
    """Stop a warehouse and log the action."""
    action = "dry_run" if dry_run else "stopped"

    if not dry_run:
        try:
            w.warehouses.stop(id=warehouse_id)
            print(f"  STOPPED: {warehouse_name} ({warehouse_id})")
        except Exception as e:
            action = f"stop_failed: {str(e)[:200]}"
            print(f"  FAILED to stop {warehouse_name}: {e}")
    else:
        print(f"  [DRY RUN] Would stop: {warehouse_name} ({warehouse_id})")

    return {
        "event_id": str(uuid.uuid4())[:8],
        "event_time": datetime.utcnow().isoformat(),
        "event_type": "WAREHOUSE_STOPPED",
        "rule_name": rule_name,
        "severity": "warning",
        "target_id": warehouse_id,
        "target_name": warehouse_name,
        "target_user": "system",
        "details": json.dumps({
            "idle_minutes": idle_minutes,
            "action": action
        }),
        "action_taken": action,
        "dry_run": dry_run
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Warehouse Audit

# COMMAND ----------

all_audit_records = []
running_warehouses = get_running_warehouses()

for wh in running_warehouses:
    wh_name = wh.name or "unnamed"
    wh_id = wh.id
    wh_type = wh.warehouse_type.value if wh.warehouse_type else "UNKNOWN"
    is_serverless = wh.enable_serverless_compute or False
    auto_stop = wh.auto_stop_mins or 0

    print(f"\n--- {wh_name} ({wh_id}) ---")
    print(f"  Type: {wh_type} | Serverless: {is_serverless} | Auto-stop: {auto_stop}min")

    # Check for active queries first
    active_count = get_active_query_count(wh_id)
    if active_count > 0:
        print(f"  SKIP: {active_count} queries currently running")
        continue

    # Get last query time
    last_query_time, recent_count = get_warehouse_last_query_time(wh_id)
    if last_query_time:
        idle_minutes = (datetime.utcnow() - last_query_time.replace(tzinfo=None)).total_seconds() / 60
        print(f"  Last query: {last_query_time} ({idle_minutes:.0f} min ago)")
        print(f"  Queries in last 24h: {recent_count}")
    else:
        idle_minutes = 999  # No queries found at all
        print(f"  No queries found in last 24 hours")

    # Evaluate rules
    for rule in WAREHOUSE_RULES:
        if not rule.get("enabled", True):
            continue

        # Check exemptions
        if wh_name in rule.get("exempt_warehouses", []):
            print(f"  EXEMPT by name for rule: {rule['name']}")
            continue
        if wh_id in rule.get("exempt_warehouse_ids", []):
            print(f"  EXEMPT by ID for rule: {rule['name']}")
            continue

        # Check warehouse type filter
        type_filter = rule.get("warehouse_type_filter")
        if type_filter:
            if type_filter == "PRO" and is_serverless:
                continue
            if type_filter == "SERVERLESS" and not is_serverless:
                continue

        # Check idle threshold
        if idle_minutes >= rule["idle_threshold_minutes"]:
            print(f"  VIOLATION: {rule['name']} (idle {idle_minutes:.0f}min > threshold {rule['idle_threshold_minutes']}min)")
            audit_record = stop_warehouse(
                warehouse_id=wh_id,
                warehouse_name=wh_name,
                rule_name=rule["name"],
                idle_minutes=round(idle_minutes),
                dry_run=DRY_RUN
            )
            all_audit_records.append(audit_record)
            break  # Only apply first matching rule per warehouse

print(f"\n{'='*60}")
print(f"SUMMARY: {len(all_audit_records)} warehouses {'would be ' if DRY_RUN else ''}stopped")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Log

# COMMAND ----------

if all_audit_records:
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType

    audit_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("target_id", StringType(), True),
        StructField("target_name", StringType(), True),
        StructField("target_user", StringType(), True),
        StructField("details", StringType(), True),
        StructField("action_taken", StringType(), True),
        StructField("dry_run", BooleanType(), True),
    ])

    audit_df = spark.createDataFrame(all_audit_records, schema=audit_schema)
    audit_df = audit_df.withColumn("event_time", audit_df.event_time.cast("timestamp"))

    try:
        audit_df.write.mode("append").saveAsTable(AUDIT_TABLE)
        print(f"Wrote {len(all_audit_records)} audit records to {AUDIT_TABLE}")
    except Exception as e:
        print(f"WARNING: Could not write audit log: {e}")
        for r in all_audit_records:
            print(f"  {json.dumps(r)}")
else:
    print("No audit records to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warehouse Status Report

# COMMAND ----------

display(spark.sql("""
WITH warehouse_activity AS (
    SELECT
        compute.warehouse_id AS warehouse_id,
        MAX(start_time) AS last_query_time,
        COUNT(*) AS queries_24h,
        COUNT(CASE WHEN execution_status = 'RUNNING' THEN 1 END) AS running_now
    FROM system.query.history
    WHERE start_time > current_timestamp() - INTERVAL 1 DAY
    GROUP BY compute.warehouse_id
)
SELECT
    e.warehouse_id,
    e.event_type AS status,
    e.cluster_count,
    a.last_query_time,
    TIMESTAMPDIFF(MINUTE, a.last_query_time, current_timestamp()) AS idle_minutes,
    a.queries_24h,
    a.running_now
FROM system.compute.warehouse_events e
LEFT JOIN warehouse_activity a ON e.warehouse_id = a.warehouse_id
WHERE e.event_time = (
    SELECT MAX(e2.event_time)
    FROM system.compute.warehouse_events e2
    WHERE e2.warehouse_id = e.warehouse_id
)
ORDER BY a.running_now DESC, idle_minutes DESC
"""))
