# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 3: Cost-Based Warehouse Monitor
# MAGIC
# MAGIC **Purpose:** Monitor warehouse spending via `system.billing.usage` and take
# MAGIC automated actions when cost thresholds are exceeded.
# MAGIC
# MAGIC **Key Insight:** Databricks Budget Policies only send email notifications (with up to 24h delay)
# MAGIC and do NOT enforce limits. This notebook provides real-time cost enforcement.
# MAGIC
# MAGIC **Schedule:** Every 60 minutes via Databricks Job

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

COST_RULES = [
    {
        "name": "Stop warehouse exceeding 500 DBU daily",
        "enabled": True,
        "max_daily_dbu": 500,
        "action": "stop",
        "exempt_warehouses": []
    },
    {
        "name": "Alert on warehouse exceeding 50 DBU/hour",
        "enabled": True,
        "max_hourly_dbu": 50,
        "action": "alert_only",
        "exempt_warehouses": []
    }
]

print(f"Cost Monitor initialized | Dry Run: {DRY_RUN}")
print(f"Cost rules loaded: {len(COST_RULES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: Cost Analysis Engine

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def get_daily_warehouse_costs():
    """Query billing system table for today's warehouse DBU usage."""
    query = """
    SELECT
        usage_metadata.warehouse_id AS warehouse_id,
        sku_name,
        SUM(usage_quantity) AS total_dbu,
        COUNT(*) AS billing_records
    FROM system.billing.usage
    WHERE usage_date = current_date()
        AND billing_origin_product = 'SQL'
        AND usage_unit = 'DBU'
        AND usage_metadata.warehouse_id IS NOT NULL
    GROUP BY usage_metadata.warehouse_id, sku_name
    ORDER BY total_dbu DESC
    """
    try:
        return spark.sql(query).collect()
    except Exception as e:
        print(f"Warning: Could not query billing data: {e}")
        return []


def get_hourly_warehouse_costs():
    """Query billing for the last hour's warehouse DBU usage."""
    query = """
    SELECT
        usage_metadata.warehouse_id AS warehouse_id,
        sku_name,
        SUM(usage_quantity) AS hourly_dbu
    FROM system.billing.usage
    WHERE usage_date = current_date()
        AND usage_end_time > current_timestamp() - INTERVAL 1 HOUR
        AND billing_origin_product = 'SQL'
        AND usage_unit = 'DBU'
        AND usage_metadata.warehouse_id IS NOT NULL
    GROUP BY usage_metadata.warehouse_id, sku_name
    ORDER BY hourly_dbu DESC
    """
    try:
        return spark.sql(query).collect()
    except Exception as e:
        print(f"Warning: Could not query hourly billing data: {e}")
        return []


def get_warehouse_name_map():
    """Build a mapping of warehouse_id -> warehouse_name."""
    warehouses = list(w.warehouses.list())
    return {wh.id: wh.name for wh in warehouses}


def stop_warehouse_for_cost(warehouse_id, warehouse_name, rule_name, dbu_usage, dry_run=False):
    """Stop a warehouse due to cost threshold and log the action."""
    action = "dry_run" if dry_run else "stopped"

    if not dry_run:
        try:
            w.warehouses.stop(id=warehouse_id)
            print(f"  STOPPED: {warehouse_name} ({warehouse_id}) - {dbu_usage:.1f} DBU")
        except Exception as e:
            action = f"stop_failed: {str(e)[:200]}"
            print(f"  FAILED to stop {warehouse_name}: {e}")
    else:
        print(f"  [DRY RUN] Would stop: {warehouse_name} - {dbu_usage:.1f} DBU")

    return {
        "event_id": str(uuid.uuid4())[:8],
        "event_time": datetime.utcnow().isoformat(),
        "event_type": "COST_ALERT",
        "rule_name": rule_name,
        "severity": "critical",
        "target_id": warehouse_id,
        "target_name": warehouse_name,
        "target_user": "system",
        "details": json.dumps({
            "dbu_usage": round(dbu_usage, 2),
            "action": action
        }),
        "action_taken": action,
        "dry_run": dry_run
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Cost Monitor

# COMMAND ----------

all_audit_records = []
warehouse_names = get_warehouse_name_map()

# --- Daily DBU Rules ---
print("=" * 60)
print("DAILY DBU ANALYSIS")
print("=" * 60)

daily_costs = get_daily_warehouse_costs()
daily_by_wh = {}
for row in daily_costs:
    wh_id = row["warehouse_id"]
    daily_by_wh[wh_id] = daily_by_wh.get(wh_id, 0) + (row["total_dbu"] or 0)

for wh_id, total_dbu in sorted(daily_by_wh.items(), key=lambda x: x[1], reverse=True):
    wh_name = warehouse_names.get(wh_id, wh_id)
    print(f"  {wh_name}: {total_dbu:.1f} DBU today")

for rule in COST_RULES:
    if not rule.get("enabled") or "max_daily_dbu" not in rule:
        continue

    print(f"\nRULE: {rule['name']} (threshold: {rule['max_daily_dbu']} DBU)")

    for wh_id, total_dbu in daily_by_wh.items():
        wh_name = warehouse_names.get(wh_id, wh_id)

        if wh_name in rule.get("exempt_warehouses", []):
            continue

        if total_dbu > rule["max_daily_dbu"]:
            print(f"  VIOLATION: {wh_name} = {total_dbu:.1f} DBU > {rule['max_daily_dbu']}")

            if rule["action"] == "stop":
                audit_record = stop_warehouse_for_cost(
                    warehouse_id=wh_id,
                    warehouse_name=wh_name,
                    rule_name=rule["name"],
                    dbu_usage=total_dbu,
                    dry_run=DRY_RUN
                )
                all_audit_records.append(audit_record)
            else:
                print(f"  ALERT ONLY: {wh_name} exceeding daily budget")
                all_audit_records.append({
                    "event_id": str(uuid.uuid4())[:8],
                    "event_time": datetime.utcnow().isoformat(),
                    "event_type": "COST_ALERT",
                    "rule_name": rule["name"],
                    "severity": "warning",
                    "target_id": wh_id,
                    "target_name": wh_name,
                    "target_user": "system",
                    "details": json.dumps({"dbu_usage": round(total_dbu, 2), "action": "alert_only"}),
                    "action_taken": "alert_only",
                    "dry_run": DRY_RUN
                })

# --- Hourly DBU Rules ---
print(f"\n{'='*60}")
print("HOURLY DBU ANALYSIS")
print("=" * 60)

hourly_costs = get_hourly_warehouse_costs()
for row in hourly_costs:
    wh_name = warehouse_names.get(row["warehouse_id"], row["warehouse_id"])
    print(f"  {wh_name}: {row['hourly_dbu']:.1f} DBU (last hour)")

for rule in COST_RULES:
    if not rule.get("enabled") or "max_hourly_dbu" not in rule:
        continue

    print(f"\nRULE: {rule['name']} (threshold: {rule['max_hourly_dbu']} DBU/hr)")

    for row in hourly_costs:
        wh_id = row["warehouse_id"]
        wh_name = warehouse_names.get(wh_id, wh_id)
        hourly_dbu = row["hourly_dbu"] or 0

        if wh_name in rule.get("exempt_warehouses", []):
            continue

        if hourly_dbu > rule["max_hourly_dbu"]:
            print(f"  VIOLATION: {wh_name} = {hourly_dbu:.1f} DBU/hr > {rule['max_hourly_dbu']}")
            all_audit_records.append({
                "event_id": str(uuid.uuid4())[:8],
                "event_time": datetime.utcnow().isoformat(),
                "event_type": "COST_ALERT",
                "rule_name": rule["name"],
                "severity": "warning",
                "target_id": wh_id,
                "target_name": wh_name,
                "target_user": "system",
                "details": json.dumps({"hourly_dbu": round(hourly_dbu, 2), "action": rule["action"]}),
                "action_taken": rule["action"],
                "dry_run": DRY_RUN
            })

print(f"\n{'='*60}")
print(f"SUMMARY: {len(all_audit_records)} cost events detected")
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
    print("No cost events to log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Dashboard Report

# COMMAND ----------

display(spark.sql("""
SELECT
    usage_date,
    usage_metadata.warehouse_id AS warehouse_id,
    sku_name,
    SUM(usage_quantity) AS total_dbu,
    COUNT(*) AS records
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
    AND billing_origin_product = 'SQL'
    AND usage_unit = 'DBU'
    AND usage_metadata.warehouse_id IS NOT NULL
GROUP BY usage_date, usage_metadata.warehouse_id, sku_name
ORDER BY usage_date DESC, total_dbu DESC
"""))
