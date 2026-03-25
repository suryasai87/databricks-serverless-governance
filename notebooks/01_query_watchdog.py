# Databricks notebook source
# MAGIC %md
# MAGIC # Approach 1: Serverless Query Watchdog
# MAGIC
# MAGIC **Purpose:** Automatically detect and cancel underperforming queries on Serverless SQL warehouses.
# MAGIC
# MAGIC Since Databricks Query Watchdog is NOT available for Serverless warehouses,
# MAGIC this notebook provides equivalent functionality via system tables + Statement Execution API.
# MAGIC
# MAGIC **Schedule:** Every 5 minutes via Databricks Job
# MAGIC
# MAGIC ## Rules Engine
# MAGIC - Kill queries exceeding duration thresholds
# MAGIC - Kill queries exceeding data scan thresholds (bytes/rows)
# MAGIC - Tag-based conditional rules (e.g., low-priority queries get shorter timeout)
# MAGIC - Exclusion lists for users, statement types, and specific warehouses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import yaml
import json
import uuid
from datetime import datetime, timedelta

# Load config - try widget param first, then default
try:
    config_path = dbutils.widgets.get("config_path")
except:
    config_path = ""

# Default configuration (can be overridden by YAML config file)
DEFAULT_CONFIG = {
    "general": {
        "catalog": "hls_amer_catalog",
        "schema": "serverless_governance",
        "audit_table": "governance_audit_log",
        "dry_run": False
    },
    "query_rules": [
        {
            "name": "Kill queries running > 30 minutes",
            "enabled": True,
            "condition": {"max_duration_minutes": 30},
            "exclude": {
                "users": ["system@databricks.com"],
                "statement_types": ["REFRESH"]
            },
            "severity": "warning"
        },
        {
            "name": "Kill queries running > 60 minutes (all types)",
            "enabled": True,
            "condition": {"max_duration_minutes": 60},
            "exclude": {"users": [], "statement_types": []},
            "severity": "critical"
        },
        {
            "name": "Kill queries reading > 10 GB",
            "enabled": True,
            "condition": {"max_read_bytes_gb": 10},
            "exclude": {"users": [], "statement_types": ["REFRESH"]},
            "severity": "warning"
        },
        {
            "name": "Kill queries scanning > 1 billion rows",
            "enabled": True,
            "condition": {"max_read_rows": 1000000000},
            "exclude": {"users": [], "statement_types": []},
            "severity": "critical"
        },
        {
            "name": "Kill low-priority queries after 15 minutes",
            "enabled": True,
            "condition": {
                "max_duration_minutes": 15,
                "query_tag_filter": {"key": "priority", "value": "low"}
            },
            "exclude": {"users": [], "statement_types": []},
            "severity": "info"
        }
    ]
}

config = DEFAULT_CONFIG
CATALOG = config["general"]["catalog"]
SCHEMA = config["general"]["schema"]
AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.{config['general']['audit_table']}"
DRY_RUN = config["general"]["dry_run"]

print(f"Query Watchdog initialized | Dry Run: {DRY_RUN}")
print(f"Audit Table: {AUDIT_TABLE}")
print(f"Rules loaded: {len(config['query_rules'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core: Query Watchdog Engine

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def build_rule_query(rule):
    """Build a SQL query to find queries violating a specific rule."""
    condition = rule["condition"]
    exclude = rule.get("exclude", {})

    where_clauses = [
        "execution_status = 'RUNNING'",
        "start_time > current_timestamp() - INTERVAL 2 DAYS"
    ]

    # Duration condition
    if "max_duration_minutes" in condition:
        ms = condition["max_duration_minutes"] * 60 * 1000
        where_clauses.append(f"total_duration_ms > {ms}")

    # Bytes read condition
    if "max_read_bytes_gb" in condition:
        bytes_val = int(condition["max_read_bytes_gb"] * 1024 * 1024 * 1024)
        where_clauses.append(f"read_bytes > {bytes_val}")

    # Rows read condition
    if "max_read_rows" in condition:
        where_clauses.append(f"read_rows > {condition['max_read_rows']}")

    # Query tag filter
    if "query_tag_filter" in condition:
        tag = condition["query_tag_filter"]
        where_clauses.append(f"query_tags['{tag['key']}'] = '{tag['value']}'")

    # Exclusions
    excluded_users = exclude.get("users", [])
    if excluded_users:
        user_list = ",".join(f"'{u}'" for u in excluded_users)
        where_clauses.append(f"executed_by NOT IN ({user_list})")

    excluded_types = exclude.get("statement_types", [])
    if excluded_types:
        type_list = ",".join(f"'{t}'" for t in excluded_types)
        where_clauses.append(f"statement_type NOT IN ({type_list})")

    where_sql = " AND ".join(where_clauses)

    return f"""
    SELECT
        statement_id,
        executed_by,
        statement_type,
        execution_status,
        total_duration_ms,
        total_duration_ms / 1000 AS duration_sec,
        total_duration_ms / 60000 AS duration_min,
        read_bytes,
        read_bytes / 1024 / 1024 / 1024 AS read_gb,
        read_rows,
        compute.warehouse_id AS warehouse_id,
        start_time,
        SUBSTRING(statement_text, 1, 300) AS query_preview
    FROM system.query.history
    WHERE {where_sql}
    ORDER BY total_duration_ms DESC
    """


def cancel_query(statement_id, rule_name, row_data, dry_run=False):
    """Cancel a running query and log the action."""
    action = "dry_run" if dry_run else "cancelled"

    if not dry_run:
        try:
            w.statement_execution.cancel_execution(statement_id=statement_id)
            print(f"  CANCELLED: {statement_id}")
        except Exception as e:
            action = f"cancel_failed: {str(e)[:200]}"
            print(f"  FAILED to cancel {statement_id}: {e}")
    else:
        print(f"  [DRY RUN] Would cancel: {statement_id}")

    return {
        "event_id": str(uuid.uuid4())[:8],
        "event_time": datetime.utcnow().isoformat(),
        "event_type": "QUERY_CANCELLED",
        "rule_name": rule_name,
        "severity": row_data.get("severity", "warning"),
        "target_id": statement_id,
        "target_name": str(row_data.get("query_preview", ""))[:300],
        "target_user": str(row_data.get("executed_by", "")),
        "details": json.dumps({
            "duration_min": row_data.get("duration_min"),
            "read_gb": row_data.get("read_gb"),
            "read_rows": row_data.get("read_rows"),
            "warehouse_id": row_data.get("warehouse_id"),
            "statement_type": row_data.get("statement_type")
        }),
        "action_taken": action,
        "dry_run": dry_run
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Query Watchdog

# COMMAND ----------

all_audit_records = []
total_cancelled = 0

for rule in config["query_rules"]:
    if not rule.get("enabled", True):
        print(f"SKIP (disabled): {rule['name']}")
        continue

    print(f"\n{'='*60}")
    print(f"RULE: {rule['name']}")
    print(f"{'='*60}")

    query = build_rule_query(rule)

    try:
        result_df = spark.sql(query)
        violations = result_df.collect()

        if not violations:
            print(f"  No violations found")
            continue

        print(f"  Found {len(violations)} violating queries:")

        for row in violations:
            row_dict = row.asDict()
            row_dict["severity"] = rule.get("severity", "warning")

            print(f"  - {row_dict['statement_id']} | "
                  f"{row_dict['executed_by']} | "
                  f"{row_dict['duration_min']:.1f} min | "
                  f"{row_dict.get('read_gb', 0):.2f} GB | "
                  f"{row_dict['statement_type']}")

            audit_record = cancel_query(
                statement_id=row_dict["statement_id"],
                rule_name=rule["name"],
                row_data=row_dict,
                dry_run=DRY_RUN
            )
            all_audit_records.append(audit_record)
            total_cancelled += 1

    except Exception as e:
        print(f"  ERROR evaluating rule: {e}")

print(f"\n{'='*60}")
print(f"SUMMARY: {total_cancelled} queries {'would be ' if DRY_RUN else ''}cancelled")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Audit Log

# COMMAND ----------

if all_audit_records:
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

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
        print("Audit records (logged to stdout):")
        for r in all_audit_records:
            print(f"  {json.dumps(r)}")
else:
    print("No audit records to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Running Queries Report (Informational)

# COMMAND ----------

display(spark.sql("""
SELECT
    statement_id,
    executed_by,
    statement_type,
    execution_status,
    total_duration_ms / 60000 AS duration_minutes,
    read_bytes / 1024 / 1024 / 1024 AS read_gb,
    read_rows,
    compute.warehouse_id AS warehouse_id,
    start_time,
    SUBSTRING(statement_text, 1, 200) AS query_preview
FROM system.query.history
WHERE execution_status = 'RUNNING'
    AND start_time > current_timestamp() - INTERVAL 2 DAYS
ORDER BY total_duration_ms DESC
LIMIT 20
"""))
