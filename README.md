# Databricks Serverless SQL Governance Automation

Automated rules engine for Databricks SQL Serverless warehouses — kill underperforming queries, shut down idle warehouses, and enforce cost budgets.

## Why This Exists

Databricks provides **Query Watchdog** for Pro/Classic SQL warehouses, but **it is NOT available for Serverless SQL warehouses**. Additionally, built-in **Budget Policies** only send email notifications (with up to 24-hour delay) and **do not enforce limits**.

This project fills those gaps with:
- **Custom Query Watchdog** for Serverless (rule-based query killing)
- **Idle Warehouse Auditor** (stop warehouses with no active queries)
- **Cost-Based Monitor** (real-time DBU budget enforcement)
- **SQL Alerts** (native notification layer)
- **Unified Audit Log** (Delta table tracking all governance actions)

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                Databricks Jobs (Scheduled)                  │
│                                                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │ 01 Query     │  │ 02 Warehouse │  │ 03 Cost          │ │
│  │ Watchdog     │  │ Auditor      │  │ Monitor          │ │
│  │ (every 5min) │  │ (every 15min)│  │ (every 60min)    │ │
│  │              │  │              │  │                  │ │
│  │ system.query │  │ Warehouses   │  │ system.billing   │ │
│  │ .history     │  │ API + Query  │  │ .usage           │ │
│  │ → cancel API │  │ History →    │  │ → stop/alert     │ │
│  │              │  │ stop API     │  │                  │ │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘ │
│         │                 │                    │           │
│         └────────────┬────┴────────────────────┘           │
│                      ▼                                     │
│  ┌────────────────────────────────────────────────────┐    │
│  │        Audit Log (Delta Table)                     │    │
│  │  hls_amer_catalog.serverless_governance            │    │
│  │  .governance_audit_log                             │    │
│  └────────────────────────────────────────────────────┘    │
│                      │                                     │
│  ┌───────────────────┴───────────────────────────────┐     │
│  │       04 SQL Alerts (Native Notifications)        │     │
│  │  • Long-running query alert                       │     │
│  │  • Idle warehouse alert                           │     │
│  │  • Cost anomaly alert (z-score)                   │     │
│  │  • High data scan alert                           │     │
│  └───────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Access to `system.query.history`, `system.billing.usage`, `system.compute.warehouse_events`
- Service Principal or user with permissions to stop warehouses and cancel statements

### Step 1: Deploy Notebooks to Workspace
```bash
databricks workspace import-dir ./notebooks /Workspace/Apps/serverless-governance \
    --profile DEFAULT --overwrite
```

### Step 2: Run Setup
Run `00_setup` notebook in Databricks to create:
- `hls_amer_catalog.serverless_governance` schema
- `governance_audit_log` Delta table
- Monitoring views

### Step 3: Test Each Approach (Dry Run)
Set `DRY_RUN = True` in each notebook, then run:
1. `01_query_watchdog` — see which queries *would* be killed
2. `02_warehouse_auditor` — see which warehouses *would* be stopped
3. `03_cost_monitor` — see which warehouses exceed budgets

### Step 4: Deploy Scheduled Jobs
Run `05_deploy_jobs` notebook to create Databricks Jobs (created in PAUSED state):
- `serverless-governance-query-watchdog` — every 5 minutes
- `serverless-governance-warehouse-auditor` — every 15 minutes
- `serverless-governance-cost-monitor` — every 60 minutes

### Step 5: Set Up SQL Alerts
Run `04_sql_alerts_setup` notebook to create alert queries and alerts.
Then configure notification destinations (email/Slack) in the Databricks SQL UI.

### Step 6: Activate
1. Go to **Workflows > Jobs** in Databricks UI
2. Resume each governance job
3. Monitor the `governance_audit_log` table for activity

## Notebooks

| Notebook | Purpose | Schedule | Approach |
|----------|---------|----------|----------|
| `00_setup` | Create schema, tables, views | Run once | Infrastructure |
| `01_query_watchdog` | Cancel underperforming queries | Every 5 min | System tables + Cancel API |
| `02_warehouse_auditor` | Stop idle warehouses | Every 15 min | Warehouses API + Query History |
| `03_cost_monitor` | Enforce DBU budgets | Every 60 min | Billing system table |
| `04_sql_alerts_setup` | Create SQL alert notifications | Run once | Native SQL Alerts |
| `05_deploy_jobs` | Create scheduled Databricks Jobs | Run once | Jobs API |

## SQL Alerts

Standalone SQL queries in `sql_alerts/` that can be used directly in the Databricks SQL Alerts UI:

| File | Trigger Condition | Schedule |
|------|-------------------|----------|
| `long_running_queries.sql` | Any query running > 30 min | Every 5 min |
| `idle_warehouses.sql` | Any warehouse idle > 30 min | Every 15 min |
| `cost_anomaly.sql` | Daily spend > 2σ above 30-day mean | Every 60 min |
| `high_data_scan.sql` | Any query scanning > 10 GB or 1B rows | Every 5 min |

## Configuration

Edit `config/governance_config.yaml` to customize:

### Query Killer Rules
```yaml
query_rules:
  - name: "Kill queries running > 30 minutes"
    enabled: true
    condition:
      max_duration_minutes: 30
    exclude:
      users: ["system@databricks.com"]
      statement_types: ["REFRESH"]
```

### Warehouse Shutdown Rules
```yaml
warehouse_rules:
  - name: "Stop warehouses idle > 10 minutes"
    enabled: true
    idle_threshold_minutes: 10
    exempt_warehouses: ["production-wh"]
```

### Cost Rules
```yaml
cost_rules:
  - name: "Stop warehouse exceeding 500 DBU daily"
    enabled: true
    max_daily_dbu: 500
    action: "stop"  # or "alert_only"
```

## Key APIs Used

| Capability | API | SDK Method |
|------------|-----|------------|
| Cancel a query | `POST /api/2.0/sql/statements/{id}/cancel` | `w.statement_execution.cancel_execution(id)` |
| Stop a warehouse | `POST /api/2.0/sql/warehouses/{id}/stop` | `w.warehouses.stop(id)` |
| Set query timeout | `SET statement_timeout = N` | SQL config on warehouse |
| Find running queries | `system.query.history` | `WHERE execution_status = 'RUNNING'` |
| Track warehouse cost | `system.billing.usage` | `WHERE billing_origin_product = 'SQL'` |
| Warehouse events | `system.compute.warehouse_events` | Start/stop/scale events |
| Create alerts | `POST /api/2.0/sql/alerts` | `w.alerts.create(...)` |

## System Tables Reference

| Table | Key Columns | Latency |
|-------|-------------|---------|
| `system.query.history` | `statement_id`, `execution_status`, `total_duration_ms`, `read_bytes`, `read_rows`, `compute.warehouse_id`, `query_tags` | ~5 min |
| `system.billing.usage` | `usage_quantity`, `sku_name`, `usage_metadata.warehouse_id`, `custom_tags` | ~hours |
| `system.compute.warehouse_events` | `warehouse_id`, `event_type`, `cluster_count`, `event_time` | ~5 min |
| `system.compute.warehouses` | Full warehouse config history | ~5 min |

## Gap Analysis: What Databricks Doesn't Have (Yet)

| Gap | Workaround in This Project |
|-----|---------------------------|
| No Query Watchdog for Serverless | Notebook 01 (custom watchdog) |
| No conditional query killing rules | YAML-based rules engine with exclusions |
| No real-time query event triggers | Scheduled polling every 5 min |
| No budget-based auto-stop | Notebook 03 (cost monitor with stop action) |
| No query complexity pre-flight | Tag-based rules (low-priority = shorter timeout) |
| Budget Policies don't enforce | Active enforcement via API calls |

## Pro Tips

### Use `on_wait_timeout: "CANCEL"` for API-submitted queries
```python
# Auto-cancel if query doesn't finish in 50 seconds
w.statement_execution.execute_statement(
    warehouse_id="...",
    statement="SELECT ...",
    wait_timeout="50s",
    on_wait_timeout="CANCEL"
)
```

### Set `statement_timeout` at the warehouse level
```sql
-- Apply to all queries on this warehouse (max query runtime in seconds)
SET statement_timeout = 3600;  -- 1 hour
```

### Use Query Tags for granular rules
```sql
SET QUERY_TAGS['priority'] = 'low';
SET QUERY_TAGS['team'] = 'data-science';
-- The watchdog applies different thresholds based on tags
```

### Set Serverless auto-stop to 1 minute via API
```python
# UI minimum is 5 min, but API allows 1 min
w.warehouses.edit(id="warehouse-id", auto_stop_mins=1)
```

## License

Internal Databricks use. Not for external distribution.
