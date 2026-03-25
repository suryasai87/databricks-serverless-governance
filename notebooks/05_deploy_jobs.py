# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Governance Jobs
# MAGIC
# MAGIC This notebook creates Databricks Jobs that run the governance notebooks on a schedule.
# MAGIC
# MAGIC **Jobs Created:**
# MAGIC 1. `serverless-governance-query-watchdog` — Every 5 minutes
# MAGIC 2. `serverless-governance-warehouse-auditor` — Every 15 minutes
# MAGIC 3. `serverless-governance-cost-monitor` — Every 60 minutes
# MAGIC
# MAGIC Run this notebook **once** after deploying the governance notebooks to the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CreateJob,
    JobSettings,
    Task,
    NotebookTask,
    CronSchedule,
    PauseStatus,
)

w = WorkspaceClient()

WORKSPACE_PATH = "/Workspace/Apps/serverless-governance"
JOB_PREFIX = "serverless-governance"

# Use serverless compute for the governance jobs themselves
SERVERLESS_ENABLED = True

print(f"Deploying governance jobs from: {WORKSPACE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Jobs

# COMMAND ----------

jobs_config = [
    {
        "name": f"{JOB_PREFIX}-query-watchdog",
        "notebook": f"{WORKSPACE_PATH}/01_query_watchdog",
        "schedule": "0 */5 * * * ?",  # Every 5 minutes
        "description": "Monitors and cancels underperforming queries on serverless SQL warehouses",
        "timeout_seconds": 240,
    },
    {
        "name": f"{JOB_PREFIX}-warehouse-auditor",
        "notebook": f"{WORKSPACE_PATH}/02_warehouse_auditor",
        "schedule": "0 */15 * * * ?",  # Every 15 minutes
        "description": "Detects and stops idle SQL warehouses",
        "timeout_seconds": 300,
    },
    {
        "name": f"{JOB_PREFIX}-cost-monitor",
        "notebook": f"{WORKSPACE_PATH}/03_cost_monitor",
        "schedule": "0 0 * * * ?",  # Every hour
        "description": "Monitors warehouse costs and enforces daily/hourly DBU budgets",
        "timeout_seconds": 600,
    },
]

created_jobs = []

for job_config in jobs_config:
    print(f"\n{'='*60}")
    print(f"Creating job: {job_config['name']}")
    print(f"{'='*60}")

    # Check if job already exists
    existing_jobs = list(w.jobs.list(name=job_config["name"]))
    if existing_jobs:
        print(f"  Job already exists (ID: {existing_jobs[0].job_id}). Skipping.")
        created_jobs.append({"name": job_config["name"], "id": existing_jobs[0].job_id, "status": "already_exists"})
        continue

    try:
        job = w.jobs.create(
            name=job_config["name"],
            tasks=[
                Task(
                    task_key="governance_task",
                    description=job_config["description"],
                    notebook_task=NotebookTask(
                        notebook_path=job_config["notebook"]
                    ),
                    timeout_seconds=job_config["timeout_seconds"],
                )
            ],
            schedule=CronSchedule(
                quartz_cron_expression=job_config["schedule"],
                timezone_id="America/New_York",
                pause_status=PauseStatus.PAUSED  # Start paused - user activates when ready
            ),
            tags={"team": "governance", "automation": "serverless-governance"},
        )

        print(f"  Created job ID: {job.job_id}")
        print(f"  Schedule: {job_config['schedule']} (PAUSED)")
        print(f"  Notebook: {job_config['notebook']}")
        created_jobs.append({"name": job_config["name"], "id": job.job_id, "status": "created"})

    except Exception as e:
        print(f"  ERROR creating job: {e}")
        created_jobs.append({"name": job_config["name"], "id": None, "status": f"error: {e}"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("DEPLOYMENT SUMMARY")
print("=" * 70)
for job in created_jobs:
    status_icon = "✓" if job["status"] in ("created", "already_exists") else "✗"
    print(f"  {status_icon} {job['name']}: {job['status']} (ID: {job['id']})")

print(f"\n{'='*70}")
print("IMPORTANT: All jobs are created in PAUSED state.")
print("To activate:")
print("  1. Go to Workflows > Jobs in the Databricks UI")
print("  2. Find each governance job")
print("  3. Click 'Resume' to start the schedule")
print("  4. Or run manually first to test: w.jobs.run_now(job_id=...)")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Run (Optional)
# MAGIC
# MAGIC Uncomment the cell below to trigger a test run of each job.

# COMMAND ----------

# Uncomment to test-run all jobs:
# for job in created_jobs:
#     if job["id"]:
#         print(f"Triggering test run for: {job['name']}...")
#         run = w.jobs.run_now(job_id=job["id"])
#         print(f"  Run ID: {run.run_id}")
