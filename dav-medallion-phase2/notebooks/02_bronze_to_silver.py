# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver
# MAGIC Casts, validates and upserts (MERGE INTO) Bronze batches into Silver; rejects go to the quarantine table.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `batch_id` | promote exactly one batch, e.g. `incremental_20240105` or `full_20240101` |
# MAGIC | `load_type` | optional filter: `full` / `incremental` / blank = both |
# MAGIC | `run_date` | promote batches with this `batch_date` |
# MAGIC | `start_date` / `end_date` | **backfill** window over `batch_date` (inclusive) |
# MAGIC | *(all blank)* | promote **every** Bronze batch (safe: the MERGE is idempotent) |
# MAGIC | `lakehouse_root` | where the Delta tables live |

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("load_type", "", ["", "full", "incremental"])
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("lakehouse_root", "dbfs:/FileStore/lakehouse")

# COMMAND ----------

from pipeline.run_pipeline import run_pipeline

get = lambda name: dbutils.widgets.get(name).strip() or None

steps = run_pipeline(
    spark,
    layer="bronze_to_silver",
    load_type=get("load_type"),
    batch_id=get("batch_id"),
    run_date=get("run_date"),
    start_date=get("start_date"),
    end_date=get("end_date"),
    lakehouse_root=get("lakehouse_root"),
)

for step in steps:
    print(
        step.status,
        step.batch_id,
        f"read={step.rows_read} inserted={step.rows_inserted} updated={step.rows_updated} quarantined={step.rows_quarantined}",
        step.error_message or "",
    )

if any(step.failed for step in steps):
    raise Exception("One or more batches failed - see pipeline_execution_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick checks

# COMMAND ----------

root = get("lakehouse_root")
display(spark.read.format("delta").load(f"{root}/silver/sp500_prices").limit(10))
display(spark.read.format("delta").load(f"{root}/quarantine/sp500_prices_rejected").limit(10))
