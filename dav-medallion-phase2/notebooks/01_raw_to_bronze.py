# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Raw to Bronze
# MAGIC Loads raw S&P 500 Parquet files into the Bronze Delta table (MERGE INTO, idempotent).
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `load_type` | `full` (one file) or `incremental` (one file per date) |
# MAGIC | `run_date` | single batch date `YYYY-MM-DD` (blank = today) |
# MAGIC | `start_date` / `end_date` | **backfill** window (incremental only, inclusive); leave blank for a normal run |
# MAGIC | `raw_path` | root of the raw files, e.g. `s3a://bucket/raw/sp500` or `dbfs:/FileStore/raw/sp500` |
# MAGIC | `lakehouse_root` | where the Delta tables live |
# MAGIC | `source_file` | optional: read exactly this file |
# MAGIC | `skip_missing` | `true` = log SKIPPED instead of FAILURE when a date has no file |

# COMMAND ----------

# Make the repo's `pipeline` package importable (Databricks Repos / Git folder).
import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"])
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("raw_path", "dbfs:/FileStore/raw/sp500")
dbutils.widgets.text("lakehouse_root", "dbfs:/FileStore/lakehouse")
dbutils.widgets.text("source_file", "")
dbutils.widgets.dropdown("skip_missing", "false", ["true", "false"])

# COMMAND ----------

from pipeline.run_pipeline import run_pipeline

get = lambda name: dbutils.widgets.get(name).strip() or None

steps = run_pipeline(
    spark,
    layer="raw_to_bronze",
    load_type=get("load_type"),
    run_date=get("run_date"),
    start_date=get("start_date"),
    end_date=get("end_date"),
    raw_path=get("raw_path"),
    lakehouse_root=get("lakehouse_root"),
    source_file=get("source_file"),
    skip_missing=get("skip_missing") == "true",
)

for step in steps:
    print(step.status, step.batch_id, f"read={step.rows_read} inserted={step.rows_inserted} updated={step.rows_updated}", step.error_message or "")

if any(step.failed for step in steps):
    raise Exception("One or more files failed - see pipeline_execution_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect the audit log

# COMMAND ----------

lakehouse_root = get("lakehouse_root")
display(
    spark.read.format("delta").load(f"{lakehouse_root}/ops/pipeline_execution_logs").orderBy("start_time", ascending=False).limit(20)
)
