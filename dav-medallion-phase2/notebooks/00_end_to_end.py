# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-end raw to Gold
# MAGIC Enter the same parameters used in Phase 2. A single execution ingests
# MAGIC Bronze, validates and MERGEs Silver, then refreshes Gold for each
# MAGIC successful Silver batch. Inspect pipeline_execution_logs for every step.

# COMMAND ----------
import sys
from pathlib import Path

for parent in [Path.cwd(), *Path.cwd().parents]:
    package_root = parent if (parent / "pipeline").is_dir() else parent / "dav-medallion-phase2"
    if (package_root / "pipeline").is_dir():
        sys.path.insert(0, str(package_root))
        break
else:
    raise RuntimeError("Open this notebook inside the connected repository Git Folder")

from pipeline.notebook_support import widgets, execute
widgets(dbutils, spark)

# COMMAND ----------
steps = execute(dbutils, spark, "all")

# COMMAND ----------
display(spark.table("gold_fact_daily_price").limit(20))
display(spark.table("gold_fact_sector_daily").limit(20))
display(spark.table("pipeline_execution_logs").orderBy("start_time", ascending=False).limit(30))
