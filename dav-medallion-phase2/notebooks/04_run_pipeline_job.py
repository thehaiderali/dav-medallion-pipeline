# Databricks notebook source
# MAGIC %md
# MAGIC # Optional manual all-in-one entry point — Bronze and Silver only
# MAGIC `source_file` enables manual full/incremental fallback. No job/API credentials are needed.

# COMMAND ----------
import sys
from pathlib import Path

for parent in [Path.cwd(), *Path.cwd().parents]:
    package_root = parent if (parent / "pipeline").is_dir() else parent / "dav-medallion-phase2"
    if (package_root / "pipeline").is_dir():
        sys.path.insert(0, str(package_root))
        break
else:
    raise RuntimeError("Configure the job notebook source as the connected Git repository")

from pipeline.notebook_support import widgets, execute
widgets(dbutils, spark)

# COMMAND ----------
steps = execute(dbutils, spark, "all")

# COMMAND ----------
display(spark.table("pipeline_execution_logs").orderBy("start_time", ascending=False).limit(20))
