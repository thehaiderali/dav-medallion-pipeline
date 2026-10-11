# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Raw to Bronze
# MAGIC Prices append; committed company snapshot loads once with overwrite.
# MAGIC Set `process_date` (YYYY-MM-DD), `load_type`, and input locations.
# MAGIC **Manual fallback:** `source_file` overrides partition lookup; choose JSONL, CSV or Parquet.
# MAGIC On Free Edition use `/Volumes/...` inputs and `uc:catalog.schema` managed tables.

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
steps = execute(dbutils, spark, "raw_to_bronze")

# COMMAND ----------
display(spark.table("phase2_bronze_prices").limit(20))
display(spark.table("phase2_bronze_companies").limit(10))
display(spark.table("pipeline_execution_logs").orderBy("start_time", ascending=False).limit(20))
