# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver to Gold
# MAGIC Explicit casts, quarantine, Silver MERGE, then automatic Gold refresh.
# MAGIC Use the same `process_date`/`load_type` as Bronze, or an exact `batch_id`.
# MAGIC A historical slice uses an older date; a window uses start/end with process_date blank.

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
steps = execute(dbutils, spark, "bronze_to_silver")

# COMMAND ----------
spark.table("phase2_silver_prices").printSchema()
display(spark.table("phase2_silver_prices").limit(20))
display(spark.table("phase2_silver_companies").limit(10))
display(spark.table("phase2_quarantine"))
display(spark.sql("SELECT ticker, trade_date, count(*) AS n FROM phase2_silver_prices GROUP BY ticker, trade_date HAVING count(*) > 1"))
display(spark.table("pipeline_execution_logs").orderBy("start_time", ascending=False).limit(20))
display(spark.table("gold_fact_daily_price").limit(20))
display(spark.table("gold_fact_sector_daily").limit(20))
