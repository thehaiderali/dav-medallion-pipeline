# Databricks notebook source
# MAGIC %md
# MAGIC # Rebuild Gold from the current Silver snapshot
# MAGIC Use this for first deployment or repair. Ordinary Bronze-to-Silver runs
# MAGIC refresh Gold automatically after each successful Silver batch.

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
steps = execute(dbutils, spark, "silver_to_gold")

# COMMAND ----------
for name in ["gold_dim_date", "gold_dim_company", "gold_dim_sector", "gold_fact_daily_price", "gold_fact_sector_daily"]:
    print(name)
    display(spark.table(name).limit(20))
display(spark.table("pipeline_execution_logs").where("pipeline_layer LIKE 'SILVER_TO_GOLD%'").orderBy("start_time", ascending=False).limit(20))
