# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Demo: idempotency, schema drift and audit trail
# MAGIC Proves the Phase 2 requirements on a throw-away lakehouse using the sample files shipped in `sample_data/`.
# MAGIC Run top to bottom. Change `SAMPLE_DIR` if the sample files live elsewhere.

# COMMAND ----------

import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

SAMPLE_DIR = f"file:{repo_root}/sample_data"  # or dbfs:/FileStore/sample_data after uploading
DEMO_ROOT = "dbfs:/FileStore/demo_lakehouse"
dbutils.fs.rm(DEMO_ROOT, True)

# COMMAND ----------

from pipeline.run_pipeline import run_pipeline

common = dict(lakehouse_root=DEMO_ROOT, raw_path=SAMPLE_DIR)

# Sample files are single files, so point at them directly with `source_file`.
def load_all(label):
    print(f"--- {label}")
    steps = run_pipeline(spark, layer="all", load_type="full", run_date="2024-01-01",
                         source_file=f"{SAMPLE_DIR}/full_load_sample.parquet", **common)
    steps += run_pipeline(spark, layer="all", load_type="incremental", run_date="2024-01-02",
                          source_file=f"{SAMPLE_DIR}/incremental_load_sample.parquet", **common)
    for s in steps:
        print(f"{s.layer:17} {s.status:8} {s.batch_id:22} inserted={s.rows_inserted} updated={s.rows_updated} quarantined={s.rows_quarantined}")

load_all("first run  -> rows inserted")
load_all("second run -> IDENTICAL input: expect inserted=0 updated=0")

# COMMAND ----------

silver = spark.read.format("delta").load(f"{DEMO_ROOT}/silver/sp500_prices")
dupes = silver.groupBy("ticker", "trade_date").count().where("count > 1").count()
print("Silver rows:", silver.count(), "| duplicate (ticker, trade_date) keys:", dupes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit trail

# COMMAND ----------

display(
    spark.read.format("delta").load(f"{DEMO_ROOT}/ops/pipeline_execution_logs")
    .select("pipeline_layer", "batch_id", "parameter", "start_time", "end_time", "status", "rows_inserted", "rows_updated", "rows_quarantined")
    .orderBy("start_time")
)
