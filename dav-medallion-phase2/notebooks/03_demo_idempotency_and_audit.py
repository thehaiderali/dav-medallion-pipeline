# Databricks notebook source
# MAGIC %md
# MAGIC # Executable Phase 2 proof
# MAGIC Use a **new** dedicated demo schema and a Volume directory for generated inputs.
# MAGIC Shows schemas, full and incremental loads, exact rerun equality, updates, historical
# MAGIC backfill, new-column evolution, bad-record quarantine, stale protection and audit logs.
# MAGIC No existing production tables or files are deleted.

# COMMAND ----------
import json
import sys
from pathlib import Path

for parent in [Path.cwd(), *Path.cwd().parents]:
    package_root = parent if (parent / "pipeline").is_dir() else parent / "dav-medallion-phase2"
    if (package_root / "pipeline").is_dir():
        sys.path.insert(0, str(package_root))
        break

catalog = spark.sql("SELECT current_catalog()").first()[0]
dbutils.widgets.text("demo_input_root", "", "Demo input directory /Volumes/...")
dbutils.widgets.text("demo_lakehouse_root", f"uc:{catalog}.dav_phase2_demo", "Fresh demo schema")

# COMMAND ----------
from pipeline.demo import demo
from pipeline.config import LakehousePaths
from pipeline.storage import read_table

input_root = dbutils.widgets.get("demo_input_root").strip()
if not input_root.startswith("/Volumes/"):
    raise ValueError("Set demo_input_root to a writable Unity Catalog Volume directory")
root = dbutils.widgets.get("demo_lakehouse_root").strip()
evidence = demo(spark, input_root, root)
evidence["execution_environment"] = "Databricks"
Path(input_root, "phase2_evidence.json").write_text(json.dumps(evidence, default=str, indent=2), encoding="utf-8")
print("Proof passed; saved phase2_evidence.json in the demo input directory")

# COMMAND ----------
paths = LakehousePaths.from_root(root)
read_table(spark, paths.bronze).printSchema()
read_table(spark, paths.silver).printSchema()
display(read_table(spark, paths.silver))
display(read_table(spark, paths.quarantine))
display(read_table(spark, paths.drift))
display(read_table(spark, paths.logs).orderBy("start_time"))
