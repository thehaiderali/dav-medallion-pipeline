"""Shared notebook widgets and validation views (no cloud credentials required)."""
from .config import LakehousePaths
from .engine import run_pipeline
from .storage import read_table


def widgets(dbutils, spark):
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    defaults = {
        "process_date": "", "start_date": "", "end_date": "", "batch_id": "",
        "raw_path": "", "source_file": "", "metadata_file": "",
        "lakehouse_root": f"uc:{catalog}.dav_phase2",
    }
    for name, default in defaults.items():
        dbutils.widgets.text(name, default)
    dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"])
    dbutils.widgets.dropdown("file_format", "auto", ["auto", "jsonl", "csv", "parquet"])
    dbutils.widgets.dropdown("skip_missing", "false", ["false", "true"])
    dbutils.widgets.dropdown("refresh_metadata", "false", ["false", "true"])


def execute(dbutils, spark, layer):
    def get(name):
        return dbutils.widgets.get(name).strip() or None
    steps = run_pipeline(
        spark, layer=layer, process_date=get("process_date"),
        start_date=get("start_date"), end_date=get("end_date"), batch_id=get("batch_id"),
        raw_path=get("raw_path"), source_file=get("source_file"), metadata_file=get("metadata_file"),
        lakehouse_root=get("lakehouse_root"), load_type=get("load_type"),
        file_format=get("file_format"), skip_missing=get("skip_missing") == "true",
        refresh_metadata=get("refresh_metadata") == "true",
    )
    for step in steps:
        print(f"{step.layer}: {step.status}; batch={step.batch_id}; read={step.rows_read}; inserted={step.rows_inserted}; updated={step.rows_updated}; quarantined={step.rows_quarantined}; {step.error_message or ''}")
    paths = LakehousePaths.from_root(get("lakehouse_root"))
    for view, target in {
        "phase2_bronze_prices": paths.bronze, "phase2_bronze_companies": paths.metadata,
        "phase2_silver_prices": paths.silver, "phase2_silver_companies": paths.silver_metadata,
        "phase2_quarantine": paths.quarantine, "pipeline_execution_logs": paths.logs,
        "schema_drift_log": paths.drift,
    }.items():
        read_table(spark, target).createOrReplaceTempView(view)
    if any(s.failed for s in steps):
        raise RuntimeError("Pipeline failed; inspect pipeline_execution_logs")
    return steps
