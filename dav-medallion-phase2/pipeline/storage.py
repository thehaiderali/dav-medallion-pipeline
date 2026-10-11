"""Managed Unity Catalog Delta tables for Databricks serverless."""
from __future__ import annotations

import re
import uuid

from pyspark.sql import SparkSession


def sql_ref(target: str) -> str:
    if target.startswith("uc:"):
        name = target[3:]
        if not re.fullmatch(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*", name):
            raise ValueError("Managed table target must be uc:catalog.schema.table")
        return ".".join(f"`{part}`" for part in name.split("."))
    raise ValueError("Use a managed Unity Catalog table target: uc:catalog.schema.table")


def read_table(spark, target):
    sql_ref(target)
    return spark.table(target[3:])


def write_table(frame, target, mode="append", evolve=False):
    writer = frame.write.format("delta").mode(mode)
    if evolve:
        writer = writer.option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    sql_ref(target)
    writer.saveAsTable(target[3:])


def ensure_table(spark, target, contract):
    # SQL DDL avoids RDDs and the JVM-only DeltaTable API on serverless compute.
    columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in contract)
    parts = target[3:].split(".")
    sql_ref(target)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {sql_ref(target)} ({columns}) USING DELTA")


def merge_into(spark: SparkSession, target: str, frame, keys, condition: str):
    view = f"merge_source_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    try:
        previous_version = spark.sql(f"DESCRIBE HISTORY {sql_ref(target)} LIMIT 1").first()["version"]
        result = spark.sql(
            f"MERGE INTO {sql_ref(target)} t USING {view} s ON {on} "
            f"WHEN MATCHED AND ({condition}) THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        metrics = result[0].asDict() if result else {}
        if "num_inserted_rows" in metrics and "num_updated_rows" in metrics:
            return {"inserted": int(metrics["num_inserted_rows"]), "updated": int(metrics["num_updated_rows"])}
        # Some command APIs do not expose the SQL metric row. Use the actual
        # Delta commit rather than guessing counts or re-counting source keys.
        history = spark.sql(f"DESCRIBE HISTORY {sql_ref(target)}").where(f"version > {int(previous_version)}").limit(2).collect()
        if not history:
            return {"inserted": 0, "updated": 0}  # no commit: unchanged MERGE
        if len(history) != 1 or history[0]["operation"] != "MERGE":
            raise RuntimeError("Cannot attribute MERGE metrics: concurrent table commits detected")
        commit_metrics = history[0]["operationMetrics"]
        if "numTargetRowsInserted" not in commit_metrics or "numTargetRowsUpdated" not in commit_metrics:
            raise RuntimeError("Delta MERGE commit did not expose insert/update metrics")
        return {"inserted": int(commit_metrics["numTargetRowsInserted"]), "updated": int(commit_metrics["numTargetRowsUpdated"])}
    finally:
        spark.catalog.dropTempView(view)
