"""Use managed Unity Catalog tables on Free Edition, path Delta tables locally."""
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
    if "`" in target:
        raise ValueError("Backticks are not permitted in storage paths")
    return f"delta.`{target}`"


def read_table(spark, target):
    return spark.table(target[3:]) if target.startswith("uc:") else spark.read.format("delta").load(target)


def write_table(frame, target, mode="append", evolve=False):
    writer = frame.write.format("delta").mode(mode)
    if evolve:
        writer = writer.option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if target.startswith("uc:"):
        writer.saveAsTable(target[3:])
    else:
        writer.save(target)


def ensure_table(spark, target, contract):
    # SQL DDL avoids RDDs and the JVM-only DeltaTable API on serverless compute.
    columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in contract)
    if target.startswith("uc:"):
        parts = target[3:].split(".")
        sql_ref(target)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {sql_ref(target)} ({columns}) USING DELTA")
    else:
        from delta.tables import DeltaTable
        if not DeltaTable.isDeltaTable(spark, target):
            write_table(spark.createDataFrame([], contract), target, "overwrite")


def merge_into(spark: SparkSession, target: str, frame, keys, condition: str):
    view = f"merge_source_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    try:
        result = spark.sql(
            f"MERGE INTO {sql_ref(target)} t USING {view} s ON {on} "
            f"WHEN MATCHED AND ({condition}) THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        if not result:
            raise RuntimeError("MERGE returned no operation metrics; verify the runtime's Delta support")
        metrics = result[0].asDict()
        if "num_inserted_rows" not in metrics or "num_updated_rows" not in metrics:
            raise RuntimeError(f"Unsupported MERGE metrics: {list(metrics)}")
        return {"inserted": int(metrics["num_inserted_rows"]), "updated": int(metrics["num_updated_rows"])}
    finally:
        spark.catalog.dropTempView(view)
