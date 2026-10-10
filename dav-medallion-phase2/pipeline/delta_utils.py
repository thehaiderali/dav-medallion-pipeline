"""Spark session + Delta Lake helpers (table creation and MERGE INTO upserts)."""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession

from .config import LakehousePaths
from .schemas import (
    BRONZE_SCHEMA,
    DRIFT_SCHEMA,
    LOG_SCHEMA,
    QUARANTINE_SCHEMA,
    SILVER_SCHEMA,
)


def get_spark(app_name: str = "dav-medallion-pipeline") -> SparkSession:
    """Reuse the Databricks session if there is one, otherwise build a local Delta session."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:  # delta-spark not installed (e.g. cluster already provides Delta)
            pass
        spark = builder.getOrCreate()
    # allow MERGE / appends to add new source columns to the target (schema evolution)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return spark


def ensure_tables(spark: SparkSession, paths: LakehousePaths) -> None:
    """Create every Delta table with its explicit schema if it does not exist yet."""
    from delta.tables import DeltaTable

    tables = [
        (paths.bronze, BRONZE_SCHEMA),
        (paths.silver, SILVER_SCHEMA),
        (paths.quarantine, QUARANTINE_SCHEMA),
        (paths.logs, LOG_SCHEMA),
        (paths.drift, DRIFT_SCHEMA),
    ]
    for path, schema in tables:
        if not DeltaTable.isDeltaTable(spark, path):
            spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def merge_into(
    spark: SparkSession,
    *,
    target_path: str,
    source_df: DataFrame,
    key_columns: list[str],
    update_condition: str | None = None,
) -> dict[str, int]:
    """Upsert ``source_df`` into the Delta table at ``target_path`` with MERGE INTO.

    * matched rows are updated only when ``update_condition`` holds (by default callers
      pass a row-hash comparison), so re-running identical data changes nothing
    * unmatched rows are inserted
    * ``source_df`` must be unique on ``key_columns`` (Delta rejects ambiguous merges)

    Returns ``{"inserted": n, "updated": n}`` taken from the metrics Delta returns.
    """
    view = f"merge_src_{uuid.uuid4().hex[:8]}"
    source_df.createOrReplaceTempView(view)
    on_clause = " AND ".join(f"t.`{key}` = s.`{key}`" for key in key_columns)
    matched = f"WHEN MATCHED AND {update_condition} THEN UPDATE SET *" if update_condition else "WHEN MATCHED THEN UPDATE SET *"
    statement = (
        f"MERGE INTO delta.`{target_path}` AS t "
        f"USING {view} AS s "
        f"ON {on_clause} "
        f"{matched} "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    try:
        metrics = spark.sql(statement).collect()[0].asDict()
    finally:
        spark.catalog.dropTempView(view)
    return {
        "inserted": int(metrics.get("num_inserted_rows") or 0),
        "updated": int(metrics.get("num_updated_rows") or 0),
    }
