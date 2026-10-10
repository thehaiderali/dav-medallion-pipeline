"""Bronze -> Silver.

Selects Bronze batches (by id, load type and/or batch-date window), casts and validates
them, MERGEs valid rows into Silver and quarantines the rest. One audit row per batch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .audit import AuditedStep, AuditLogger
from .config import LAYER_BRONZE_TO_SILVER, LakehousePaths
from .delta_utils import merge_into
from .transforms import build_silver_candidates, split_silver


@dataclass(frozen=True)
class BronzeBatch:
    batch_id: str
    load_type: str
    batch_date: date


def select_bronze_batches(
    spark: SparkSession,
    paths: LakehousePaths,
    *,
    batch_id: str | None = None,
    load_type: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[BronzeBatch]:
    """Which Bronze batches should be promoted? Oldest first, so newer data wins."""
    bronze = spark.read.format("delta").load(paths.bronze)
    if batch_id:
        bronze = bronze.where(F.col("batch_id") == batch_id)
    if load_type:
        bronze = bronze.where(F.col("load_type") == load_type)
    if start_date:
        bronze = bronze.where(F.col("batch_date") >= F.lit(start_date.isoformat()).cast("date"))
    if end_date:
        bronze = bronze.where(F.col("batch_date") <= F.lit(end_date.isoformat()).cast("date"))
    rows = (
        bronze.select("batch_id", "load_type", "batch_date")
        .distinct()
        .orderBy("batch_date", "load_type", "batch_id")
        .collect()
    )
    return [BronzeBatch(r["batch_id"], r["load_type"], r["batch_date"]) for r in rows]


def bronze_to_silver(
    spark: SparkSession,
    paths: LakehousePaths,
    logger: AuditLogger,
    batch: BronzeBatch,
) -> AuditedStep:
    """Promote ONE Bronze batch to Silver. Re-running it is a no-op for unchanged data."""
    with AuditedStep(
        logger, LAYER_BRONZE_TO_SILVER, batch.load_type, batch.batch_id, parameter=f"bronze batch {batch.batch_id}"
    ) as step:
        bronze = spark.read.format("delta").load(paths.bronze).where(F.col("batch_id") == batch.batch_id)
        candidates = build_silver_candidates(bronze).cache()
        silver_rows, rejected_rows = split_silver(candidates)
        silver_rows = silver_rows.cache()
        rejected_rows = rejected_rows.cache()

        step.rows_read = candidates.count()
        step.rows_quarantined = rejected_rows.count()

        # Upsert valid rows. A row is only updated when its content changed AND the incoming
        # batch is not older than the batch that produced the current Silver row, so replaying
        # an old batch can never overwrite newer data.
        silver_result = merge_into(
            spark,
            target_path=paths.silver,
            source_df=silver_rows,
            key_columns=["ticker", "trade_date"],
            update_condition="s.row_hash <> t.row_hash AND s.source_batch_date >= t.source_batch_date",
        )
        step.rows_inserted = silver_result["inserted"]
        step.rows_updated = silver_result["updated"]

        if step.rows_quarantined:
            merge_into(
                spark,
                target_path=paths.quarantine,
                source_df=rejected_rows,
                key_columns=["record_key"],
                update_condition="s.row_hash <> t.row_hash OR s.reject_reason <> t.reject_reason",
            )

        candidates.unpersist()
        silver_rows.unpersist()
        rejected_rows.unpersist()
    return step
