"""Raw -> Bronze.

Reads one raw Parquet file (full load or one incremental partition), lands it as
strings with lineage metadata, and upserts it into the Bronze Delta table.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession

try:  # PySpark >= 3.4
    from pyspark.errors import AnalysisException
except ImportError:  # older runtimes
    from pyspark.sql.utils import AnalysisException

from .audit import AuditedStep, AuditLogger, utc_now
from .config import LAYER_RAW_TO_BRONZE, LakehousePaths, make_batch_id, resolve_raw_file
from .delta_utils import merge_into
from .schemas import DRIFT_SCHEMA, RAW_SCHEMA
from .transforms import add_bronze_metadata, detect_drift, to_bronze_strings


def _is_missing_path(error: Exception) -> bool:
    text = str(error)
    return "PATH_NOT_FOUND" in text or "does not exist" in text or "Path does not exist" in text


def _log_drift(
    spark: SparkSession,
    paths: LakehousePaths,
    *,
    run_id: str,
    batch_id: str,
    source_file: str,
    drift: list[dict],
) -> None:
    if not drift:
        return
    now = utc_now()
    action = {
        "NEW_COLUMN": "kept as STRING in Bronze via schema evolution (mergeSchema)",
        "MISSING_COLUMN": "filled with NULL; rows will be quarantined at Silver if the field is required",
        "TYPE_CHANGE": "landed as STRING in Bronze; non-castable values quarantined at Silver",
    }
    rows = [
        (
            run_id,
            batch_id,
            source_file,
            item["drift_type"],
            item["column_name"],
            item["expected_type"],
            item["actual_type"],
            action[item["drift_type"]],
            now,
        )
        for item in drift
    ]
    spark.createDataFrame(rows, schema=DRIFT_SCHEMA).write.format("delta").mode("append").save(paths.drift)


def read_raw_file(spark: SparkSession, source: str) -> tuple[DataFrame, list[dict]]:
    """Read a raw Parquet file against the explicit contract and report any schema drift.

    1. Footer-only schema check (no data is sampled) against ``RAW_SCHEMA``.
    2. No drift -> strict schema-on-read with ``RAW_SCHEMA``.
    3. Drift     -> Parquet cannot be forced into the contract (e.g. a string column read as
       double fails), so the file's own schema is used and every column is normalised to
       STRING by ``to_bronze_strings``. The batch is never rejected for drift alone.
    """
    drift = detect_drift(spark.read.parquet(source).schema)
    if drift:
        return spark.read.parquet(source), drift
    return spark.read.schema(RAW_SCHEMA).parquet(source), drift


def raw_to_bronze(
    spark: SparkSession,
    paths: LakehousePaths,
    logger: AuditLogger,
    *,
    raw_path: str,
    load_type: str,
    batch_date: date,
    source_file: str | None = None,
    skip_missing: bool = False,
) -> AuditedStep:
    """Process ONE raw file into Bronze. Safe to re-run: the MERGE is keyed on ``record_key``."""
    batch_id = make_batch_id(load_type, batch_date)
    source = resolve_raw_file(raw_path, load_type, batch_date, source_file)

    with AuditedStep(logger, LAYER_RAW_TO_BRONZE, load_type, batch_id, parameter=source) as step:
        try:
            raw, drift = read_raw_file(spark, source)
        except AnalysisException as error:
            if skip_missing and _is_missing_path(error):
                step.skip(f"source file not found: {source}")
                return step
            raise
        _log_drift(spark, paths, run_id=logger.run_id, batch_id=batch_id, source_file=source, drift=drift)

        bronze = add_bronze_metadata(
            to_bronze_strings(raw),
            source_file=source,
            batch_id=batch_id,
            load_type=load_type,
            batch_date=batch_date,
        ).cache()

        step.rows_read = bronze.count()
        result = merge_into(
            spark,
            target_path=paths.bronze,
            source_df=bronze,
            key_columns=["record_key"],
            update_condition="s.row_hash <> t.row_hash",
        )
        step.rows_inserted = result["inserted"]
        step.rows_updated = result["updated"]
        bronze.unpersist()
    return step
