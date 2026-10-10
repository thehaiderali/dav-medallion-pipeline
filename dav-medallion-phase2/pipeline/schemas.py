"""Explicit schemas (data contracts) for every table in the Lakehouse.

Nothing in this project uses Spark schema inference: every read is made with one
of the StructTypes below.

Layer summary
-------------
RAW_SCHEMA         contract of the Parquet files produced by Phase 1 ingestion
BRONZE_SCHEMA      raw values landed as STRING + lineage metadata (append/upsert only)
SILVER_SCHEMA      cleaned, strongly typed, de-duplicated (one row per ticker + day)
QUARANTINE_SCHEMA  Bronze rows that failed Silver validation, with the reason
LOG_SCHEMA         pipeline_execution_logs (one row per processed file / batch)
DRIFT_SCHEMA       schema_drift_log (one row per detected drift per file)
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Raw contract (what Phase 1 writes to S3)
# --------------------------------------------------------------------------- #
RAW_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ]
)

CONTRACT_COLUMNS = [field.name for field in RAW_SCHEMA]
PRICE_COLUMNS = ["open", "high", "low", "close"]

# --------------------------------------------------------------------------- #
# Bronze: faithful landing zone. Every contract column is a STRING so that a
# type change in the source can never make the load fail; casting happens in
# Silver, where bad values are quarantined instead.
# --------------------------------------------------------------------------- #
BRONZE_SCHEMA = StructType(
    [StructField("record_key", StringType(), True)]
    + [StructField(name, StringType(), True) for name in CONTRACT_COLUMNS]
    + [
        StructField("source_file", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("batch_date", DateType(), True),
        StructField("row_hash", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
)

# --------------------------------------------------------------------------- #
# Silver: typed and de-duplicated
# --------------------------------------------------------------------------- #
SILVER_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("row_hash", StringType(), True),
        StructField("source_batch_id", StringType(), True),
        StructField("source_batch_date", DateType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
)

# --------------------------------------------------------------------------- #
# Quarantine: rejected Bronze rows (raw strings preserved for reprocessing)
# --------------------------------------------------------------------------- #
QUARANTINE_SCHEMA = StructType(
    [StructField("record_key", StringType(), True)]
    + [StructField(name, StringType(), True) for name in CONTRACT_COLUMNS]
    + [
        StructField("source_file", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("batch_date", DateType(), True),
        StructField("row_hash", StringType(), True),
        StructField("reject_stage", StringType(), True),
        StructField("reject_reason", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
)

# --------------------------------------------------------------------------- #
# Operational tables
# --------------------------------------------------------------------------- #
LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("pipeline_layer", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_quarantined", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
)

DRIFT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("drift_type", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("expected_type", StringType(), True),
        StructField("actual_type", StringType(), True),
        StructField("action_taken", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ]
)
