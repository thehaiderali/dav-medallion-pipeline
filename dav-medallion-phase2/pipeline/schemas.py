"""Operational contracts; business-table contracts live in contracts.py."""
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

LOG_SCHEMA = StructType([
    StructField(name, kind, True) for name, kind in [
        ("run_id", StringType()), ("pipeline_layer", StringType()), ("load_type", StringType()),
        ("batch_id", StringType()), ("parameter", StringType()), ("parameter_processed", StringType()),
        ("start_time", TimestampType()), ("end_time", TimestampType()), ("duration_seconds", DoubleType()),
        ("status", StringType()), ("rows_read", LongType()), ("rows_inserted", LongType()),
        ("rows_updated", LongType()), ("rows_quarantined", LongType()),
        ("rows_aggregated", LongType()),
        ("error_message", StringType()), ("load_timestamp", TimestampType()),
    ]
])
DRIFT_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in [
        "run_id", "batch_id", "source_file", "drift_type", "column_name",
        "expected_type", "actual_type", "action_taken",
    ]
] + [StructField("load_timestamp", TimestampType(), True)])
