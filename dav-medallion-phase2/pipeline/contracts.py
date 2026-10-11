"""Explicit contracts for the two-source pipeline. JSONL values land as strings."""
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType


def schema(fields):
    return StructType([StructField(name, kind, True) for name, kind in fields])


TEXT_SCHEMA = schema([("value", StringType())])
PRICE_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
RAW_SCHEMA = schema([(name, StringType()) for name in PRICE_COLUMNS] + [("_corrupt_record", StringType())])
COMPANY_COLUMNS = ["ticker", "company_name", "sector", "sub_industry"]
COMPANY_RAW_SCHEMA = schema([(name, StringType()) for name in COMPANY_COLUMNS])
LINEAGE = [("source_file", StringType()), ("batch_id", StringType()), ("load_timestamp", TimestampType())]
BRONZE_SCHEMA = schema([
    ("bronze_row_id", StringType()), ("record_key", StringType()),
    *[(name, StringType()) for name in PRICE_COLUMNS],
    ("raw_payload", StringType()), ("rescued_data", StringType()),
    ("load_type", StringType()), ("batch_date", DateType()), ("run_id", StringType()),
    *LINEAGE,
])
COMPANY_SCHEMA = schema([*[(name, StringType()) for name in COMPANY_COLUMNS], ("snapshot_file", StringType()), ("row_hash", StringType()), *LINEAGE])
SILVER_SCHEMA = schema([
    ("ticker", StringType()), ("trade_date", DateType()),
    *[(name, DoubleType()) for name in ["open", "high", "low", "close", "adj_close"]],
    ("volume", LongType()), *[(name, StringType()) for name in COMPANY_COLUMNS[1:]],
    ("metadata_source_file", StringType()), ("metadata_batch_id", StringType()),
    ("source_batch_date", DateType()), ("row_hash", StringType()), *LINEAGE,
])
QUARANTINE_SCHEMA = schema([
    ("record_key", StringType()), *[(name, StringType()) for name in PRICE_COLUMNS],
    ("raw_payload", StringType()), ("rescued_data", StringType()),
    ("reject_reason", StringType()), ("reject_stage", StringType()),
    ("batch_date", DateType()), *LINEAGE,
])
