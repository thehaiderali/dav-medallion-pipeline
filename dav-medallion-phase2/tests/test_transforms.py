"""Unit tests for the pure transformations (plain Spark, no Delta required)."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from pipeline.bronze import read_raw_file
from pipeline.schemas import BRONZE_SCHEMA, CONTRACT_COLUMNS, QUARANTINE_SCHEMA, RAW_SCHEMA, SILVER_SCHEMA
from pipeline.transforms import (
    add_bronze_metadata,
    build_silver_candidates,
    detect_drift,
    split_silver,
    to_bronze_strings,
)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def raw_df(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def bronze_from(spark, rows, batch_id="incremental_20240102", batch_date=date(2024, 1, 2), load_type="incremental"):
    return add_bronze_metadata(
        to_bronze_strings(raw_df(spark, rows)),
        source_file="mem://file",
        batch_id=batch_id,
        load_type=load_type,
        batch_date=batch_date,
    )


def name_types(schema):
    """(name, type) pairs - nullability is a Spark planner detail, not part of the contract."""
    return [(f.name, f.dataType) for f in schema.fields]


GOOD = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0, 1_000)


# --------------------------------------------------------------------------- #
# schema drift detection
# --------------------------------------------------------------------------- #
def test_no_drift_for_contract_schema():
    assert detect_drift(RAW_SCHEMA) == []


def test_detects_new_missing_and_changed_columns():
    actual = StructType(
        [
            StructField("ticker", StringType()),
            StructField("date", DateType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", StringType()),  # long -> string
            StructField("adj_close", DoubleType()),  # new
        ]
    )
    drift = {(d["drift_type"], d["column_name"]) for d in detect_drift(actual)}
    assert drift == {("TYPE_CHANGE", "volume"), ("NEW_COLUMN", "adj_close")}

    without_volume = StructType([f for f in RAW_SCHEMA.fields if f.name != "volume"])
    assert [(d["drift_type"], d["column_name"]) for d in detect_drift(without_volume)] == [("MISSING_COLUMN", "volume")]


# --------------------------------------------------------------------------- #
# reading raw files with drift (parquet written by Spark, as Phase 1 writes parquet)
# --------------------------------------------------------------------------- #
def test_read_raw_file_without_drift_uses_contract(spark, tmp_path):
    path = str(tmp_path / "ok.parquet")
    raw_df(spark, [GOOD]).write.parquet(path)
    df, drift = read_raw_file(spark, path)
    assert drift == []
    assert name_types(df.schema) == name_types(RAW_SCHEMA)


def test_read_raw_file_survives_type_change_and_new_column(spark, tmp_path):
    path = str(tmp_path / "drifted.parquet")
    drifted = spark.createDataFrame(
        [("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0, "1000", 103.5)],
        schema="ticker string, date date, open double, high double, low double, close double, "
        "volume string, adj_close double",
    )
    drifted.write.parquet(path)

    df, drift = read_raw_file(spark, path)
    assert {d["drift_type"] for d in drift} == {"TYPE_CHANGE", "NEW_COLUMN"}

    bronze = to_bronze_strings(df)
    assert all(f.dataType == StringType() for f in bronze.schema.fields)
    assert bronze.columns == [*CONTRACT_COLUMNS, "adj_close"]
    assert bronze.collect()[0]["volume"] == "1000"
    assert bronze.collect()[0]["adj_close"] == "103.5"


def test_missing_contract_column_becomes_null(spark, tmp_path):
    path = str(tmp_path / "missing.parquet")
    spark.createDataFrame(
        [("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0)],
        schema="ticker string, date date, open double, high double, low double, close double",
    ).write.parquet(path)
    df, drift = read_raw_file(spark, path)
    assert [d["drift_type"] for d in drift] == ["MISSING_COLUMN"]
    assert to_bronze_strings(df).collect()[0]["volume"] is None


# --------------------------------------------------------------------------- #
# Bronze metadata / idempotency building blocks
# --------------------------------------------------------------------------- #
def test_bronze_frame_matches_bronze_schema(spark):
    bronze = bronze_from(spark, [GOOD])
    assert bronze.columns == BRONZE_SCHEMA.fieldNames()
    assert name_types(bronze.schema) == name_types(BRONZE_SCHEMA)


def test_record_key_and_hash_are_deterministic(spark):
    first = bronze_from(spark, [GOOD]).collect()[0]
    second = bronze_from(spark, [GOOD]).collect()[0]
    assert first["record_key"] == second["record_key"]
    assert first["row_hash"] == second["row_hash"]
    assert first["load_timestamp"] is not None


def test_hash_changes_when_value_changes_but_key_does_not(spark):
    changed = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.5, 1_000)
    a = bronze_from(spark, [GOOD]).collect()[0]
    b = bronze_from(spark, [changed]).collect()[0]
    assert a["record_key"] == b["record_key"]
    assert a["row_hash"] != b["row_hash"]


def test_duplicate_rows_in_one_file_collapse_to_one_per_key(spark):
    bronze = bronze_from(spark, [GOOD, GOOD])
    assert bronze.count() == 1


# --------------------------------------------------------------------------- #
# Bronze -> Silver: casting, validation, quarantine, de-duplication
# --------------------------------------------------------------------------- #
def reasons_by_ticker(rejected):
    return {r["ticker"]: set(r["reject_reason"].split(";")) for r in rejected.collect()}


def test_silver_output_has_exact_silver_schema(spark):
    silver, rejected = split_silver(build_silver_candidates(bronze_from(spark, [GOOD])))
    assert name_types(silver.schema) == name_types(SILVER_SCHEMA)
    assert name_types(rejected.schema) == name_types(QUARANTINE_SCHEMA)
    row = silver.collect()[0]
    assert row["trade_date"] == date(2024, 1, 2)
    assert row["volume"] == 1000
    assert row["load_timestamp"] is not None


def test_stringified_numbers_are_cast(spark):
    # simulate a drifted Bronze row where numbers arrived as text
    bronze = spark.createDataFrame(
        [("k1", "msft ", "2024-01-03", "10.5", "11", "10", "10.75", "123.0")],
        schema="record_key string, ticker string, date string, open string, high string, low string, "
        "close string, volume string",
    )
    for name, value in [
        ("source_file", "f"),
        ("batch_id", "incremental_20240103"),
        ("load_type", "incremental"),
        ("row_hash", "h"),
    ]:
        bronze = bronze.withColumn(name, F.lit(value))

    bronze = bronze.withColumn("batch_date", F.lit("2024-01-03").cast("date")).withColumn(
        "load_timestamp", F.current_timestamp()
    )
    silver, rejected = split_silver(build_silver_candidates(bronze))
    row = silver.collect()[0]
    assert (row["ticker"], row["open"], row["volume"]) == ("MSFT", 10.5, 123)
    assert rejected.count() == 0


@pytest.mark.parametrize(
    "bad_row, expected_reason",
    [
        (("", date(2024, 1, 2), 1.0, 2.0, 1.0, 1.5, 10), "MISSING_TICKER"),
        (("BAD", date(2024, 1, 2), -1.0, 2.0, 1.0, 1.5, 10), "NON_POSITIVE_OPEN"),
        (("BAD", date(2024, 1, 2), 1.0, 1.0, 2.0, 1.5, 10), "HIGH_LESS_THAN_LOW"),
        (("BAD", date(2024, 1, 2), 1.0, 2.0, 1.0, 1.5, -5), "NEGATIVE_VOLUME"),
        (("BAD", date(2024, 1, 2), None, 2.0, 1.0, 1.5, 10), "INVALID_OPEN"),
        (("BAD", date(2024, 1, 2), 1.0, 2.0, 1.0, 1.5, None), "INVALID_VOLUME"),
        (("BAD", None, 1.0, 2.0, 1.0, 1.5, 10), "INVALID_DATE"),
    ],
)
def test_bad_rows_are_quarantined_with_reason(spark, bad_row, expected_reason):
    silver, rejected = split_silver(build_silver_candidates(bronze_from(spark, [GOOD, bad_row])))
    assert silver.count() == 1  # only the good row survives; the batch does not fail
    assert rejected.count() == 1
    assert expected_reason in rejected.collect()[0]["reject_reason"].split(";")


def test_garbage_strings_are_quarantined_not_raised(spark):
    bronze = spark.createDataFrame(
        [("k1", "AAPL", "not-a-date", "abc", "2", "1", "1.5", "12x")],
        schema="record_key string, ticker string, date string, open string, high string, low string, "
        "close string, volume string",
    )
    bronze = (
        bronze.withColumn("source_file", F.lit("f"))
        .withColumn("batch_id", F.lit("b"))
        .withColumn("load_type", F.lit("incremental"))
        .withColumn("row_hash", F.lit("h"))
        .withColumn("batch_date", F.lit("2024-01-02").cast("date"))
        .withColumn("load_timestamp", F.current_timestamp())
    )
    silver, rejected = split_silver(build_silver_candidates(bronze))
    assert silver.count() == 0
    reasons = set(rejected.collect()[0]["reject_reason"].split(";"))
    assert {"INVALID_DATE", "INVALID_OPEN", "INVALID_VOLUME"} <= reasons


def test_multiple_reasons_are_all_reported(spark):
    row = ("", date(2024, 1, 2), -1.0, 2.0, 1.0, 1.5, 10)
    _, rejected = split_silver(build_silver_candidates(bronze_from(spark, [row])))
    assert {"MISSING_TICKER", "NON_POSITIVE_OPEN"} <= set(rejected.collect()[0]["reject_reason"].split(";"))


def test_duplicate_ticker_day_across_batches_keeps_latest_batch(spark):
    older = bronze_from(spark, [GOOD], batch_id="full_20240101", batch_date=date(2024, 1, 1), load_type="full")
    corrected = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.9, 1_000)
    newer = bronze_from(spark, [corrected], batch_id="incremental_20240102", batch_date=date(2024, 1, 2))
    silver, _ = split_silver(build_silver_candidates(older.unionByName(newer)))
    rows = silver.collect()
    assert len(rows) == 1
    assert rows[0]["close"] == 104.9
    assert rows[0]["source_batch_id"] == "incremental_20240102"


def test_silver_hash_is_stable_between_runs(spark):
    a, _ = split_silver(build_silver_candidates(bronze_from(spark, [GOOD])))
    b, _ = split_silver(build_silver_candidates(bronze_from(spark, [GOOD])))
    assert a.collect()[0]["row_hash"] == b.collect()[0]["row_hash"]
