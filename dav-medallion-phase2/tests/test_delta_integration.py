"""End-to-end tests of the Delta pipeline (MERGE INTO, idempotency, backfill, drift, audit log).

These need a Delta-enabled Spark (``pip install delta-spark``, or a Databricks cluster) and are
skipped automatically when Delta cannot be loaded.
"""

from __future__ import annotations

from datetime import date

from pipeline.config import LakehousePaths
from pipeline.run_pipeline import run_pipeline
from pipeline.schemas import LOG_SCHEMA, RAW_SCHEMA

AAPL = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0, 1_000)
MSFT = ("MSFT", date(2024, 1, 2), 300.0, 305.0, 299.0, 304.0, 2_000)


def write_raw(spark, path, rows, schema=RAW_SCHEMA):
    spark.createDataFrame(rows, schema=schema).write.mode("overwrite").parquet(str(path))


def table(spark, path):
    return spark.read.format("delta").load(path)


def run(spark, tmp_path, **kwargs):
    return run_pipeline(
        spark,
        lakehouse_root=str(tmp_path / "lake"),
        raw_path=str(tmp_path / "raw"),
        **kwargs,
    )


def paths_for(tmp_path):
    return LakehousePaths.from_root(str(tmp_path / "lake"))


def test_rerunning_same_raw_data_changes_nothing(delta_spark, tmp_path):
    spark = delta_spark
    write_raw(spark, tmp_path / "raw/incremental/2024-01-02/data.parquet", [AAPL, MSFT])

    first = run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")
    silver_before = {r["ticker"]: r.asDict() for r in table(spark, paths_for(tmp_path).silver).collect()}
    assert [s.rows_inserted for s in first] == [2, 2]  # bronze, silver

    second = run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")
    assert [(s.rows_inserted, s.rows_updated) for s in second] == [(0, 0), (0, 0)]

    silver_after = {r["ticker"]: r.asDict() for r in table(spark, paths_for(tmp_path).silver).collect()}
    assert silver_after == silver_before  # no duplicates, load_timestamp untouched
    assert table(spark, paths_for(tmp_path).bronze).count() == 2


def test_changed_source_values_update_in_place(delta_spark, tmp_path):
    spark = delta_spark
    raw = tmp_path / "raw/incremental/2024-01-02/data.parquet"
    write_raw(spark, raw, [AAPL])
    run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")

    corrected = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.9, 1_000)
    write_raw(spark, raw, [corrected])
    steps = run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")

    assert [(s.rows_inserted, s.rows_updated) for s in steps] == [(0, 1), (0, 1)]
    silver = table(spark, paths_for(tmp_path).silver).collect()
    assert len(silver) == 1 and silver[0]["close"] == 104.9


def test_replaying_an_older_batch_does_not_overwrite_newer_silver_data(delta_spark, tmp_path):
    spark = delta_spark
    old_close = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0, 1_000)
    new_close = ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.9, 1_000)
    write_raw(spark, tmp_path / "raw/full/full_load.parquet", [old_close])
    write_raw(spark, tmp_path / "raw/incremental/2024-01-05/data.parquet", [new_close])

    run(spark, tmp_path, layer="all", load_type="full", run_date="2024-01-01")
    run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-05")
    run(spark, tmp_path, layer="bronze_to_silver", batch_id="full_20240101")  # replay the old batch

    (row,) = table(spark, paths_for(tmp_path).silver).collect()
    assert row["close"] == 104.9 and row["source_batch_id"] == "incremental_20240105"


def test_backfill_window_and_skip_missing(delta_spark, tmp_path):
    spark = delta_spark
    for day in ("2024-01-02", "2024-01-04"):
        write_raw(spark, tmp_path / f"raw/incremental/{day}/data.parquet", [AAPL])

    steps = run(
        spark,
        tmp_path,
        layer="raw_to_bronze",
        load_type="incremental",
        start_date="2024-01-02",
        end_date="2024-01-04",
        skip_missing=True,
    )
    assert [s.status for s in steps] == ["SUCCESS", "SKIPPED", "SUCCESS"]
    assert {r["batch_id"] for r in table(spark, paths_for(tmp_path).bronze).collect()} == {
        "incremental_20240102",
        "incremental_20240104",
    }

    # Bronze -> Silver for just one day of the window
    promoted = run(spark, tmp_path, layer="bronze_to_silver", start_date="2024-01-04", end_date="2024-01-04")
    assert [s.batch_id for s in promoted] == ["incremental_20240104"]


def test_missing_file_without_skip_is_logged_as_failure_and_run_continues(delta_spark, tmp_path):
    spark = delta_spark
    write_raw(spark, tmp_path / "raw/incremental/2024-01-03/data.parquet", [AAPL])
    steps = run(
        spark, tmp_path, layer="raw_to_bronze", load_type="incremental", start_date="2024-01-02", end_date="2024-01-03"
    )
    assert [s.status for s in steps] == ["FAILURE", "SUCCESS"]


def test_schema_drift_is_absorbed_and_bad_values_quarantined(delta_spark, tmp_path):
    spark = delta_spark
    drifted = spark.createDataFrame(
        [
            ("AAPL", date(2024, 1, 2), 100.0, 105.0, 99.0, 104.0, "1000", 103.5),
            ("MSFT", date(2024, 1, 2), 300.0, 305.0, 299.0, 304.0, "lots", 303.5),  # un-castable volume
        ],
        schema="ticker string, date date, open double, high double, low double, close double, "
        "volume string, adj_close double",
    )
    drifted.write.mode("overwrite").parquet(str(tmp_path / "raw/incremental/2024-01-02/data.parquet"))

    steps = run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")
    assert all(not s.failed for s in steps)

    paths = paths_for(tmp_path)
    assert "adj_close" in table(spark, paths.bronze).columns  # schema evolved
    assert [r["ticker"] for r in table(spark, paths.silver).collect()] == ["AAPL"]
    (rejected,) = table(spark, paths.quarantine).collect()
    assert rejected["ticker"] == "MSFT" and "INVALID_VOLUME" in rejected["reject_reason"]
    drift_types = {r["drift_type"] for r in table(spark, paths.drift).collect()}
    assert drift_types == {"TYPE_CHANGE", "NEW_COLUMN"}
    assert steps[-1].rows_quarantined == 1


def test_every_table_row_has_load_timestamp_and_logs_are_complete(delta_spark, tmp_path):
    spark = delta_spark
    write_raw(spark, tmp_path / "raw/incremental/2024-01-02/data.parquet", [AAPL])
    run(spark, tmp_path, layer="all", load_type="incremental", run_date="2024-01-02")
    paths = paths_for(tmp_path)

    for path in (paths.bronze, paths.silver, paths.logs):
        frame = table(spark, path)
        assert frame.count() > 0
        assert frame.where("load_timestamp IS NULL").count() == 0

    logs = table(spark, paths.logs).collect()
    assert {r["pipeline_layer"] for r in logs} == {"RAW_TO_BRONZE", "BRONZE_TO_SILVER"}
    assert set(table(spark, paths.logs).columns) >= {f.name for f in LOG_SCHEMA.fields}
    for record in logs:
        assert record["start_time"] <= record["end_time"]
        assert record["status"] == "SUCCESS"
        assert record["parameter"]
