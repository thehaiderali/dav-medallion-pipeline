"""Tests for parameter handling and audit logging (no Spark / Delta needed)."""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.audit import AuditedStep, AuditLogger
from pipeline.config import LakehousePaths, date_range, make_batch_id, parse_date, resolve_raw_file


def test_batch_id_is_deterministic():
    assert make_batch_id("incremental", date(2024, 1, 5)) == "incremental_20240105"
    assert make_batch_id("full", date(2024, 1, 5)) == "full_20240105"
    with pytest.raises(ValueError):
        make_batch_id("weekly", date(2024, 1, 5))


def test_date_range_is_inclusive_and_validated():
    days = list(date_range(date(2024, 1, 30), date(2024, 2, 2)))
    assert days[0] == date(2024, 1, 30) and days[-1] == date(2024, 2, 2) and len(days) == 4
    with pytest.raises(ValueError):
        list(date_range(date(2024, 2, 2), date(2024, 1, 30)))


def test_parse_date_accepts_strings_and_dates():
    assert parse_date("2024-01-05") == date(2024, 1, 5)
    assert parse_date(date(2024, 1, 5)) == date(2024, 1, 5)


def test_raw_file_resolution_mirrors_phase1_layout():
    assert resolve_raw_file("s3a://b/raw/sp500", "full", date(2024, 1, 5)) == "s3a://b/raw/sp500/full/full_load.parquet"
    assert (
        resolve_raw_file("s3a://b/raw/sp500/", "incremental", date(2024, 1, 5))
        == "s3a://b/raw/sp500/incremental/2024-01-05/data.parquet"
    )
    assert resolve_raw_file("x", "incremental", date(2024, 1, 5), source_file="custom.parquet") == "custom.parquet"


def test_lakehouse_paths():
    paths = LakehousePaths.from_root("dbfs:/FileStore/lakehouse/")
    assert paths.bronze == "dbfs:/FileStore/lakehouse/bronze/sp500_prices"
    assert paths.logs == "dbfs:/FileStore/lakehouse/ops/pipeline_execution_logs"


def make_logger():
    records: list[dict] = []
    return AuditLogger(None, None, "run-1", sink=records.append), records


def test_success_is_logged_with_metrics_and_times():
    logger, records = make_logger()
    with AuditedStep(logger, "RAW_TO_BRONZE", "incremental", "incremental_20240105", "s3a://f") as step:
        step.rows_read, step.rows_inserted, step.rows_updated = 10, 8, 2
    (record,) = records
    assert record["status"] == "SUCCESS"
    assert (record["rows_inserted"], record["rows_updated"]) == (8, 2)
    assert record["pipeline_layer"] == "RAW_TO_BRONZE"
    assert record["parameter"] == "s3a://f"
    assert record["start_time"] <= record["end_time"]
    assert record["run_id"] == "run-1" and record["load_timestamp"] is not None


def test_failure_is_logged_and_does_not_stop_the_run():
    logger, records = make_logger()
    with AuditedStep(logger, "BRONZE_TO_SILVER", "full", "full_20240101", "b") as step:
        raise RuntimeError("boom")
    # execution continues here: the error was swallowed and recorded
    assert step.failed
    assert records[0]["status"] == "FAILURE"
    assert "RuntimeError: boom" in records[0]["error_message"]


def test_errors_can_be_propagated_when_requested():
    logger, records = make_logger()
    with pytest.raises(RuntimeError):
        with AuditedStep(logger, "RAW_TO_BRONZE", "full", "b", "p", suppress_errors=False):
            raise RuntimeError("boom")
    assert records[0]["status"] == "FAILURE"


def test_skipped_status():
    logger, records = make_logger()
    with AuditedStep(logger, "RAW_TO_BRONZE", "incremental", "b", "p") as step:
        step.skip("no file")
    assert records[0]["status"] == "SKIPPED" and records[0]["error_message"] == "no file"
