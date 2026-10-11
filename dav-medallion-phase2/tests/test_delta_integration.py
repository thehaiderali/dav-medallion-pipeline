"""Real Delta regression tests for the submitted architecture and manual fallback."""
import json

import pytest

from pipeline.config import LakehousePaths
from pipeline.demo import demo
from pipeline.engine import run_pipeline
from pipeline.storage import read_table


def test_complete_rubric_proof(delta_spark, tmp_path):
    evidence = demo(delta_spark, str(tmp_path / "inputs"), str(tmp_path / "lake"))
    assert evidence["silver_unchanged_on_identical_rerun"]
    assert evidence["duplicate_silver_keys"] == 0


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_manual_file_fallback_and_type_drift(delta_spark, tmp_path, file_format):
    spark = delta_spark
    metadata = tmp_path / "companies.csv"
    metadata.write_text("ticker,company_name,sector,sub_industry\nAAPL,Apple,Technology,Hardware\nMSFT,Microsoft,Technology,Software\n")
    raw = tmp_path / f"manual.{file_format}"
    frame = spark.createDataFrame([
        ("AAPL", "2026-10-01", "100", "110", "99", "105", "1000", "2.0"),
        ("MSFT", "2026-10-01", "100", "110", "99", "105", "bad", "3.0"),
    ], "ticker string, date string, open string, high string, low string, close string, volume string, dividend string")
    if file_format == "csv":
        raw.write_text("ticker,date,open,high,low,close,volume,dividend\nAAPL,2026-10-01,100,110,99,105,1000,2.0\nMSFT,2026-10-01,100,110,99,105,bad,3.0\n")
    else:
        frame.write.parquet(str(raw))
    root = str(tmp_path / "lake")
    steps = run_pipeline(spark, lakehouse_root=root, metadata_file=str(metadata), process_date="2026-10-01", source_file=str(raw), file_format=file_format)
    assert not any(s.failed for s in steps), [(s.layer, s.error_message) for s in steps]
    paths = LakehousePaths.from_root(root)
    assert read_table(spark, paths.silver).count() == 1
    assert "INVALID_VOLUME" in read_table(spark, paths.quarantine).first().reject_reason
    assert any(c.startswith("extra_dividend_") for c in read_table(spark, paths.bronze).columns)


def test_missing_file_is_audited_and_other_backfill_dates_continue(delta_spark, tmp_path):
    spark = delta_spark
    metadata = tmp_path / "companies.csv"
    metadata.write_text("ticker,company_name,sector,sub_industry\nAAPL,Apple,Technology,Hardware\n")
    raw = tmp_path / "raw/incremental/2026-10-02/data.jsonl"
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps({"ticker": "AAPL", "date": "2026-10-02", "open": "100", "high": "110", "low": "99", "close": "105", "volume": "1000"}) + "\n")
    root = str(tmp_path / "lake")
    steps = run_pipeline(spark, layer="raw_to_bronze", lakehouse_root=root, raw_path=str(tmp_path / "raw"), metadata_file=str(metadata), start_date="2026-10-01", end_date="2026-10-02")
    raw_steps = [s for s in steps if s.layer == "RAW_TO_BRONZE"]
    assert [s.status for s in raw_steps] == ["FAILURE", "SUCCESS"]
    assert steps[-1].status == "FAILURE"
    empty = run_pipeline(spark, layer="bronze_to_silver", lakehouse_root=root, batch_id="missing")
    assert any(s.layer == "BRONZE_TO_SILVER" and s.status == "SKIPPED" for s in empty)
