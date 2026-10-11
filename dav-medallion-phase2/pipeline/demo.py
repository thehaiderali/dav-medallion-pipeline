"""Executable rubric proof shared by Databricks and local Spark; no network needed."""
from __future__ import annotations

import json
from pathlib import Path

from .config import LakehousePaths
from .engine import run_pipeline
from .storage import read_table


def demo(spark, input_root, lakehouse_root):
    directory = Path(input_root)
    directory.mkdir(parents=True, exist_ok=True)
    metadata = directory / "companies.csv"
    metadata.write_text("ticker,company_name,sector,sub_industry\nAAPL,Apple Inc.,Information Technology,Technology Hardware\nMSFT,Microsoft,Information Technology,Systems Software\n", encoding="utf-8")
    paths = LakehousePaths.from_root(lakehouse_root)

    def price(ticker="AAPL", day="2026-09-30", close="104", **extras):
        return {"ticker": ticker, "date": day, "open": "100", "high": "110", "low": "99", "close": close, "adj_close": close, "volume": "1000", **extras}

    def write(name, rows):
        file = directory / name
        file.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
        return str(file)

    history = []

    def run(label, **kwargs):
        steps = run_pipeline(spark, lakehouse_root=lakehouse_root, metadata_file=str(metadata), **kwargs)
        records = [{"layer": s.layer, "batch_id": s.batch_id, "status": s.status, "inserted": s.rows_inserted, "updated": s.rows_updated, "quarantined": s.rows_quarantined, "error": s.error_message} for s in steps]
        print(label, json.dumps(records), flush=True)
        history.append({"label": label, "steps": records})
        assert not any(s.failed for s in steps), records
        return steps

    full = write("full.jsonl", [price(), price("MSFT"), price()])
    run("full load", layer="all", load_type="full", process_date="2026-09-30", source_file=full)
    before = [r.asDict() for r in read_table(spark, paths.silver).orderBy("ticker", "trade_date").collect()]
    rerun = run("identical rerun", layer="all", load_type="full", process_date="2026-09-30", source_file=full)
    after = [r.asDict() for r in read_table(spark, paths.silver).orderBy("ticker", "trade_date").collect()]
    assert before == after, "Silver rows (including timestamps and lineage) changed on replay"
    assert all(s.rows_inserted == s.rows_updated == 0 for s in rerun if s.layer.startswith("BRONZE_TO_SILVER"))
    incremental = write("incremental.jsonl", [price(day="2026-10-01", close="105")])
    run("one-day incremental", layer="all", process_date="2026-10-01", source_file=incremental)
    incremental = write("incremental.jsonl", [price(day="2026-10-01", close="106")])
    updates = run("changed source update", layer="all", process_date="2026-10-01", source_file=incremental)
    assert next(s for s in updates if s.layer == "BRONZE_TO_SILVER").rows_updated == 1
    old = write("historical.jsonl", [price(day="2026-09-28")])
    run("historical raw backfill", layer="raw_to_bronze", process_date="2026-09-28", source_file=old)
    run("historical Silver backfill", layer="bronze_to_silver", process_date="2026-09-28")
    drift = write("drift.jsonl", [price(day="2026-10-02", dividend="1.5"), price("MSFT", day="2026-10-02", volume="not-an-integer", dividend="2.0"), price("UNKNOWN", day="2026-10-02")])
    run("schema drift and quarantine", layer="all", process_date="2026-10-02", source_file=drift)
    assert any(c.startswith("extra_dividend_") for c in read_table(spark, paths.bronze).columns)
    rejects = [r.asDict() for r in read_table(spark, paths.quarantine).collect()]
    assert any("INVALID_VOLUME" in r["reject_reason"] for r in rejects)
    assert any("MISSING_COMPANY_METADATA" in r["reject_reason"] for r in rejects)
    newer = write("newer.jsonl", [price(day="2026-10-01", close="106")])
    run("newer identical observation", layer="all", process_date="2026-10-03", source_file=newer)
    write("incremental.jsonl", [price(day="2026-10-01", close="103")])
    run("older corrected replay rejected", layer="all", process_date="2026-10-01", source_file=incremental)
    current = read_table(spark, paths.silver).where("ticker = 'AAPL' AND trade_date = DATE '2026-10-01'").first()
    assert current.close == 106.0 and str(current.source_batch_date) == "2026-10-03"
    silver = read_table(spark, paths.silver)
    duplicates = silver.groupBy("ticker", "trade_date").count().where("count > 1").count()
    assert duplicates == 0
    for target in [paths.bronze, paths.metadata, paths.silver, paths.silver_metadata]:
        assert read_table(spark, target).where("load_timestamp IS NULL OR source_file IS NULL OR batch_id IS NULL").count() == 0
    logs = [r.asDict() for r in read_table(spark, paths.logs).orderBy("start_time").collect()]
    assert all(r["parameter_processed"] and r["start_time"] <= r["end_time"] for r in logs)
    return {"execution_environment": "Apache Spark / Delta Lake (local or Databricks depending on caller)", "silver_unchanged_on_identical_rerun": before == after, "duplicate_silver_keys": duplicates, "silver_rows": silver.count(), "bronze_schema": read_table(spark, paths.bronze).schema.jsonValue(), "silver_schema": silver.schema.jsonValue(), "history": history, "quarantine": rejects, "execution_logs": logs}
