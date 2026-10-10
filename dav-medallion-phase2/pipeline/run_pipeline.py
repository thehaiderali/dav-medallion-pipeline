"""Orchestration + command line interface.

Examples
--------
Standard daily incremental (yesterday's/today's partition, then promote to Silver)::

    python -m pipeline.run_pipeline --layer all --load-type incremental \\
        --raw-path s3a://my-bucket/raw/sp500 --run-date 2024-01-05

Backfill a month of incremental partitions into Bronze only::

    python -m pipeline.run_pipeline --layer raw_to_bronze --load-type incremental \\
        --raw-path s3a://my-bucket/raw/sp500 \\
        --start-date 2024-01-01 --end-date 2024-01-31 --skip-missing

Re-promote already-landed Bronze data to Silver for any window or batch::

    python -m pipeline.run_pipeline --layer bronze_to_silver --start-date 2024-01-01 --end-date 2024-01-31
    python -m pipeline.run_pipeline --layer bronze_to_silver --batch-id full_20240101
"""

from __future__ import annotations

import argparse
import uuid
from datetime import date

from pyspark.sql import SparkSession

from .audit import AuditedStep, AuditLogger
from .bronze import raw_to_bronze
from .config import LOAD_TYPES, LakehousePaths, date_range, parse_date
from .delta_utils import ensure_tables, get_spark
from .silver import bronze_to_silver, select_bronze_batches

LAYERS = ("raw_to_bronze", "bronze_to_silver", "all")


def run_pipeline(
    spark: SparkSession,
    *,
    layer: str,
    lakehouse_root: str | None = None,
    raw_path: str | None = None,
    load_type: str | None = None,
    run_date: str | date | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    batch_id: str | None = None,
    source_file: str | None = None,
    skip_missing: bool = False,
    run_id: str | None = None,
) -> list[AuditedStep]:
    """Run one or both layers. Used by the CLI and by the Databricks notebooks.

    Parameters decide *what* is processed - nothing is hardcoded to "today":

    * ``run_date``               a single batch date (default: today)
    * ``start_date``/``end_date`` inclusive window (backfill)
    * ``batch_id``               Bronze->Silver only: promote exactly this batch
    * ``source_file``            Raw->Bronze only: read this exact file instead of the default layout
    """
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {LAYERS}")
    if load_type is not None and load_type not in LOAD_TYPES:
        raise ValueError(f"load_type must be one of {LOAD_TYPES}")

    run_date = parse_date(run_date) if run_date else None
    start_date = parse_date(start_date) if start_date else None
    end_date = parse_date(end_date) if end_date else None
    if (start_date is None) != (end_date is None):
        raise ValueError("--start-date and --end-date must be given together")

    paths = LakehousePaths.from_root(lakehouse_root)
    ensure_tables(spark, paths)
    logger = AuditLogger(spark, paths.logs, run_id or uuid.uuid4().hex)
    steps: list[AuditedStep] = []

    today = date.today()

    if layer in ("raw_to_bronze", "all"):
        if load_type is None:
            raise ValueError("--load-type is required for the raw_to_bronze layer")
        if raw_path is None and source_file is None:
            raise ValueError("--raw-path (or --source-file) is required for the raw_to_bronze layer")
        if load_type == "full" and start_date:
            raise ValueError("a full load is a single file: use --run-date, not --start-date/--end-date")
        # Dates the Bronze step will load (one raw file per date for incremental loads).
        if load_type == "incremental" and start_date:
            batch_dates = list(date_range(start_date, end_date))
        else:
            batch_dates = [run_date or today]
        if layer == "all":
            silver_start, silver_end = batch_dates[0], batch_dates[-1]
        for batch_date in batch_dates:
            steps.append(
                raw_to_bronze(
                    spark,
                    paths,
                    logger,
                    raw_path=raw_path or "",
                    load_type=load_type,
                    batch_date=batch_date,
                    source_file=source_file,
                    skip_missing=skip_missing,
                )
            )

    if layer in ("bronze_to_silver", "all"):
        if layer == "bronze_to_silver":
            # explicit selection: a batch id, a window, or one date; no filter = every Bronze batch
            if start_date:
                silver_start, silver_end = start_date, end_date
            elif run_date:
                silver_start = silver_end = run_date
            else:
                silver_start = silver_end = None
        batches = select_bronze_batches(
            spark,
            paths,
            batch_id=batch_id,
            load_type=load_type,
            start_date=None if batch_id else silver_start,
            end_date=None if batch_id else silver_end,
        )
        for batch in batches:
            steps.append(bronze_to_silver(spark, paths, logger, batch))

    return steps


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="S&P 500 Bronze/Silver Lakehouse pipeline")
    parser.add_argument("--layer", choices=LAYERS, required=True)
    parser.add_argument("--load-type", choices=LOAD_TYPES, help="full | incremental")
    parser.add_argument("--lakehouse-root", help="Root folder for Delta tables (env LAKEHOUSE_ROOT)")
    parser.add_argument("--raw-path", help="Root of raw files, e.g. s3a://bucket/raw/sp500")
    parser.add_argument("--run-date", help="Single batch date YYYY-MM-DD (default: today)")
    parser.add_argument("--start-date", help="Backfill window start YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", help="Backfill window end YYYY-MM-DD (inclusive)")
    parser.add_argument("--batch-id", help="bronze_to_silver: promote exactly this batch id")
    parser.add_argument("--source-file", help="raw_to_bronze: read this exact file")
    parser.add_argument("--skip-missing", action="store_true", help="Log SKIPPED instead of FAILURE when a raw file is absent")
    parser.add_argument("--run-id", help="Correlation id written to the logs (default: random)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = get_spark()
    steps = run_pipeline(
        spark,
        layer=args.layer,
        lakehouse_root=args.lakehouse_root,
        raw_path=args.raw_path,
        load_type=args.load_type,
        run_date=args.run_date,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_id=args.batch_id,
        source_file=args.source_file,
        skip_missing=args.skip_missing,
        run_id=args.run_id,
    )
    for step in steps:
        print(
            f"{step.layer:17} {step.status:8} {step.batch_id} "
            f"read={step.rows_read} inserted={step.rows_inserted} updated={step.rows_updated} "
            f"quarantined={step.rows_quarantined}"
            + (f"  [{step.error_message}]" if step.error_message else "")
        )
    return 1 if any(step.failed for step in steps) else 0


if __name__ == "__main__":
    raise SystemExit(main())
