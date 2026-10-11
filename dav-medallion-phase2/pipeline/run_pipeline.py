"""CLI and notebook entry point for Phase 2 (Bronze and Silver only)."""
from __future__ import annotations

import argparse

from .engine import run_pipeline


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=["all", "raw_to_bronze", "bronze_to_silver"], default="all")
    parser.add_argument("--load-type", choices=["full", "incremental"], default="incremental")
    for option in ["lakehouse-root", "raw-path", "metadata-file", "process-date", "run-date", "start-date", "end-date", "batch-id", "source-file", "run-id"]:
        parser.add_argument(f"--{option}")
    parser.add_argument("--file-format", choices=["auto", "jsonl", "csv", "parquet"], default="auto")
    parser.add_argument("--lineage-mode", choices=["metadata", "input_file_name"])
    parser.add_argument("--skip-missing", action="store_true")
    parser.add_argument("--refresh-metadata", action="store_true")
    return parser


def main(argv=None):
    from .delta_utils import get_spark
    args = build_parser().parse_args(argv)
    steps = run_pipeline(get_spark(), **vars(args))
    for step in steps:
        print(f"{step.layer} {step.batch_id} {step.status} inserted={step.rows_inserted} updated={step.rows_updated} quarantined={step.rows_quarantined} {step.error_message or ''}")
    return int(any(step.failed for step in steps))


if __name__ == "__main__":
    raise SystemExit(main())
