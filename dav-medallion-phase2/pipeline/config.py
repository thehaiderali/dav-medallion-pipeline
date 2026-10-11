"""Paths, batch identifiers and date helpers.

Nothing here is tied to "today": every function takes the date / path it should
work on, which is what makes backfills possible.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator

LOAD_TYPES = ("full", "incremental")

LAYER_RAW_TO_BRONZE = "RAW_TO_BRONZE"
LAYER_BRONZE_TO_SILVER = "BRONZE_TO_SILVER"

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILURE = "FAILURE"
STATUS_SKIPPED = "SKIPPED"


def join_path(root: str, *parts: str) -> str:
    """Join path segments with '/', which works for local, dbfs:/, s3a:// and abfss://."""
    return "/".join([root.rstrip("/"), *[part.strip("/") for part in parts]])


def _has_scheme(path: str) -> bool:
    return "://" in path or path.startswith("dbfs:")


@dataclass(frozen=True)
class LakehousePaths:
    """Location of every Delta table. Path based, so it works with or without a metastore."""

    root: str

    @classmethod
    def from_root(cls, root: str | None = None) -> "LakehousePaths":
        root = root or os.getenv("LAKEHOUSE_ROOT", "lakehouse")
        if not _has_scheme(root) and not root.startswith("uc:"):
            root = os.path.abspath(root)
        return cls(root=root.rstrip("/"))

    @property
    def bronze(self) -> str:
        return self.target("bronze_prices", "bronze/sp500_prices")

    @property
    def silver(self) -> str:
        return self.target("silver_prices", "silver/sp500_prices")

    @property
    def quarantine(self) -> str:
        return self.target("quarantine_prices", "quarantine/sp500_prices_rejected")

    @property
    def logs(self) -> str:
        return self.target("pipeline_execution_logs", "ops/pipeline_execution_logs")

    @property
    def drift(self) -> str:
        return self.target("schema_drift_log", "ops/schema_drift_log")

    def target(self, name: str, path: str) -> str:
        if self.root.startswith("uc:"):
            return f"{self.root}.{name}"
        return join_path(self.root, path)

    @property
    def metadata(self) -> str:
        return self.target("bronze_companies", "bronze/sp500_companies")

    @property
    def silver_metadata(self) -> str:
        return self.target("silver_companies", "silver/sp500_companies")


def parse_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterator[date]:
    """Inclusive range of calendar dates."""
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def make_batch_id(load_type: str, batch_date: date) -> str:
    """Deterministic batch id. Re-running the same batch yields the same id, which is
    what lets the Bronze MERGE recognise (and not duplicate) records it already holds."""
    if load_type not in LOAD_TYPES:
        raise ValueError(f"load_type must be one of {LOAD_TYPES}, got {load_type!r}")
    return f"{load_type}_{batch_date:%Y%m%d}"


def resolve_raw_file(
    raw_path: str,
    load_type: str,
    batch_date: date,
    source_file: str | None = None,
) -> str:
    """Where the raw Parquet file for a batch lives (mirrors the Phase 1 S3 layout)."""
    if source_file:
        return source_file
    if load_type == "full":
        return join_path(raw_path, "full", "full_load.jsonl")
    return join_path(raw_path, "incremental", batch_date.isoformat(), "data.jsonl")
