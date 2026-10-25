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
LAYER_SILVER_TO_GOLD = "SILVER_TO_GOLD"

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILURE = "FAILURE"
STATUS_SKIPPED = "SKIPPED"


def join_path(root: str, *parts: str) -> str:
    """Join path segments with '/', which works for local, dbfs:/, s3a:// and abfss://."""
    return "/".join([root.rstrip("/"), *[part.strip("/") for part in parts]])


@dataclass(frozen=True)
class LakehousePaths:
    """Managed Delta tables for Databricks Free Edition serverless."""

    root: str

    @classmethod
    def from_root(cls, root: str | None = None) -> "LakehousePaths":
        root = root or os.getenv("LAKEHOUSE_ROOT", "uc:workspace.dav_phase2")
        if not root.startswith("uc:") or len(root[3:].split(".")) != 2:
            raise ValueError("lakehouse_root must be uc:<catalog>.<schema>; raw files belong in /Volumes/...")
        return cls(root=root.rstrip("/"))

    @property
    def bronze(self) -> str:
        return self.target("bronze_prices")

    @property
    def silver(self) -> str:
        return self.target("silver_prices")

    @property
    def quarantine(self) -> str:
        return self.target("quarantine_prices")

    @property
    def logs(self) -> str:
        return self.target("pipeline_execution_logs")

    @property
    def drift(self) -> str:
        return self.target("schema_drift_log")

    def target(self, name: str) -> str:
        return f"{self.root}.{name}"

    @property
    def metadata(self) -> str:
        return self.target("bronze_companies")

    @property
    def silver_metadata(self) -> str:
        return self.target("silver_companies")

    @property
    def gold_date(self) -> str:
        return self.target("gold_dim_date")

    @property
    def gold_company(self) -> str:
        return self.target("gold_dim_company")

    @property
    def gold_sector(self) -> str:
        return self.target("gold_dim_sector")

    @property
    def gold_price(self) -> str:
        return self.target("gold_fact_daily_price")

    @property
    def gold_sector_daily(self) -> str:
        return self.target("gold_fact_sector_daily")


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
    """Stable logical batch identity across append-only arrivals and Silver replays."""
    if load_type not in LOAD_TYPES:
        raise ValueError(f"load_type must be one of {LOAD_TYPES}, got {load_type!r}")
    return f"{load_type}_{batch_date:%Y%m%d}"


def resolve_raw_file(
    raw_path: str,
    load_type: str,
    batch_date: date,
    source_file: str | None = None,
) -> str:
    """Resolve a canonical JSONL partition, or use an exact manual file/folder."""
    if source_file:
        return source_file
    if load_type == "full":
        return join_path(raw_path, "full", "full_load.jsonl")
    return join_path(raw_path, "incremental", batch_date.isoformat(), "data.jsonl")
