"""Audit / execution logging.

Every file or batch processed by the pipeline is wrapped in an ``AuditedStep``.
When the ``with`` block ends, one row is appended to ``pipeline_execution_logs``
with the layer, the parameter processed, start/end time, status and row counts.

A failing step is logged as FAILURE and (by default) does *not* stop the run, so one
bad file cannot take down a whole backfill.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import SparkSession

from .config import STATUS_FAILURE, STATUS_SKIPPED, STATUS_SUCCESS
from .schemas import LOG_SCHEMA
from .storage import write_table


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AuditLogger:
    """Appends audit records to the ``pipeline_execution_logs`` Delta table.

    ``sink`` can be injected (e.g. a list.append in unit tests) to bypass Spark.
    """

    def __init__(
        self,
        spark: SparkSession | None,
        logs_path: str | None,
        run_id: str,
        sink: Callable[[dict], None] | None = None,
    ):
        self.spark = spark
        self.logs_path = logs_path
        self.run_id = run_id
        self._sink = sink

    def write(self, record: dict) -> None:
        record = {**record, "run_id": self.run_id, "parameter_processed": record.get("parameter"), "load_timestamp": utc_now()}
        if self._sink is not None:
            self._sink(record)
            return
        row = tuple(record.get(field.name) for field in LOG_SCHEMA.fields)
        frame = self.spark.createDataFrame([row], schema=LOG_SCHEMA)
        write_table(frame, self.logs_path, evolve=True)


@dataclass
class AuditedStep:
    """Context manager that times a unit of work and writes its audit record."""

    logger: AuditLogger
    layer: str
    load_type: str | None
    batch_id: str | None
    parameter: str
    suppress_errors: bool = True

    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_quarantined: int = 0
    rows_aggregated: int = 0
    status: str = STATUS_SUCCESS
    error_message: str | None = None
    start_time: datetime | None = field(default=None, init=False)
    end_time: datetime | None = field(default=None, init=False)

    @property
    def failed(self) -> bool:
        return self.status == STATUS_FAILURE

    def skip(self, reason: str) -> None:
        """Mark the step as intentionally skipped (e.g. no file for that date)."""
        self.status = STATUS_SKIPPED
        self.error_message = reason

    def __enter__(self) -> "AuditedStep":
        self.start_time = utc_now()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.end_time = utc_now()
        if exc_type is not None:
            self.status = STATUS_FAILURE
            self.error_message = f"{exc_type.__name__}: {exc}"[:2000]
        try:
            self.logger.write(
                {
                    "pipeline_layer": self.layer,
                    "load_type": self.load_type,
                    "batch_id": self.batch_id,
                    "parameter": self.parameter,
                    "start_time": self.start_time,
                    "end_time": self.end_time,
                    "duration_seconds": (self.end_time - self.start_time).total_seconds(),
                    "status": self.status,
                    "rows_read": int(self.rows_read),
                    "rows_inserted": int(self.rows_inserted),
                    "rows_updated": int(self.rows_updated),
                    "rows_quarantined": int(self.rows_quarantined),
                    "rows_aggregated": int(self.rows_aggregated),
                    "error_message": self.error_message,
                }
            )
        except Exception as log_error:
            raise RuntimeError("Execution audit persistence failed") from log_error
        # swallow ordinary errors so the rest of the batch list keeps running
        return bool(exc_type is not None and self.suppress_errors and issubclass(exc_type, Exception))
