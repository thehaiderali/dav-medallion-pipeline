"""Pure DataFrame transformations (no Delta, no I/O), so they are unit-testable
with a plain local Spark session.

Bronze  : everything is a string; metadata + hashes are added.
Silver  : strings are cast with ``try_cast`` (bad values become NULL instead of
          raising), validation rules produce a list of reject reasons per row,
          valid rows are de-duplicated per (ticker, trade_date).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .schemas import CONTRACT_COLUMNS, PRICE_COLUMNS, RAW_SCHEMA


# --------------------------------------------------------------------------- #
# Schema drift
# --------------------------------------------------------------------------- #
def detect_drift(actual: StructType, expected: StructType = RAW_SCHEMA) -> list[dict]:
    """Compare the schema found in a raw file with the contract.

    Returns one dict per drift: NEW_COLUMN, MISSING_COLUMN or TYPE_CHANGE.
    Nullability is ignored; column names are compared case-insensitively.
    """
    expected_by_name = {f.name.lower(): f.dataType.simpleString() for f in expected.fields}
    actual_by_name = {f.name.lower(): f.dataType.simpleString() for f in actual.fields}

    drift: list[dict] = []
    for name, actual_type in actual_by_name.items():
        if name not in expected_by_name:
            drift.append(
                {"drift_type": "NEW_COLUMN", "column_name": name, "expected_type": None, "actual_type": actual_type}
            )
        elif expected_by_name[name] != actual_type:
            drift.append(
                {
                    "drift_type": "TYPE_CHANGE",
                    "column_name": name,
                    "expected_type": expected_by_name[name],
                    "actual_type": actual_type,
                }
            )
    for name, expected_type in expected_by_name.items():
        if name not in actual_by_name:
            drift.append(
                {"drift_type": "MISSING_COLUMN", "column_name": name, "expected_type": expected_type, "actual_type": None}
            )
    return drift


def _clean_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


# --------------------------------------------------------------------------- #
# Raw -> Bronze
# --------------------------------------------------------------------------- #
def to_bronze_strings(df: DataFrame) -> DataFrame:
    """Land every column as STRING.

    * contract columns come first, in contract order; a missing one becomes NULL
    * columns that are not in the contract (new in the source) are kept, as strings,
      so Delta schema evolution can add them to Bronze instead of losing them
    """
    by_clean_name = {_clean_name(c): c for c in df.columns}
    selected = []
    for name in CONTRACT_COLUMNS:
        if name in by_clean_name:
            selected.append(F.col(f"`{by_clean_name[name]}`").cast("string").alias(name))
        else:
            selected.append(F.lit(None).cast("string").alias(name))
    for clean, original in by_clean_name.items():
        if clean not in CONTRACT_COLUMNS:
            selected.append(F.col(f"`{original}`").cast("string").alias(clean))
    return df.select(*selected)


def _string_hash(columns: list[str]) -> F.Column:
    parts = [F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_file: str,
    batch_id: str,
    load_type: str,
    batch_date: date,
) -> DataFrame:
    """Add lineage, a deterministic record key, a content hash and the load timestamp,
    then keep one row per record_key so the MERGE source is unique."""
    enriched = (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("load_type", F.lit(load_type))
        .withColumn("batch_date", F.lit(batch_date.isoformat()).cast("date"))
        .withColumn(
            "record_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(batch_id),
                    F.coalesce(F.col("ticker"), F.lit("")),
                    F.coalesce(F.col("date"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("row_hash", _string_hash(CONTRACT_COLUMNS))
        .withColumn("load_timestamp", F.current_timestamp())
    )
    window = Window.partitionBy("record_key").orderBy("row_hash")
    deduped = (
        enriched.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
    )
    leading = ["record_key", *CONTRACT_COLUMNS]
    trailing = ["source_file", "batch_id", "load_type", "batch_date", "row_hash", "load_timestamp"]
    extras = [c for c in deduped.columns if c not in leading and c not in trailing]
    return deduped.select(*leading, *extras, *trailing)


# --------------------------------------------------------------------------- #
# Bronze -> Silver
# --------------------------------------------------------------------------- #
def build_silver_candidates(bronze: DataFrame) -> DataFrame:
    """Cast Bronze strings to their Silver types and compute ``reject_reasons``.

    ``try_cast`` is used on purpose: a non-conforming value becomes NULL (and is
    reported as a reject reason) rather than failing the whole batch.
    """
    typed = (
        bronze.withColumn("s_ticker", F.upper(F.trim(F.col("ticker"))))
        .withColumn("s_trade_date", F.expr("try_cast(`date` as date)"))
        .withColumn("s_open", F.expr("try_cast(`open` as double)"))
        .withColumn("s_high", F.expr("try_cast(`high` as double)"))
        .withColumn("s_low", F.expr("try_cast(`low` as double)"))
        .withColumn("s_close", F.expr("try_cast(`close` as double)"))
        .withColumn("s_volume_d", F.expr("try_cast(`volume` as double)"))
        .withColumn("s_volume", F.expr("try_cast(s_volume_d as bigint)"))
    )

    checks = [
        F.when(F.col("s_ticker").isNull() | (F.col("s_ticker") == ""), F.lit("MISSING_TICKER")),
        F.when(F.col("s_trade_date").isNull(), F.lit("INVALID_DATE")),
    ]
    for name in PRICE_COLUMNS:
        col = F.col(f"s_{name}")
        checks.append(F.when(col.isNull(), F.lit(f"INVALID_{name.upper()}")))
        checks.append(F.when(F.isnan(col), F.lit(f"NAN_{name.upper()}")))
        checks.append(F.when(col <= 0, F.lit(f"NON_POSITIVE_{name.upper()}")))
    checks += [
        F.when(F.col("s_volume_d").isNull(), F.lit("INVALID_VOLUME")),
        F.when(F.col("s_volume_d") < 0, F.lit("NEGATIVE_VOLUME")),
        F.when(F.col("s_volume_d") != F.floor(F.col("s_volume_d")), F.lit("NON_INTEGER_VOLUME")),
        F.when(F.col("s_high") < F.col("s_low"), F.lit("HIGH_LESS_THAN_LOW")),
    ]
    reasons = F.filter(F.array(*checks), lambda reason: reason.isNotNull())
    return typed.withColumn("reject_reasons", reasons)


def split_silver(candidates: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return ``(silver_rows, quarantine_rows)`` ready to be MERGEd."""
    valid = candidates.where(F.size("reject_reasons") == 0)

    window = Window.partitionBy("s_ticker", "s_trade_date").orderBy(
        F.col("batch_date").desc(), F.col("load_timestamp").desc(), F.col("record_key").desc()
    )
    latest = valid.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1)

    silver = latest.select(
        F.col("s_ticker").alias("ticker"),
        F.col("s_trade_date").alias("trade_date"),
        F.col("s_open").alias("open"),
        F.col("s_high").alias("high"),
        F.col("s_low").alias("low"),
        F.col("s_close").alias("close"),
        F.col("s_volume").alias("volume"),
        F.col("batch_id").alias("source_batch_id"),
        F.col("batch_date").alias("source_batch_date"),
    )
    silver = silver.withColumn(
        "row_hash",
        _string_hash(["ticker", "trade_date", "open", "high", "low", "close", "volume"]),
    ).withColumn("load_timestamp", F.current_timestamp())
    silver = silver.select(
        "ticker",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "row_hash",
        "source_batch_id",
        "source_batch_date",
        "load_timestamp",
    )

    rejected = (
        candidates.where(F.size("reject_reasons") > 0)
        .withColumn("reject_stage", F.lit("BRONZE_TO_SILVER"))
        .withColumn("reject_reason", F.concat_ws(";", F.col("reject_reasons")))
        .withColumn("load_timestamp", F.current_timestamp())
        .select(
            "record_key",
            *CONTRACT_COLUMNS,
            "source_file",
            "batch_id",
            "load_type",
            "batch_date",
            "row_hash",
            "reject_stage",
            "reject_reason",
            "load_timestamp",
        )
    )
    return silver, rejected
