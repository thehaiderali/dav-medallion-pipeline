"""Phase 2: append Bronze prices, snapshot companies, validate and MERGE Silver."""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date

from pyspark.sql import Window, functions as F
from pyspark.sql.types import MapType, StringType

from .audit import AuditLogger, AuditedStep
from .config import LakehousePaths, date_range, make_batch_id, parse_date, resolve_raw_file
from .contracts import (
    BRONZE_SCHEMA, COMPANY_COLUMNS, COMPANY_RAW_SCHEMA, COMPANY_SCHEMA,
    PRICE_COLUMNS, QUARANTINE_SCHEMA, RAW_SCHEMA, SILVER_SCHEMA,
)
from .schemas import DRIFT_SCHEMA, LOG_SCHEMA
from .storage import ensure_table, merge_into, read_table, write_table
from .readers import read_price_payloads
from .gold import refresh_gold

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def content_hash(columns):
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns]), {"ignoreNullFields": "false"}), 256)


def ensure_tables(spark, paths):
    for target, contract in [
        (paths.logs, LOG_SCHEMA), (paths.bronze, BRONZE_SCHEMA),
        (paths.metadata, COMPANY_SCHEMA), (paths.silver_metadata, COMPANY_SCHEMA),
        (paths.silver, SILVER_SCHEMA), (paths.quarantine, QUARANTINE_SCHEMA),
        (paths.drift, DRIFT_SCHEMA),
    ]:
        ensure_table(spark, target, contract)


def with_file_lineage(frame, lineage_mode):
    if lineage_mode == "metadata":
        return frame.select("*", F.col("_metadata.file_path").alias("_source_file"))
    raise ValueError("Serverless file lineage uses lineage_mode='metadata'")


def load_metadata(spark, paths, logger, metadata_file, lineage_mode, refresh=False):
    with AuditedStep(logger, "RAW_TO_BRONZE_METADATA", "snapshot", "metadata_snapshot", metadata_file or "existing snapshot") as step:
        if read_table(spark, paths.metadata).limit(1).count() and not refresh:
            step.skip("Committed company snapshot already loaded; no web scrape")
            return step
        if not metadata_file:
            raise ValueError("metadata_file is required to initialize company metadata")
        raw = with_file_lineage(spark.read.schema(COMPANY_RAW_SCHEMA).option("header", True).option("mode", "FAILFAST").csv(metadata_file), lineage_mode)
        cleaned = raw.select(
            F.upper(F.regexp_replace(F.trim("ticker"), r"\.", "-")).alias("ticker"),
            *[F.trim(c).alias(c) for c in COMPANY_COLUMNS[1:]],
            F.col("_source_file").alias("snapshot_file"),
        )
        step.rows_read = cleaned.count()
        invalid = cleaned.where(" OR ".join(f"`{c}` IS NULL OR trim(`{c}`) = ''" for c in COMPANY_COLUMNS)).count()
        if not step.rows_read or invalid or cleaned.groupBy("ticker").count().where("count > 1").count():
            raise ValueError("Company snapshot must be nonempty, complete and unique on ticker")
        digest = hashlib.sha256(json.dumps([r.asDict() for r in cleaned.select(*COMPANY_COLUMNS).orderBy("ticker").collect()], sort_keys=True).encode()).hexdigest()[:16]
        step.batch_id = f"metadata_{digest}"
        companies = (cleaned.withColumn("row_hash", content_hash(COMPANY_COLUMNS))
                     .withColumn("source_file", F.lit(WIKIPEDIA_URL))
                     .withColumn("batch_id", F.lit(f"metadata_{digest}"))
                     .withColumn("load_timestamp", F.current_timestamp()))
        write_table(companies, paths.metadata, "overwrite")
        step.rows_inserted = step.rows_read  # snapshot replacement, not an upsert
    return step


def promote_metadata(spark, paths, logger):
    with AuditedStep(logger, "BRONZE_TO_SILVER_METADATA", "snapshot", "metadata_snapshot", paths.metadata) as step:
        source = read_table(spark, paths.metadata).withColumn("load_timestamp", F.current_timestamp())
        step.rows_read = source.count()
        if not step.rows_read:
            raise ValueError("Company snapshot is empty")
        step.batch_id = source.select("batch_id").first()[0]
        result = merge_into(spark, paths.silver_metadata, source, ["ticker"], "NOT (s.row_hash <=> t.row_hash)")
        step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
    return step


def raw_to_bronze(spark, paths, logger, *, raw_path, load_type, batch_date, source_file=None, skip_missing=False, lineage_mode="metadata", file_format="auto"):
    source = resolve_raw_file(raw_path, load_type, batch_date, source_file)
    batch_id = make_batch_id(load_type, batch_date)
    with AuditedStep(logger, "RAW_TO_BRONZE", load_type, batch_id, source) as step:
        try:
            raw = read_price_payloads(spark, source, lineage_mode, file_format)
        except Exception as error:
            if skip_missing and (isinstance(error, FileNotFoundError) or "PATH_NOT_FOUND" in str(error) or "does not exist" in str(error)):
                step.skip("Raw partition is absent")
                return step
            raise
        parsed = raw.withColumn("_fields", F.from_json("value", MapType(StringType(), StringType())))
        source_columns = [*RAW_SCHEMA.fieldNames(), "_csv_corrupt_record"]
        extra_keys = [r["key"] for r in parsed.select(F.explode(F.map_keys("_fields")).alias("key")).distinct().collect() if r["key"] not in source_columns]
        contract = parsed.withColumn("_contract", F.from_json("value", RAW_SCHEMA, {"mode": "PERMISSIVE"}))
        bronze = contract.select(
            *[F.col(f"_contract.{c}").alias(c) for c in PRICE_COLUMNS],
            F.col("value").alias("raw_payload"),
            F.to_json(F.map_filter("_fields", lambda k, v: ~k.isin(*source_columns))).alias("rescued_data"),
            F.col("_source_file").alias("source_file"),
            *[F.element_at("_fields", F.lit(key)).cast("string").alias("extra_" + re.sub(r"[^a-zA-Z0-9_]", "_", key)[:40] + "_" + hashlib.sha256(key.encode()).hexdigest()[:8]) for key in sorted(extra_keys)],
        )
        bronze = (bronze.withColumn("batch_id", F.lit(batch_id))
                  .withColumn("batch_date", F.lit(batch_date).cast("date"))
                  .withColumn("load_type", F.lit(load_type)).withColumn("run_id", F.lit(logger.run_id))
                  .withColumn("record_key", content_hash(["batch_id", "source_file", "raw_payload"]))
                  .withColumn("bronze_row_id", F.expr("uuid()"))
                  .withColumn("load_timestamp", F.current_timestamp()))
        step.rows_read = bronze.count()
        write_table(bronze, paths.bronze, evolve=True)  # raw arrivals are append-only, including replays
        step.rows_inserted = step.rows_read
        if extra_keys:
            from .audit import utc_now
            rows = [(logger.run_id, batch_id, source, "NEW_COLUMN", key, None, "string", "Bronze schema evolved; original JSON retained", utc_now()) for key in sorted(extra_keys)]
            write_table(spark.createDataFrame(rows, DRIFT_SCHEMA), paths.drift)
    return step


def price_candidates(bronze):
    typed = bronze.withColumn("s_ticker", F.upper(F.regexp_replace(F.trim("ticker"), r"\.", "-")))
    typed = typed.withColumn("s_trade_date", F.expr("try_cast(`date` as date)"))
    for name in ["open", "high", "low", "close", "adj_close"]:
        typed = typed.withColumn(f"s_{name}", F.expr(f"try_cast(`{name}` as double)"))
    # Adjusted close is optional for older feeds; malformed supplied values are rejected.
    typed = typed.withColumn("s_adj_close", F.when(F.col("adj_close").isNull(), F.col("s_close")).otherwise(F.col("s_adj_close")))
    typed = typed.withColumn("_volume", F.expr("try_cast(volume as decimal(38,18))")).withColumn("s_volume", F.expr("try_cast(_volume as bigint)"))
    checks = [
        F.when(F.col("s_ticker").isNull() | (F.col("s_ticker") == ""), F.lit("MISSING_TICKER")),
        F.when(F.col("s_trade_date").isNull(), F.lit("INVALID_DATE")),
        F.when(F.from_json("raw_payload", RAW_SCHEMA).getField("_corrupt_record").isNotNull(), F.lit("INVALID_JSON")),
        F.when(F.get_json_object("raw_payload", "$._csv_corrupt_record").isNotNull(), F.lit("MALFORMED_CSV")),
    ]
    for name in ["open", "high", "low", "close", "adj_close"]:
        col = F.col(f"s_{name}")
        checks += [F.when(col.isNull() | F.isnan(col) | (F.abs(col) == F.lit(float("inf"))), F.lit(f"INVALID_{name.upper()}")), F.when(col <= 0, F.lit(f"NON_POSITIVE_{name.upper()}"))]
    checks += [
        F.when(F.col("_volume").isNull() | F.col("s_volume").isNull(), F.lit("INVALID_VOLUME")),
        F.when(F.col("_volume") < 0, F.lit("NEGATIVE_VOLUME")),
        F.when(F.col("_volume") != F.floor("_volume"), F.lit("NON_INTEGER_VOLUME")),
        F.when(F.col("s_high") < F.col("s_low"), F.lit("HIGH_LESS_THAN_LOW")),
    ]
    return typed.withColumn("reject_reasons", F.filter(F.array(*checks), lambda reason: reason.isNotNull()))


def bronze_to_silver(spark, paths, logger, batch_id, load_type):
    with AuditedStep(logger, "BRONZE_TO_SILVER", load_type, batch_id, batch_id) as step:
        bronze = read_table(spark, paths.bronze).where(F.col("batch_id") == batch_id)
        candidates = price_candidates(bronze)
        step.rows_read = candidates.count()
        metadata = read_table(spark, paths.silver_metadata)
        unmatched = candidates.join(metadata.select(F.col("ticker").alias("s_ticker")), "s_ticker", "left_anti").withColumn("reject_reasons", F.array_union("reject_reasons", F.array(F.lit("MISSING_COMPANY_METADATA"))))
        matched = candidates.join(metadata.select(F.col("ticker").alias("s_ticker")), "s_ticker", "left_semi")
        rejected = matched.where(F.size("reject_reasons") > 0).unionByName(unmatched)
        rejected = rejected.withColumn("reject_reason", F.concat_ws(";", "reject_reasons")).withColumn("reject_stage", F.lit("BRONZE_TO_SILVER"))
        rq_window = Window.partitionBy("record_key").orderBy(F.col("load_timestamp").desc(), F.col("bronze_row_id").desc())
        rejected = rejected.withColumn("_rn", F.row_number().over(rq_window)).where("_rn = 1").withColumn("load_timestamp", F.current_timestamp()).select(*QUARANTINE_SCHEMA.fieldNames())
        step.rows_quarantined = rejected.count()
        if step.rows_quarantined:
            merge_into(spark, paths.quarantine, rejected, ["record_key"], "NOT (s.reject_reason <=> t.reject_reason)")
        valid = matched.where(F.size("reject_reasons") == 0)
        window = Window.partitionBy("s_ticker", "s_trade_date").orderBy(F.col("batch_date").desc(), F.col("load_timestamp").desc(), F.col("record_key").desc())
        valid = valid.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
        prices = valid.select(F.col("s_ticker").alias("ticker"), F.col("s_trade_date").alias("trade_date"), *[F.col(f"s_{c}").alias(c) for c in ["open", "high", "low", "close", "adj_close", "volume"]], "source_file", "batch_id", F.col("batch_date").alias("source_batch_date"))
        company_fields = metadata.select("ticker", "company_name", "sector", "sub_industry", F.col("source_file").alias("metadata_source_file"), F.col("batch_id").alias("metadata_batch_id"))
        enriched = prices.join(company_fields, "ticker", "inner")
        business = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "company_name", "sector", "sub_industry"]
        enriched = enriched.withColumn("row_hash", content_hash(business)).withColumn("load_timestamp", F.current_timestamp()).select(*SILVER_SCHEMA.fieldNames())
        result = merge_into(spark, paths.silver, enriched, ["ticker", "trade_date"], "t.source_batch_date IS NULL OR s.source_batch_date > t.source_batch_date OR (s.source_batch_date = t.source_batch_date AND NOT (s.row_hash <=> t.row_hash))")
        step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
    return step


def run_pipeline(spark, *, layer="all", lakehouse_root=None, raw_path=None, metadata_file=None, process_date=None, run_date=None, start_date=None, end_date=None, load_type="incremental", batch_id=None, source_file=None, skip_missing=False, run_id=None, refresh_metadata=False, lineage_mode=None, file_format="auto"):
    paths = LakehousePaths.from_root(lakehouse_root)
    logger = AuditLogger(spark, paths.logs, run_id or uuid.uuid4().hex)
    parameters = json.dumps({"layer": layer, "process_date": str(process_date or run_date or date.today()), "start_date": str(start_date), "end_date": str(end_date), "batch_id": batch_id, "source_file": source_file}, sort_keys=True)
    steps = []
    # Initialize the audit destination before parameter validation so invalid
    # date/window selections can still produce a persisted FAILURE record.
    ensure_table(spark, paths.logs, LOG_SCHEMA)
    with AuditedStep(logger, "PIPELINE", load_type, batch_id, parameters, suppress_errors=False) as invocation:
        if layer not in ("all", "raw_to_bronze", "bronze_to_silver", "silver_to_gold"):
            raise ValueError("Unsupported layer")
        if load_type not in ("full", "incremental"):
            raise ValueError("load_type must be full or incremental")
        if process_date and run_date and parse_date(process_date) != parse_date(run_date):
            raise ValueError("process_date and run_date disagree")
        if bool(start_date) != bool(end_date):
            raise ValueError("start_date and end_date must be supplied together")
        if start_date and (process_date or run_date or batch_id):
            raise ValueError("Use one process_date/batch_id OR a backfill window")
        if batch_id and (process_date or run_date):
            raise ValueError("Use batch_id OR process_date, not both")
        if source_file and start_date:
            raise ValueError("An explicit source_file represents one batch; use process_date, not a window")
        if load_type == "full" and start_date:
            raise ValueError("Full load is one historical file; supply process_date")
        selected = parse_date(process_date or run_date or date.today())
        dates = list(date_range(parse_date(start_date), parse_date(end_date))) if start_date else [selected]
        lineage_mode = lineage_mode or "metadata"
        ensure_tables(spark, paths)
        if layer in ("all", "raw_to_bronze"):
            steps.append(load_metadata(spark, paths, logger, metadata_file, lineage_mode, refresh_metadata))
            if raw_path is None and source_file is None:
                raise ValueError("raw_path or source_file is required")
            for day in dates:
                steps.append(raw_to_bronze(spark, paths, logger, raw_path=raw_path or "", load_type=load_type, batch_date=day, source_file=source_file, skip_missing=skip_missing, lineage_mode=lineage_mode, file_format=file_format))
        if layer in ("all", "bronze_to_silver"):
            metadata_step = promote_metadata(spark, paths, logger)
            steps.append(metadata_step)
            if metadata_step.failed:
                raise RuntimeError("Silver metadata promotion failed; Gold refresh stopped")
            bronze = read_table(spark, paths.bronze)
            if batch_id:
                bronze = bronze.where(F.col("batch_id") == batch_id)
            else:
                bronze = bronze.where(F.col("batch_date").between(dates[0], dates[-1]) & (F.col("load_type") == load_type))
            batches = bronze.select("batch_id", "load_type", "batch_date").distinct().orderBy("batch_date", "batch_id").collect()
            if not batches:
                with AuditedStep(logger, "BRONZE_TO_SILVER", load_type, batch_id, parameters) as empty:
                    empty.skip("No Bronze batches matched this parameter")
                steps.append(empty)
                if metadata_step.rows_inserted or metadata_step.rows_updated:
                    steps.extend(refresh_gold(spark, paths, logger, None, load_type, None))
            for batch in batches:
                silver_step = bronze_to_silver(spark, paths, logger, batch.batch_id, batch.load_type)
                steps.append(silver_step)
                if not silver_step.failed:
                    steps.extend(refresh_gold(spark, paths, logger, batch.batch_id, batch.load_type, batch.batch_date))
        if layer == "silver_to_gold":
            steps.extend(refresh_gold(spark, paths, logger, batch_id, load_type, None))
        invocation.rows_read = sum(s.rows_read for s in steps)
        invocation.rows_inserted = sum(s.rows_inserted for s in steps)
        invocation.rows_updated = sum(s.rows_updated for s in steps)
        invocation.rows_quarantined = sum(s.rows_quarantined for s in steps)
        invocation.rows_aggregated = sum(s.rows_aggregated for s in steps)
        if any(s.failed for s in steps):
            invocation.status = "FAILURE"
            invocation.error_message = "; ".join(s.error_message or "" for s in steps if s.failed)[:2000]
    return [*steps, invocation]
