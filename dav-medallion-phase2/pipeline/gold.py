"""Business-facing star schema derived from the current Silver snapshot."""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from .audit import AuditedStep
from .storage import ensure_table, merge_into, read_table, write_table


def _schema(fields):
    return StructType([StructField(name, kind, True) for name, kind in fields])


DATE_SCHEMA = _schema([
    ("date_key", IntegerType()), ("trade_date", DateType()), ("year", IntegerType()),
    ("quarter", IntegerType()), ("month", IntegerType()), ("day_of_month", IntegerType()),
    ("day_of_week", IntegerType()), ("year_month", StringType()),
])
COMPANY_DIM_SCHEMA = _schema([
    ("ticker", StringType()), ("company_name", StringType()),
    ("sector_key", StringType()), ("sub_industry", StringType()), ("row_hash", StringType()),
])
SECTOR_DIM_SCHEMA = _schema([("sector_key", StringType()), ("sector", StringType())])
PRICE_FACT_SCHEMA = _schema([
    ("date_key", IntegerType()), ("ticker", StringType()),
    ("open", DoubleType()), ("high", DoubleType()), ("low", DoubleType()),
    ("close", DoubleType()), ("adj_close", DoubleType()), ("volume", LongType()),
    ("dollar_volume_proxy", DoubleType()), ("row_hash", StringType()),
    ("gold_updated_at", TimestampType()),
])
SECTOR_FACT_SCHEMA = _schema([
    ("date_key", IntegerType()), ("sector_key", StringType()),
    ("company_observations", LongType()), ("total_volume", LongType()),
    ("dollar_volume_proxy", DoubleType()), ("gold_updated_at", TimestampType()),
])


def ensure_gold_tables(spark, paths):
    for target, contract in [
        (paths.gold_date, DATE_SCHEMA), (paths.gold_company, COMPANY_DIM_SCHEMA),
        (paths.gold_sector, SECTOR_DIM_SCHEMA), (paths.gold_price, PRICE_FACT_SCHEMA),
        (paths.gold_sector_daily, SECTOR_FACT_SCHEMA),
    ]:
        ensure_table(spark, target, contract)


def _date_key(column):
    return F.date_format(column, "yyyyMMdd").cast("int")


def _hash(columns):
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns]), {"ignoreNullFields": "false"}), 256)


def refresh_gold(spark, paths, logger, batch_id=None, load_type=None, batch_date=None):
    """MERGE changed dimensions/prices; rebuild small sector summary from all Silver.

    Batch refresh reads only Silver observations accepted from that logical batch.
    Gold-only execution without a batch performs a full synchronization/backfill.
    """
    ensure_gold_tables(spark, paths)
    steps = []
    metadata = read_table(spark, paths.silver_metadata)
    silver = read_table(spark, paths.silver)
    source = silver.where(F.col("source_batch_date") == F.lit(batch_date)) if batch_date else silver

    with AuditedStep(logger, "SILVER_TO_GOLD_SECTOR_DIM", load_type, batch_id, "gold_dim_sector") as step:
        sectors = metadata.select(F.col("sector")).distinct()
        sectors = sectors.select(F.sha2("sector", 256).alias("sector_key"), "sector")
        step.rows_read = sectors.count()
        if step.rows_read:
            result = merge_into(spark, paths.gold_sector, sectors, ["sector_key"], "NOT (s.sector <=> t.sector)")
            step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
    steps.append(step)

    with AuditedStep(logger, "SILVER_TO_GOLD_COMPANY_DIM", load_type, batch_id, "gold_dim_company") as step:
        companies = metadata.select("ticker", "company_name", F.sha2("sector", 256).alias("sector_key"), "sub_industry")
        companies = companies.withColumn("row_hash", _hash(["ticker", "company_name", "sector_key", "sub_industry"]))
        step.rows_read = companies.count()
        if step.rows_read:
            result = merge_into(spark, paths.gold_company, companies, ["ticker"], "NOT (s.row_hash <=> t.row_hash)")
            step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
    steps.append(step)

    with AuditedStep(logger, "SILVER_TO_GOLD_DATE_DIM", load_type, batch_id, "gold_dim_date") as step:
        dates = source.select("trade_date").distinct()
        dates = dates.select(
            _date_key(F.col("trade_date")).alias("date_key"), "trade_date",
            F.year("trade_date").alias("year"), F.quarter("trade_date").alias("quarter"),
            F.month("trade_date").alias("month"), F.dayofmonth("trade_date").alias("day_of_month"),
            F.dayofweek("trade_date").alias("day_of_week"),
            F.date_format("trade_date", "yyyy-MM").alias("year_month"),
        )
        step.rows_read = dates.count()
        if step.rows_read:
            result = merge_into(spark, paths.gold_date, dates, ["date_key"], "NOT (s.trade_date <=> t.trade_date)")
            step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
        else:
            step.skip("No accepted Silver dates for this batch")
    steps.append(step)

    with AuditedStep(logger, "SILVER_TO_GOLD_PRICE_FACT", load_type, batch_id, "gold_fact_daily_price") as step:
        prices = source.select(
            _date_key(F.col("trade_date")).alias("date_key"), "ticker",
            "open", "high", "low", "close", "adj_close", "volume",
            (F.col("close") * F.col("volume")).alias("dollar_volume_proxy"),
        )
        prices = prices.withColumn("row_hash", _hash(["date_key", "ticker", "open", "high", "low", "close", "adj_close", "volume"]))
        prices = prices.withColumn("gold_updated_at", F.current_timestamp())
        step.rows_read = prices.count()
        if step.rows_read:
            result = merge_into(spark, paths.gold_price, prices, ["date_key", "ticker"], "NOT (s.row_hash <=> t.row_hash)")
            step.rows_inserted, step.rows_updated = result["inserted"], result["updated"]
        else:
            step.skip("No accepted Silver prices for this batch")
    steps.append(step)

    if any(s.failed for s in steps):
        return steps

    with AuditedStep(logger, "SILVER_TO_GOLD_SECTOR_FACT", load_type, batch_id, "gold_fact_sector_daily") as step:
        # Sector membership comes from the current company dimension. A full
        # rebuild also removes groups made obsolete by a metadata refresh.
        enriched = read_table(spark, paths.gold_price).join(
            read_table(spark, paths.gold_company).select("ticker", "sector_key"), "ticker", "inner"
        )
        step.rows_read = enriched.count()
        daily = enriched.groupBy("date_key", "sector_key").agg(
            F.count(F.lit(1)).cast("long").alias("company_observations"),
            F.sum("volume").cast("long").alias("total_volume"),
            F.sum("dollar_volume_proxy").alias("dollar_volume_proxy"),
        ).withColumn("gold_updated_at", F.current_timestamp())
        step.rows_inserted = daily.count()  # complete summary rows materialized
        step.rows_aggregated = step.rows_inserted
        write_table(daily.select(*SECTOR_FACT_SCHEMA.fieldNames()), paths.gold_sector_daily, mode="overwrite")
    steps.append(step)
    return steps
