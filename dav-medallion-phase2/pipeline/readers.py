"""Explicit schema-on-read for automatic JSONL and manual CSV/Parquet inputs.

CSV headers and Parquet footers are inspected as metadata before constructing a
StructType. No Spark reader infers a schema. Every raw row becomes a JSON payload.
"""
from __future__ import annotations

import csv
import io
from urllib.parse import urlparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType, BooleanType, DateType, DecimalType, DoubleType, FloatType,
    IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from .contracts import TEXT_SCHEMA


def driver_path(source):
    if source.startswith("file:"):
        return source[5:]
    if source.startswith("dbfs:/"):
        return "/dbfs/" + source[6:]
    return source.replace("s3a://", "s3://", 1)


def header_schema(source):
    path = driver_path(source)
    if path.startswith("s3://"):
        import boto3
        uri = urlparse(path)
        body = boto3.client("s3").get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))["Body"]
        try:
            header = next(csv.reader(io.StringIO(next(body.iter_lines()).decode("utf-8-sig"))))
        finally:
            body.close()
    else:
        with open(path, encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream))
    if not header or len(header) != len(set(header)) or any(not c or "`" in c or c == "_corrupt_record" for c in header):
        raise ValueError("CSV header must contain unique, nonempty safe column names")
    return StructType([StructField(c, StringType(), True) for c in header])


def parquet_schema(source):
    import pyarrow as pa
    import pyarrow.dataset as ds
    arrow = ds.dataset(driver_path(source), format="parquet").schema
    fields = []
    for field in arrow:
        kind = field.type
        if pa.types.is_string(kind) or pa.types.is_large_string(kind):
            spark_type = StringType()
        elif pa.types.is_boolean(kind):
            spark_type = BooleanType()
        elif pa.types.is_int64(kind) or pa.types.is_uint32(kind):
            spark_type = LongType()
        elif pa.types.is_integer(kind):
            spark_type = IntegerType()
        elif pa.types.is_float64(kind):
            spark_type = DoubleType()
        elif pa.types.is_floating(kind):
            spark_type = FloatType()
        elif pa.types.is_date(kind):
            spark_type = DateType()
        elif pa.types.is_timestamp(kind):
            spark_type = TimestampType()
        elif pa.types.is_decimal(kind):
            spark_type = DecimalType(kind.precision, kind.scale)
        elif pa.types.is_binary(kind):
            spark_type = BinaryType()
        else:
            raise ValueError(f"Unsupported Parquet field {field.name}: {kind}; convert to CSV/JSONL")
        fields.append(StructField(field.name, spark_type, True))
    return StructType(fields)


def read_price_payloads(spark, source, lineage_mode, file_format="auto"):
    from .engine import with_file_lineage
    selected = file_format
    if selected == "auto":
        selected = "parquet" if source.lower().endswith(".parquet") else "csv" if source.lower().endswith(".csv") else "jsonl"
    if selected == "jsonl":
        return with_file_lineage(spark.read.schema(TEXT_SCHEMA).text(source), lineage_mode)
    if selected == "csv":
        contract = header_schema(source).add("_corrupt_record", StringType(), True)
        frame = (spark.read.schema(contract).option("header", True)
                 .option("mode", "PERMISSIVE")
                 .option("columnNameOfCorruptRecord", "_corrupt_record").csv(source))
    elif selected == "parquet":
        contract = parquet_schema(source)
        frame = spark.read.schema(contract).parquet(source)
    else:
        raise ValueError("file_format must be auto, jsonl, csv or parquet")
    original_columns = frame.columns
    frame = with_file_lineage(frame, lineage_mode)
    # Preserve extra columns and original values; the Silver contract owns validation.
    return frame.select(F.to_json(F.struct(*[F.col(f"`{c}`").cast("string").alias("_csv_corrupt_record" if c == "_corrupt_record" else c) for c in original_columns]), {"ignoreNullFields": "false"}).alias("value"), "_source_file")
