"""Shared Spark fixtures.

``spark`` tries to start a Delta-enabled session. If Delta cannot be loaded (for example
no network access to Maven) it falls back to a plain local session, and the tests that
need Delta (MERGE INTO) are skipped. Set PIPELINE_TEST_NO_DELTA=1 to skip the attempt.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _base_builder():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )


@pytest.fixture(scope="session")
def spark_env():
    has_delta = False
    spark = None
    if not os.getenv("PIPELINE_TEST_NO_DELTA"):
        try:
            from delta import configure_spark_with_delta_pip

            builder = (
                _base_builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            has_delta = True
        except Exception as error:  # noqa: BLE001 - any failure means "no Delta here"
            print(f"[tests] Delta unavailable, using plain Spark: {str(error)[:120]}")
            spark = None
    if spark is None:
        spark = _base_builder().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    yield spark, has_delta
    spark.stop()


@pytest.fixture(scope="session")
def spark(spark_env):
    return spark_env[0]


@pytest.fixture(scope="session")
def delta_spark(spark_env):
    spark, has_delta = spark_env
    if not has_delta:
        pytest.skip("Delta Lake is not available in this environment")
    return spark
