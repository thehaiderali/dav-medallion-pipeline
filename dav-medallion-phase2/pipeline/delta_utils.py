"""Reuse Databricks Spark, or build a local Delta-enabled Spark session."""
from pyspark.sql import SparkSession


def get_spark(app_name="dav-phase2"):
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master("local[2]").appName(app_name)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.databricks.delta.snapshotPartitions", "2")
               .config("spark.sql.session.timeZone", "UTC"))
    return configure_spark_with_delta_pip(builder).getOrCreate()
