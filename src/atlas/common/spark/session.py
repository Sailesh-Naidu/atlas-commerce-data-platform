from functools import lru_cache

from pyspark.sql import SparkSession

from atlas.common.config.models import SparkSettings, StorageSettings
from delta import configure_spark_with_delta_pip

@lru_cache(maxsize=32)
def get_spark_session(spark_settings: SparkSettings, storage_settings: StorageSettings, app_name: str) -> SparkSession:
    """Create and configure a Spark session.

    Args:
        spark_settings: SparkSettings object.
        app_name: Name of the application.
        storage_settings: StorageSettings object.

    Returns:
        SparkSession object.
    """
    builder = SparkSession.builder
    builder = builder.appName(app_name)
    builder = builder.master(spark_settings.master)
    builder = builder.config("spark.sql.shuffle.partitions", spark_settings.shuffle_partitions)
    builder = builder.config("spark.sql.session.timeZone", spark_settings.session_timezone)
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", spark_settings.broadcast_size_mb * 1024 * 1024)
    builder = builder.config("spark.sql.adaptive.enabled", spark_settings.adaptive_query_execution)
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if storage_settings.mode == "object_store":
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", storage_settings.endpoint)
        builder = builder.config("spark.hadoop.fs.s3a.access.key", storage_settings.access_key)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", storage_settings.secret_key.get_secret_value())
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = configure_spark_with_delta_pip(builder,extra_packages=["org.apache.hadoop:hadoop-aws:3.4.1",],)

    return builder.getOrCreate()

