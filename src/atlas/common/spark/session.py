from functools import lru_cache

from pyspark.sql import SparkSession

from atlas.common.config.models import SparkSettings


@lru_cache(maxsize=32)
def get_spark_session(spark_settings: SparkSettings, app_name: str) -> SparkSession:
    """Create and configure a Spark session.
    Args:
        spark_settings: SparkSettings object.
        app_name: Name of the application.

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
    return builder.getOrCreate()
