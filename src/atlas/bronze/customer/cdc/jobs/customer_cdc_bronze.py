from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from atlas.common.config.loader import get_settings
from atlas.common.config.models import AtlasSettings
from atlas.common.paths.loader import get_paths
from atlas.common.spark.session import get_spark_session


def customer_cdc_read_stream(spark: SparkSession, bootstrap_servers: str,
                             cdc_topic_name: str) -> DataFrame:
    """Read raw customer CDC events from Kafka.
    Args:
        spark: Active Spark session.
        bootstrap_servers: Kafka bootstrap server addresses.
        cdc_topic_name: Kafka topic containing customer CDC events.

    Returns:
        Streaming DataFrame containing raw customer CDC events and Kafka metadata.
    """
    customer_raw_stream = (spark.readStream.format("kafka")
                       .option("kafka.bootstrap.servers", bootstrap_servers)
                       .option("subscribe", cdc_topic_name)
                       .option("startingOffsets", "earliest")
                       .load()
            )

    customer_stream_parsed = (customer_raw_stream
                              .selectExpr("CAST(key as String) AS raw_key", "CAST(value as String) AS raw_value",
                                                    " topic AS kafka_topic", "partition AS kafka_partition",
                                                    "offset AS kafka_offset", "timestamp AS kafka_timestamp"))

    customer_stream_parsed = (customer_stream_parsed
     .withColumn("is_tombstone", F.when(F.col("raw_value").isNull(), True).otherwise(False))
     .withColumn("ingested_at", F.current_timestamp())
     )

    customer_stream_parsed = customer_stream_parsed.withColumn("ingested_date", F.to_date("ingested_at"))

    return customer_stream_parsed


def customer_cdc_write_stream(customer_data_bronze: DataFrame, customer_data_path: str,
                              customer_checkpoint_path: str) -> None:
    """Write customer CDC events to the bronze layer.
    Args:
        customer_data_bronze: Streaming DataFrame containing customer CDC events.
        customer_data_path: Destination path for bronze customer data.
        customer_checkpoint_path: Checkpoint path for the streaming query.

    Returns:
        None.
    """
    query =(
    customer_data_bronze.writeStream.format("parquet")
    .outputMode("append")
    .option("checkpointLocation", customer_checkpoint_path)
    .partitionBy("ingested_date")
    .trigger(availableNow=True)
    .start(customer_data_path)
    )

    query.awaitTermination()


def get_customer_paths(settings: AtlasSettings)  -> tuple[str, str]:
    """Build storage paths for the customer CDC bronze job.
    Args:
        settings: Validated Atlas application settings.

    Returns:
        Tuple containing the customer bronze data path and checkpoint path.

    """
    paths = get_paths(settings)
    return paths.bronze_path("customer/cdc/customers/job"), paths.checkpoint_path("customer/cdc/customers/job")


def customer_cdc_bronze() -> None:
    """Run the customer CDC bronze ingestion job.
        Loads application settings, initializes Spark, reads customer CDC events
        from Kafka, and writes the raw events to the bronze storage layer.
        """
    settings = get_settings("configs/base.yaml", "configs/local.yaml", "pyproject.toml")
    spark = get_spark_session(settings.spark, settings.storage, settings.application.name)

    customer_data_bronze = customer_cdc_read_stream(spark, settings.kafka.bootstrap_servers,
                                                    settings.customer.cdc_topic)

    customer_data_path, customer_checkpoint_path = get_customer_paths(settings)
    customer_cdc_write_stream(customer_data_bronze, customer_data_path, customer_checkpoint_path)

if __name__ == "__main__":
    customer_cdc_bronze()




