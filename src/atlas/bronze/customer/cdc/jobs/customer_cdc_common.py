from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from atlas.common.config.models import AtlasSettings
from atlas.common.paths.loader import get_paths


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
                       .option("includeHeaders", True)
                       .load()
            )

    customer_stream_parsed = (customer_raw_stream
                              .selectExpr("CAST(key as String) AS raw_key", "CAST(value as String) AS raw_value",
                                                    "headers as kafka_headers",
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


def get_customer_paths(settings: AtlasSettings, entity_name: str)  -> tuple[str, str]:
    """Build storage paths for the customer CDC bronze job.
    Args:
        settings: Validated Atlas application settings.
        entity_name: Name of entity folder to write data

    Returns:
        Tuple containing the customer bronze data path and checkpoint path.


    """
    paths = get_paths(settings)
    return (paths.bronze_path(f"customer/cdc/{entity_name}/job"),
            paths.checkpoint_path(f"customer/cdc/{entity_name}/job"))





