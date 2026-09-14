import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = structlog.get_logger(__name__)

def entity_cdc_read_stream(spark: SparkSession, bootstrap_servers: str,
                           cdc_topic_name: str) -> DataFrame:
    """Read raw entity CDC events from Kafka.
    Args:
        spark: Active Spark session.
        bootstrap_servers: Kafka bootstrap server addresses.
        cdc_topic_name: Kafka topic containing entity CDC events.

    Returns:
        Streaming DataFrame containing raw entity CDC events and Kafka metadata.
    """
    entity_raw_stream = (spark.readStream.format("kafka")
                       .option("kafka.bootstrap.servers", bootstrap_servers)
                       .option("subscribe", cdc_topic_name)
                       .option("startingOffsets", "earliest")
                       .option("includeHeaders", True)
                       .load()
            )

    entity_stream_parsed = (entity_raw_stream
                              .selectExpr("CAST(key as String) AS raw_key", "CAST(value as String) AS raw_value",
                                                    "headers as kafka_headers",
                                                    " topic AS kafka_topic", "partition AS kafka_partition",
                                                    "offset AS kafka_offset", "timestamp AS kafka_timestamp"))

    entity_stream_parsed = (entity_stream_parsed
     .withColumn("is_tombstone", F.when(F.col("raw_value").isNull(), True).otherwise(False))
     .withColumn("ingested_at", F.current_timestamp())
     )

    entity_stream_parsed = entity_stream_parsed.withColumn("ingested_date", F.to_date("ingested_at"))

    return entity_stream_parsed


def entity_cdc_write_stream(entity_name: str, entity_data_bronze: DataFrame, entity_data_path: str,
                            entity_checkpoint_path: str) -> None:
    """Write entity CDC events to the bronze layer.
    Args:
        entity_name: Name of entity event.
        entity_data_bronze: Streaming DataFrame containing entity CDC events.
        entity_data_path: Destination path for bronze entity data.
        entity_checkpoint_path: Checkpoint path for the streaming query.

    Returns:
        None.
    """
    logger.info(f"{entity_name}_bronze_stream_started",
                data_path=entity_data_path,
                checkpoint_path=entity_checkpoint_path,

                )
    try:
        query =(
        entity_data_bronze.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", entity_checkpoint_path)
        .partitionBy("ingested_date")
        .trigger(availableNow=True)
        .start(entity_data_path)
        )

        query.awaitTermination()
        logger.info(
            f"{entity_name}_bronze_stream_completed",
            data_path=entity_data_path,
            checkpoint_path=entity_checkpoint_path,

        )
    except Exception:
        logger.exception(
            f"{entity_name}_bronze_stream_failed",
            data_path=entity_data_path,
            checkpoint_path=entity_checkpoint_path,
        )
        raise








