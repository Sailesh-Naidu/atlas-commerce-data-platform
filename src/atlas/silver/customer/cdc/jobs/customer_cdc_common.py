from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def build_debezium_schema(entity_schema: StructType) -> StructType:
    """Build the Debezium envelope schema for a CDC entity.

    Args:
        entity_schema: Schema describing the source representation of the entity.

    Returns:
        Complete Debezium schema containing the payload, before/after records,
        source metadata, operation, and event timestamps.
    """
    debezium_source_schema = StructType([
        StructField("version", StringType(), False),
        StructField("connector", StringType(), False),
        StructField("name", StringType(), False),
        StructField("ts_ms", LongType(), False),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), False),
        StructField("sequence", StringType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("schema", StringType(), False),
        StructField("table", StringType(), False),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True),
    ])

    debezium_payload_schema = StructType([
        StructField("before", entity_schema, True),
        StructField("after", entity_schema, True),
        StructField("source", debezium_source_schema, False),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), False),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
    ])

    return StructType([
        StructField("payload", debezium_payload_schema, True),
    ])

def select_cdc_record(parsed_data: DataFrame, entity_name: str) -> DataFrame:
    """Select the effective entity record from a parsed Debezium CDC event.
        Uses the before record for delete events and the after record for all
        other CDC operations.

        Args:
            parsed_data: DataFrame containing the parsed Debezium payload.
            entity_name: Name of the column that will contain the selected entity struct.

        Returns:
            DataFrame with the effective CDC entity record added as a struct column.
        """
    return parsed_data.withColumn(entity_name,
                                  F.when(
                                        F.col("debezium.payload.op") == "d",
                                        F.col("debezium.payload.before"),

                                    ).otherwise(
                                        F.col("debezium.payload.after")
                                    )
                                  )

def split_cdc_events(entity_valid_data: DataFrame, entity_key: str) -> tuple[DataFrame, DataFrame]:
    """Split valid CDC events into orderable and ambiguous event sets.

        Deduplicates events by Kafka record identity and detects same-run ordering
        ambiguity when the same entity and source LSN appear across multiple Kafka
        partitions.

        Args:
            entity_valid_data: Valid normalized CDC events containing Kafka metadata
                and source ordering information.
            entity_key: Business key column used to identify the CDC entity.

        Returns:
            Tuple containing orderable CDC events and ambiguous CDC events. Ambiguous
            events include rejection metadata describing the ordering conflict.
        """
    entity_deduplicated_data = entity_valid_data.drop_duplicates(["kafka_topic", "kafka_partition", "kafka_offset"])

    entity_incoming_ambiguous_keys = (entity_deduplicated_data.groupBy([entity_key, 'source_lsn'])
                                        .agg(F.countDistinct("kafka_partition").alias("distinct_partition")).filter(
        F.col("distinct_partition") >= 2))

    entity_incoming_ambiguous_events = entity_deduplicated_data.join(entity_incoming_ambiguous_keys,
                                                                         [entity_key, "source_lsn"], "left_semi")
    entity_incoming_ambiguous_events = (
        entity_incoming_ambiguous_events
        .withColumn("cdc_status", F.lit("AMBIGUOUS_ORDERING"))
        .withColumn("persisted_source_lsn", F.lit(None).cast("long"))
        .withColumn("persisted_kafka_partition", F.lit(None).cast("int"))
        .withColumn("persisted_kafka_offset", F.lit(None).cast("long"))
        .withColumn("rejected_at", F.current_timestamp())
    )

    entity_incoming_orderable_data = entity_deduplicated_data.join(
        entity_incoming_ambiguous_keys,
        [entity_key, "source_lsn"],
        "left_anti"
    )

    return entity_incoming_orderable_data, entity_incoming_ambiguous_events

def classify_cdc_against_history(spark: SparkSession, silver_entity_history_path:str, entity_key: str,
                                 entity_incoming_orderable_data: DataFrame,
                                 entity_incoming_ambiguous_events: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Classify incoming CDC events against previously accepted event history.

        Compares each orderable incoming event with the latest accepted event for the
        same entity using source LSN and Kafka metadata. Events that can advance the
        entity state are accepted, while stale or ambiguously ordered events are
        returned as rejected events. Same-run ambiguous events are also included in
        the rejected result.

        Args:
            spark: Active Spark session used to inspect the Delta history table.
            silver_entity_history_path: Delta path containing previously accepted CDC
                event history for the entity.
            entity_key: Business key column used to identify the CDC entity.
            entity_incoming_orderable_data: Incoming CDC events that passed same-run
                ambiguity detection and can be ordered against persisted history.
            entity_incoming_ambiguous_events: Incoming CDC events rejected because
                their ordering is ambiguous within the current run.

        Returns:
            Tuple containing accepted CDC events and rejected CDC events. Accepted
            events are NEW or NEWER relative to persisted history. Rejected events
            contain stale or ambiguous events together with ordering metadata when
            persisted history is available.
        """

    entity_history_exists = DeltaTable.isDeltaTable(spark,silver_entity_history_path)
    if entity_history_exists:

        # Read PREVIOUS accepted history
        entity_cdc_history_table = DeltaTable.forPath(spark,silver_entity_history_path)

        entity_cdc_history_data = entity_cdc_history_table.toDF()

        # Latest accepted event for each entity
        entity_cdc_latest_window = (Window.partitionBy(entity_key)
                                      .orderBy(F.col("source_lsn").desc(),F.col("kafka_offset").desc()))

        entity_cdc_latest = (entity_cdc_history_data
                             .withColumn("row_number",F.row_number().over(entity_cdc_latest_window))
                             .filter(F.col("row_number") == 1).drop("row_number")
        )

        # Compare this run's incoming events against PREVIOUS history
        entity_cdc_comparison = (entity_incoming_orderable_data.alias("s")
                                 .join(entity_cdc_latest.alias("t"),
                                       F.col(f"s.{entity_key}") == F.col(f"t.{entity_key}"), "left")
        )

        # Classify source ordering
        entity_cdc_classified = entity_cdc_comparison.withColumn(
            "cdc_status",
            F.when(F.col(f"t.{entity_key}").isNull(),F.lit("NEW"))
            .when(F.col("s.source_lsn") > F.col("t.source_lsn"),F.lit("NEWER"))
            .when(F.col("s.source_lsn") < F.col("t.source_lsn"),F.lit("STALE"))
            .when(F.col("s.kafka_partition") != F.col("t.kafka_partition"),F.lit("AMBIGUOUS_ORDERING"))
            .when(F.col("s.kafka_offset") > F.col("t.kafka_offset"),F.lit("NEWER"))
            .otherwise(F.lit("STALE")))

        # Only accepted incoming events
        entity_cdc_accepted_events = (
            entity_cdc_classified.filter(F.col("cdc_status").isin("NEW", "NEWER")).select("s.*"))

        # Keep these separately for later monitoring/quarantine
        entity_cdc_rejected_events = (entity_cdc_classified
                                      .filter(F.col("cdc_status").isin("STALE","AMBIGUOUS_ORDERING"))
                                      .select("s.*","cdc_status",
                                              F.col("t.source_lsn").alias("persisted_source_lsn"),
                                              F.col("t.kafka_partition").alias("persisted_kafka_partition"),
                                              F.col("t.kafka_offset").alias("persisted_kafka_offset"))
                                      .withColumn("rejected_at",F.current_timestamp()))
        entity_cdc_rejected_events = entity_cdc_rejected_events.unionByName(entity_incoming_ambiguous_events)

    else:
        # FIRST RUN:There is no previous history to compare against.
        entity_cdc_accepted_events = entity_incoming_orderable_data
        entity_cdc_rejected_events = entity_incoming_ambiguous_events

    return entity_cdc_accepted_events, entity_cdc_rejected_events

def merge_cdc_events(spark: SparkSession,data: DataFrame,target_path: str,) -> None:
    """Persist CDC events idempotently using Kafka record identity.
    Args:
        spark:  Active Spark session used to inspect the Delta history table.
        data: Data that needs to be persisted.
        target_path: Target path for the Delta history table.

    Returns:
        None
    """

    if not DeltaTable.isDeltaTable(spark, target_path):
        data.write.format("delta").save(target_path)
        return

    target_table = DeltaTable.forPath(spark,target_path)

    event_identity_condition = (
        (F.col("t.kafka_topic") == F.col("s.kafka_topic"))
        & (F.col("t.kafka_partition") == F.col("s.kafka_partition"))
        & (F.col("t.kafka_offset") == F.col("s.kafka_offset"))
    )

    (target_table.alias("t").merge(data.alias("s"),event_identity_condition)
     .whenNotMatchedInsertAll().execute())


