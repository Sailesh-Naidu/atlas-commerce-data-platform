from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from atlas.common.paths.get_cdc_paths import get_bronze_paths, get_silver_checkpoint_path, get_silver_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.cdc.jobs.customer_cdc_common import (
    build_debezium_schema,
    classify_cdc_against_history,
    merge_cdc_events,
    select_cdc_record,
    split_cdc_events,
)


def build_customer_schema() -> StructType:
    """Build the source schema for Customer records in Debezium CDC events.

    Returns:
        Spark StructType describing the Customer record as represented in the
        Debezium payload before canonical type normalization.
    """
    customer_record_schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("date_of_birth", LongType(), True),
        StructField("status", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
    ])
    return customer_record_schema


def normalize_customer(customer_cdc_record: DataFrame) -> DataFrame:
    """Normalize a parsed Customer CDC record into the canonical Customer shape.

    Flattens the selected Customer struct, converts source-specific date and
    timestamp representations to Spark types, and retains CDC, Kafka, and
    ingestion metadata required by downstream Silver processing.

    Args:
        customer_cdc_record: DataFrame containing the effective Customer CDC
            record selected from the Debezium before or after struct.

    Returns:
        DataFrame containing normalized Customer fields and CDC metadata.
    """
    return customer_cdc_record.select(
        F.col("customer.customer_id").alias("customer_id"),
        F.col("customer.first_name").alias("first_name"),
        F.col("customer.last_name").alias("last_name"),
        F.col("customer.email").alias("email"),
        F.col("customer.phone_number").alias("phone_number"),
        F.date_add(
            F.lit("1970-01-01").cast("date"),
            F.col("customer.date_of_birth").cast("int")
        ).alias("date_of_birth"),
        F.col("customer.status").alias("status"),
        F.col("customer.segment").alias("segment"),
        F.try_to_timestamp(F.col("customer.created_at")).alias("created_at"),
        F.try_to_timestamp(F.col("customer.updated_at")).alias("updated_at"),
        F.timestamp_millis(F.col("debezium.payload.ts_ms")).alias("cdc_timestamp"),
        F.timestamp_millis(F.col("debezium.payload.source.ts_ms")).alias("source_timestamp"),
        F.col("debezium.payload.op").alias("cdc_operation"),
        F.col("debezium.payload.source.lsn").alias("source_lsn"),
        F.col("kafka_topic").alias("kafka_topic"),
        F.col("kafka_partition").alias("kafka_partition"),
        F.col("kafka_offset").alias("kafka_offset"),
        F.col("kafka_timestamp").alias("kafka_timestamp"),
        F.col("is_tombstone").alias("is_tombstone"),
        F.col("ingested_at").alias("ingested_at"),
    )

def apply_customer_dq(customer_data: DataFrame) -> DataFrame:
    """Apply row-level Customer data-quality rules.

    Args:
        customer_data: Normalized non-tombstone Customer CDC records.

    Returns:
        DataFrame with a dq_errors array containing all failed DQ rules per row.
    """
    customer_filter_condition = (
        F.array(
            F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
            F.when((F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")), F.lit("MISSING_FIRST_NAME")),
            F.when((F.col("last_name").isNull() | (F.trim(F.col("last_name")) == "")), F.lit("MISSING_LAST_NAME")),
            F.when((F.col("email").isNull() & F.col("phone_number").isNull()), F.lit("MISSING_CONTACT_INFO")),
            F.when(F.col("date_of_birth") > F.current_date(), F.lit("FUTURE_DATE_OF_BIRTH")),
            F.when(~F.col("status").isin(["ACTIVE", "INACTIVE", "SUSPENDED"]), F.lit("INVALID_STATUS")),
            F.when(~F.col("segment").isin(["STANDARD", "GOLD", "PREMIUM"]), F.lit("INVALID_SEGMENT"))
        ))

    return customer_data.withColumn("dq_errors", F.array_compact(customer_filter_condition))

def split_customer_dq(customer_contain_error_info: DataFrame,) -> tuple[DataFrame, DataFrame]:
    """Split evaluated Customer records into valid and quarantine datasets.

    Args:
        customer_contain_error_info: Customer records containing the dq_errors array.

    Returns:
        Tuple containing valid Customer records and quarantined Customer records.
    """
    customer_valid_data = customer_contain_error_info.filter(F.size(F.col("dq_errors")) == 0)
    customer_valid_data = customer_valid_data.drop("dq_errors")

    customer_quarantine_data = customer_contain_error_info.filter(F.size(F.col("dq_errors")) > 0)

    customer_quarantine_data = (customer_quarantine_data.withColumn("dq_error_count", F.size(F.col("dq_errors")))
                                .withColumn("quarantined_at", F.current_timestamp()))

    return customer_valid_data, customer_quarantine_data


def process_customer_microbatch(spark,customer_bronze_data: DataFrame, batch_id: int,
                                customer_debezium_schema: StructType, silver_customer_history_path: str,
                                silver_quarantine_data_path: str, silver_rejected_data_path: str)-> None:


    print(f"Processing Customer Silver micro-batch: {batch_id}")

    customer_parsed_data = customer_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))

    customer_cdc_record = select_cdc_record(customer_parsed_data, "customer")

    customer_data = normalize_customer(customer_cdc_record)

    customer_non_tombstone_data = customer_data.filter(~F.col("is_tombstone"))
    customer_contain_error_info = apply_customer_dq(customer_non_tombstone_data)

    customer_valid_data, customer_quarantine_data = split_customer_dq(customer_contain_error_info)

    customer_incoming_orderable_data, customer_incoming_ambiguous_events = split_cdc_events(customer_valid_data,
                                                                                            "customer_id")

    customer_cdc_accepted_events, customer_cdc_rejected_events = classify_cdc_against_history(spark,
                                                                                              silver_customer_history_path,
                                                                                              "customer_id",
                                                                                              customer_incoming_orderable_data,
                                                                                              customer_incoming_ambiguous_events)

    # DQ quarantine
    merge_cdc_events(spark, customer_quarantine_data, silver_quarantine_data_path, )

    # Accepted CDC history
    merge_cdc_events(spark, customer_cdc_accepted_events, silver_customer_history_path, )

    # Persist CDC ordering rejections
    if customer_cdc_rejected_events is not None:
        merge_cdc_events(spark, customer_cdc_rejected_events, silver_rejected_data_path, )

def process_batch(customer_microbatch: DataFrame, batch_id: int) -> None:
    settings, spark = initialize_atlas()
    silver_customer_history_path = get_silver_paths(settings, "customer", "customers", "cdc_history")
    silver_quarantine_data_path = get_silver_paths(settings, "customer", "customers", "quarantine")
    silver_rejected_data_path = get_silver_paths(settings, "customer", "customers", "rejected")
    customer_record_schema = build_customer_schema()
    customer_debezium_schema = build_debezium_schema(customer_record_schema)

    process_customer_microbatch(spark, customer_microbatch,batch_id,customer_debezium_schema,
                                silver_customer_history_path,silver_quarantine_data_path,silver_rejected_data_path)

def run_customer_silver() -> None:
    """Run the Customer Bronze-to-Silver CDC transformation.

    Initializes Atlas, reads Customer Bronze data, parses and normalizes the
    Debezium CDC records, excludes Kafka tombstones from business DQ, applies
    Customer data-quality rules, and separates valid and quarantined records.
    """
    settings, spark = initialize_atlas()
    bronze_customer_path, _ = get_bronze_paths(settings, "customer", "customers")
    silver_customer_checkpoint_path = get_silver_checkpoint_path(settings,"customer", "customers")
    customer_bronze_schema = spark.read.format("parquet").load(bronze_customer_path).schema

    customer_bronze_data = spark.readStream.format("parquet").schema(customer_bronze_schema).load(bronze_customer_path)


    customer_silver_query = (customer_bronze_data.writeStream
                             .foreachBatch(process_batch)
                             .option("checkpointLocation",silver_customer_checkpoint_path,)
                             .trigger(availableNow=True).start())

    customer_silver_query.awaitTermination()
    silver_customer_history_path = get_silver_paths(settings, "customer", "customers", "cdc_history")
    silver_rejected_data_path = get_silver_paths(settings, "customer", "customers", "rejected")
    test_history = spark.read.format("delta").load(silver_customer_history_path)
    test_rejected = spark.read.format("delta").load(silver_rejected_data_path)
    test_history.show()
    test_rejected.show()

if __name__ == "__main__":
    run_customer_silver()
