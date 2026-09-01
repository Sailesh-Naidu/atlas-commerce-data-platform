from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from atlas.common.paths.get_cdc_paths import get_bronze_paths, get_silver_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.cdc.jobs.customer_cdc_common import (
    build_debezium_schema,
    classify_cdc_against_history,
    merge_cdc_events,
    select_cdc_record,
    split_cdc_events,
)


def build_customer_address_schema() -> StructType:
    """Build the source schema for Customer address records in Debezium CDC events.

    Returns:
        Spark StructType describing the Customer address record as represented in the
        Debezium payload before canonical type normalization.
    """
    customer_address_record_schema = StructType([
        StructField("address_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("address_type", StringType(), False),
        StructField("address_line_1", StringType(), False),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), False),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), False),
        StructField("country", StringType(), False),
        StructField("is_primary", BooleanType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
    ])
    return customer_address_record_schema

def normalize_customer_address(customer_address_cdc_record: DataFrame) -> DataFrame:
    """Normalize a parsed Customer address CDC record into the canonical Customer address shape.

    Flattens the selected Customer address struct, converts source-specific date and
    timestamp representations to Spark types, and retains CDC, Kafka, and
    ingestion metadata required by downstream Silver processing.

    Args:
        customer_address_cdc_record: DataFrame containing the effective Customer address CDC
            record selected from the Debezium before or after struct.

    Returns:
        DataFrame containing normalized Customer address fields and CDC metadata.
    """
    return  customer_address_cdc_record.select(
        F.col("customer_addresses.address_id").alias("address_id"),
        F.col("customer_addresses.customer_id").alias("customer_id"),
        F.col("customer_addresses.address_type").alias("address_type"),
        F.col("customer_addresses.address_line_1").alias("address_line_1"),
        F.col("customer_addresses.address_line_2").alias("address_line_2"),
        F.col("customer_addresses.city").alias("city"),
        F.col("customer_addresses.state").alias("state"),
        F.col("customer_addresses.postal_code").alias("postal_code"),
        F.col("customer_addresses.country").alias("country"),
        F.col("customer_addresses.is_primary").alias("is_primary"),
        F.try_to_timestamp(F.col("customer_addresses.created_at")).alias("created_at"),
        F.try_to_timestamp(F.col("customer_addresses.updated_at")).alias("updated_at"),
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

def apply_customer_address_dq(
    customer_address_data: DataFrame,
) -> DataFrame:
    """Apply row-level Customer address data-quality rules.

    Args:
        customer_address_data: Normalized non-tombstone Customer address CDC records.

    Returns:
        DataFrame with a dq_errors array containing all failed DQ rules per row.
    """
    customer_address_filter_condition = F.array(
        F.when(F.col("address_id").isNull(),F.lit("MISSING_ADDRESS_ID"),),
        F.when(F.col("customer_id").isNull(),F.lit("MISSING_CUSTOMER_ID"),),

        F.when(F.col("address_type").isNull()| ~F.col("address_type").isin(["HOME", "SHIPPING", "BILLING"]),
            F.lit("INVALID_ADDRESS_TYPE"),),

        F.when(F.col("address_line_1").isNull()| (F.trim(F.col("address_line_1")) == ""),
            F.lit("MISSING_ADDRESS_LINE_1"),),
        F.when(F.col("city").isNull()| (F.trim(F.col("city")) == ""),F.lit("MISSING_CITY"),),

        F.when(F.col("postal_code").isNull()| (F.trim(F.col("postal_code")) == ""),
            F.lit("MISSING_POSTAL_CODE"),),

        F.when( F.col("country").isNull()| (F.trim(F.col("country")) == ""),
            F.lit("MISSING_COUNTRY"),),
        F.when(F.col("is_primary").isNull(),F.lit("MISSING_IS_PRIMARY"),),
    )

    return customer_address_data.withColumn("dq_errors",F.array_compact(customer_address_filter_condition),)


def split_customer_address_dq(
    customer_address_error_info: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split evaluated Customer address records into valid and quarantine datasets.

    Args:
        customer_address_error_info: Customer address records containing dq_errors.

    Returns:
        Tuple containing valid Customer address records and quarantined records.
    """
    customer_address_valid_data = (customer_address_error_info.filter(F.size(F.col("dq_errors")) == 0)
        .drop("dq_errors"))

    customer_address_quarantine_data = (customer_address_error_info.filter(F.size(F.col("dq_errors")) > 0)
        .withColumn("dq_error_count", F.size(F.col("dq_errors")),)
        .withColumn("quarantined_at",F.current_timestamp(),
        ))

    return customer_address_valid_data, customer_address_quarantine_data,

def run_customer_address_silver() -> None:
    """Run the Customer address Bronze-to-Silver CDC transformation.

    Initializes Atlas, reads Customer address Bronze data, parses and normalizes the
    Debezium CDC records, excludes Kafka tombstones from business DQ, applies
    Customer address data-quality rules, and separates valid and quarantined records.
    """
    settings, spark = initialize_atlas()
    bronze_customer_address_path, _ = get_bronze_paths(settings, "customer",
                                                       "customer_addresses")

    silver_address_history_path = get_silver_paths(settings, "customer", "customer_addresses", "cdc_history")
    silver_quarantine_data_path = get_silver_paths(settings, "customer", "customer_addresses", "quarantine")
    silver_rejected_data_path = get_silver_paths(settings, "customer", "customer_addresses", "rejected")

    customer_address_bronze_data = spark.read.format("parquet").load(bronze_customer_address_path)

    customer_address_record_schema = build_customer_address_schema()

    customer_debezium_schema = build_debezium_schema(customer_address_record_schema)

    customer_address_parsed_data = customer_address_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))


    customer_addresses_cdc_record = select_cdc_record(customer_address_parsed_data, "customer_addresses")

    customer_address_data = normalize_customer_address(customer_addresses_cdc_record)

    customer_address_non_tombstone_data = customer_address_data.filter(~F.col("is_tombstone"))

    customer_address_error_info = apply_customer_address_dq(customer_address_non_tombstone_data)

    customer_address_valid_data, customer_address_quarantine_data = (split_customer_address_dq
                                                                     (customer_address_error_info))


    address_incoming_orderable_data, address_incoming_ambiguous_events = (split_cdc_events(customer_address_valid_data,
                                                                                            "address_id"))

    address_cdc_accepted_events, address_cdc_rejected_events = classify_cdc_against_history(spark,
                                                                                            silver_address_history_path,
                                                                                             "address_id",
                                                                                              address_incoming_orderable_data,
                                                                                             address_incoming_ambiguous_events)

    # DQ quarantine
    merge_cdc_events(spark,customer_address_quarantine_data,silver_quarantine_data_path,)

    # Accepted CDC history
    merge_cdc_events(spark,address_cdc_accepted_events,silver_address_history_path,)

    # Persist CDC ordering rejections
    if address_cdc_rejected_events is not None:
        merge_cdc_events(spark,address_cdc_rejected_events,silver_rejected_data_path,)

    test_silver_history_data = spark.read.format("delta").load(silver_address_history_path)
    test_silver_quarantine_data = spark.read.format("delta").load(silver_quarantine_data_path)
    test_silver_rejected_data = spark.read.format("delta").load(silver_rejected_data_path)
    test_silver_history_data.show()
    test_silver_quarantine_data.show()
    test_silver_rejected_data.show()

if __name__ == "__main__":
    run_customer_address_silver()