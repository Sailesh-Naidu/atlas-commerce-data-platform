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


def build_customer_consent_schema() -> StructType:
    """Build the source schema for Customer consent records in Debezium CDC events.

    Returns:
        Spark StructType describing the Customer consent record as represented in the
        Debezium payload before canonical type normalization.
    """
    customer_consent_record_schema = StructType([
        StructField("consent_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("consent_type", StringType(), False),
        StructField("granted", BooleanType(), False),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
    ])
    return customer_consent_record_schema

def normalize_customer_consent(customer_consent_cdc_record: DataFrame) -> DataFrame:
    """Normalize a parsed Customer consent CDC record into the canonical Customer consent shape.

    Flattens the selected Customer consent struct, converts source-specific date and
    timestamp representations to Spark types, and retains CDC, Kafka, and
    ingestion metadata required by downstream Silver processing.

    Args:
        customer_consent_cdc_record: DataFrame containing the effective Customer consent CDC
            record selected from the Debezium before or after struct.

    Returns:
        DataFrame containing normalized Customer consent fields and CDC metadata.
    """
    return  customer_consent_cdc_record.select(
        F.col("customer_consent.consent_id").alias("consent_id"),
        F.col("customer_consent.customer_id").alias("customer_id"),
        F.col("customer_consent.consent_type").alias("consent_type"),
        F.col("customer_consent.granted").alias("granted"),
        F.try_to_timestamp(F.col("customer_consent.created_at")).alias("created_at"),
        F.try_to_timestamp(F.col("customer_consent.updated_at")).alias("updated_at"),
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

def apply_customer_consent_dq(
    customer_consent_data: DataFrame,
) -> DataFrame:
    """Apply row-level Customer consent data-quality rules.

    Args:
        customer_consent_data: Normalized non-tombstone Customer consent CDC records.

    Returns:
        DataFrame with a dq_errors array containing all failed DQ rules per row.
    """
    customer_consent_filter_condition = F.array(
        F.when(F.col("consent_id").isNull(),F.lit("MISSING_CONSENT_ID"),),
        F.when(F.col("customer_id").isNull(),F.lit("MISSING_CUSTOMER_ID"),),
        F.when(F.col("consent_type").isNull()| (F.trim(F.col("consent_type")) == ""),F.lit("MISSING_CONSENT_TYPE"),),
        F.when(F.col("granted").isNull(),F.lit("MISSING_GRANTED"),),)

    return customer_consent_data.withColumn("dq_errors",F.array_compact(customer_consent_filter_condition),)


def split_customer_consent_dq(
    customer_consent_error_info: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split evaluated Customer consent records into valid and quarantine datasets.

    Args:
        customer_consent_error_info: Customer consent records containing dq_errors.

    Returns:
        Tuple containing valid Customer consent records and quarantined records.
    """
    customer_consent_valid_data = (customer_consent_error_info.filter(F.size(F.col("dq_errors")) == 0)
                                   .drop("dq_errors"))

    customer_consent_quarantine_data = (customer_consent_error_info.filter(F.size(F.col("dq_errors")) > 0)
        .withColumn("dq_error_count",F.size(F.col("dq_errors")),)
        .withColumn("quarantined_at",F.current_timestamp(),)
    )

    return customer_consent_valid_data, customer_consent_quarantine_data

def run_customer_consent_silver() -> None:
    """Run the Customer consents Bronze-to-Silver CDC transformation.

    Initializes Atlas, reads Customer consents Bronze data, parses and normalizes the
    Debezium CDC records, excludes Kafka tombstones from business DQ, applies
    Customer consents data-quality rules, and separates valid and quarantined records.
    """
    settings, spark = initialize_atlas()
    bronze_customer_consent_path, _ = get_bronze_paths(settings, "customer",
                                                       "customer_consents")

    silver_consent_history_path = get_silver_paths(settings, "customer", "customer_consents", "cdc_history")
    silver_quarantine_data_path = get_silver_paths(settings, "customer", "customer_consents", "quarantine")
    silver_rejected_data_path = get_silver_paths(settings, "customer", "customer_consents", "rejected")

    customer_consent_bronze_data = spark.read.format("parquet").load(bronze_customer_consent_path)

    customer_consent_record_schema = build_customer_consent_schema()

    customer_debezium_schema = build_debezium_schema(customer_consent_record_schema)

    customer_consent_parsed_data = customer_consent_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))


    customer_consent_cdc_record = select_cdc_record(customer_consent_parsed_data, "customer_consent")

    customer_consent_data = normalize_customer_consent(customer_consent_cdc_record)

    customer_consent_non_tombstone_data = customer_consent_data.filter(~F.col("is_tombstone"))

    customer_consent_error_info = apply_customer_consent_dq(customer_consent_non_tombstone_data)

    customer_consent_valid_data, customer_consent_quarantine_data = (split_customer_consent_dq
                                                                     (customer_consent_error_info))

    consent_incoming_orderable_data, consent_incoming_ambiguous_events = split_cdc_events(customer_consent_valid_data,
                                                                                            "consent_id")

    consent_cdc_accepted_events, consent_cdc_rejected_events = classify_cdc_against_history(spark,
                                                                                              silver_consent_history_path,
                                                                                              "consent_id",
                                                                                              consent_incoming_orderable_data,
                                                                                              consent_incoming_ambiguous_events)

    # DQ quarantine
    merge_cdc_events(spark, customer_consent_quarantine_data, silver_quarantine_data_path, )

    # Accepted CDC history
    merge_cdc_events(spark, consent_cdc_accepted_events, silver_consent_history_path, )

    # Persist CDC ordering rejections
    if consent_cdc_rejected_events is not None:
        merge_cdc_events(spark, consent_cdc_rejected_events, silver_rejected_data_path, )

    test_silver_history_data = spark.read.format("delta").load(silver_consent_history_path)
    test_silver_quarantine_data = spark.read.format("delta").load(silver_quarantine_data_path)
    test_silver_rejected_data = spark.read.format("delta").load(silver_rejected_data_path)
    test_silver_history_data.show()
    test_silver_quarantine_data.show()
    test_silver_rejected_data.show()


if __name__ == "__main__":
    run_customer_consent_silver()