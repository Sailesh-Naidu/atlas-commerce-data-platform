from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from atlas.common.paths.get_cdc_paths import get_bronze_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.cdc.jobs.customer_cdc_common import build_debezium_schema, select_cdc_record


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
        StructField("granted", BooleanType(), True),
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
        F.col("kafka_partition").alias("kafka_partition"),
        F.col("kafka_offset").alias("kafka_offset"),
        F.col("kafka_timestamp").alias("kafka_timestamp"),
        F.col("is_tombstone").alias("is_tombstone"),
        F.col("ingested_at").alias("ingested_at"),
    )

def run_customer_consent_silver() -> None:
    """Run the Customer consents Bronze-to-Silver S1 transformation.

    Initializes Atlas, reads Customer consents Bronze data, parses the raw Debezium
    payload using the explicit Customer consents schema, selects the effective CDC
    record, and normalizes it into the canonical Customer consent representation.
    """
    settings, spark = initialize_atlas()
    bronze_customer_consent_path, _ = get_bronze_paths(settings, "customer",
                                                       "customer_consents")

    customer_bronze_data = spark.read.format("parquet").load(bronze_customer_consent_path)

    customer_consent_record_schema = build_customer_consent_schema()

    customer_debezium_schema = build_debezium_schema(customer_consent_record_schema)

    customer_consent_parsed_data = customer_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))


    customer_consent_cdc_record = select_cdc_record(customer_consent_parsed_data, "customer_consent")

    _ = normalize_customer_consent(customer_consent_cdc_record)
    _.show()

if __name__ == "__main__":
    run_customer_consent_silver()