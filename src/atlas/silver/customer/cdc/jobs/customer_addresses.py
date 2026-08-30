from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from atlas.common.paths.get_cdc_paths import get_bronze_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.cdc.jobs.customer_cdc_common import build_debezium_schema, select_cdc_record


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
        StructField("state", StringType(), False),
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
        F.col("kafka_partition").alias("kafka_partition"),
        F.col("kafka_offset").alias("kafka_offset"),
        F.col("kafka_timestamp").alias("kafka_timestamp"),
        F.col("is_tombstone").alias("is_tombstone"),
        F.col("ingested_at").alias("ingested_at"),
    )


def run_customer_address_silver() -> None:
    """Run the Customer address Bronze-to-Silver S1 transformation.

    Initializes Atlas, reads Customer address Bronze data, parses the raw Debezium
    payload using the explicit Customer address schema, selects the effective CDC
    record, and normalizes it into the canonical Customer address representation.
    """
    settings, spark = initialize_atlas()
    bronze_customer_address_path, _ = get_bronze_paths(settings, "customer",
                                                       "customer_addresses")

    customer_address_bronze_data = spark.read.format("parquet").load(bronze_customer_address_path)

    customer_address_record_schema = build_customer_address_schema()

    customer_debezium_schema = build_debezium_schema(customer_address_record_schema)

    customer_address_parsed_data = customer_address_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))


    customer_addresses_cdc_record = select_cdc_record(customer_address_parsed_data, "customer_addresses")

    _ = normalize_customer_address(customer_addresses_cdc_record)
    _.show()

if __name__ == "__main__":
    run_customer_address_silver()