from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from atlas.common.paths.get_cdc_paths import get_bronze_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.cdc.jobs.customer_cdc_common import build_debezium_schema, select_cdc_record


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


def normalize_customer(customer_after_before: DataFrame) -> DataFrame:
    """Normalize a parsed Customer CDC record into the canonical Customer shape.

    Flattens the selected Customer struct, converts source-specific date and
    timestamp representations to Spark types, and retains CDC, Kafka, and
    ingestion metadata required by downstream Silver processing.

    Args:
        customer_after_before: DataFrame containing the effective Customer CDC
            record selected from the Debezium before or after struct.

    Returns:
        DataFrame containing normalized Customer fields and CDC metadata.
    """
    return customer_after_before.select(
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
        F.col("kafka_partition").alias("kafka_partition"),
        F.col("kafka_offset").alias("kafka_offset"),
        F.col("kafka_timestamp").alias("kafka_timestamp"),
        F.col("is_tombstone").alias("is_tombstone"),
        F.col("ingested_at").alias("ingested_at"),
    )


def run_customer_silver() -> None:
    """Run the Customer Bronze-to-Silver S1 transformation.

    Initializes Atlas, reads Customer Bronze data, parses the raw Debezium
    payload using the explicit Customer schema, selects the effective CDC
    record, and normalizes it into the canonical Customer representation.
    """
    settings, spark = initialize_atlas()
    bronze_customer_path, _ = get_bronze_paths(settings, "customer", "customers")

    customer_bronze_data = spark.read.format("parquet").load(bronze_customer_path)

    customer_record_schema = build_customer_schema()

    customer_debezium_schema = build_debezium_schema(customer_record_schema)

    customer_parsed_data = customer_bronze_data.withColumn("debezium",
                                                           F.from_json(F.col("raw_value"), customer_debezium_schema))


    customer_after_before = select_cdc_record( customer_parsed_data, "customer")

    _ = normalize_customer(customer_after_before)

if __name__ == "__main__":
    run_customer_silver()
