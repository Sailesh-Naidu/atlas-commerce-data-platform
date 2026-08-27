from pyspark.sql import functions as F
from pyspark.sql.classic.dataframe import DataFrame
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