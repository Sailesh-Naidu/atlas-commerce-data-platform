import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from atlas.common.config.models import AtlasSettings
from atlas.common.paths.get_cdc_paths import get_reconciliation_paths, get_silver_paths, get_snapshot_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas
from atlas.silver.customer.reconciliation.reconciliation_common import (
    attach_snapshot_metadata,
    get_cdc_valid_data,
    get_final_reconciliation_metadata,
    get_incremental_cdc_changes,
    get_latest_expected_state_as_of,
    get_latest_incremental_cdc_changes,
    get_reconciliation_records,
    get_reconciliation_run_metrics,
    get_snapshot_valid_data,
    persist_expected_state,
    persist_expected_state_metadata,
    persist_reconciliation_results,
)


def get_customer_address_snapshot_schema() -> StructType:
    """
    Return the explicit schema for the authoritative customer address snapshot.

    Returns:
        StructType: Customer address snapshot schema used during file ingestion.
    """
    return StructType([
        StructField("address_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("address_type", StringType(), False),
        StructField("address_line_1", StringType(), False),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), False),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), False),
        StructField("country", StringType(), False),
        StructField("is_primary", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])


def get_normalized_address_reconciliation_columns() -> list[Column]:
    """
    Build normalized customer address expressions used for deterministic row checksums.

    Returns:
        list[Column]: Ordered Spark expressions representing the normalized
            customer address state used during reconciliation.
    """
    return [
        F.coalesce(F.col("address_id").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.col("customer_id").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.upper(F.trim(F.col("address_type"))), F.lit("__NULL__")),
        F.coalesce(F.trim(F.col("address_line_1")), F.lit("__NULL__")),
        F.coalesce(F.trim(F.col("address_line_2")), F.lit("__NULL__")),
        F.coalesce(F.trim(F.col("city")), F.lit("__NULL__")),
        F.coalesce(F.trim(F.col("state")), F.lit("__NULL__")),
        F.coalesce(F.trim(F.col("postal_code")), F.lit("__NULL__")),
        F.coalesce(F.upper(F.trim(F.col("country"))), F.lit("__NULL__")),
        F.coalesce(F.col("is_primary").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.date_format(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS"), F.lit("__NULL__")),
    ]


def address_snapshot_valid_conditions() -> Column:
    """
    Build customer address-specific snapshot validation conditions.

    Returns:
        Column: Array-valued Spark expression containing customer address
            validation failure codes.
    """
    return F.array(
        F.when(F.col("address_id").isNull(), F.lit("MISSING_ADDRESS_ID")),
        F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
        F.when(F.col("address_type").isNull() | ~F.upper(F.trim(F.col("address_type"))).isin(["HOME", "SHIPPING", "BILLING"]),
               F.lit("INVALID_ADDRESS_TYPE")),
        F.when(F.col("address_line_1").isNull() | (F.trim(F.col("address_line_1")) == ""), F.lit("MISSING_ADDRESS_LINE_1")),
        F.when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), F.lit("MISSING_CITY")),
        F.when(F.col("postal_code").isNull() | (F.trim(F.col("postal_code")) == ""), F.lit("MISSING_POSTAL_CODE")),
        F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), F.lit("MISSING_COUNTRY")),
        F.when(F.col("is_primary").isNull(), F.lit("MISSING_IS_PRIMARY")),
    )


def get_address_snapshot_valid_records(spark: SparkSession, snapshot_path: str, snapshot_as_of: str) -> DataFrame:
    """
    Load and prepare the authoritative customer address snapshot for reconciliation.

    Args:
        spark: Active Spark session.
        snapshot_path: Customer address snapshot location.
        snapshot_as_of: Point-in-time represented by the snapshot.

    Returns:
        DataFrame: Valid, deduplicated customer address snapshot records ready
            for reconciliation.
    """
    customer_address_snapshot_schema = get_customer_address_snapshot_schema()
    snapshot_df = spark.read.option("header", True).schema(customer_address_snapshot_schema).csv(snapshot_path)
    snapshot_df = snapshot_df.withColumn(
        "is_primary",
        F.when(F.lower(F.trim(F.col("is_primary"))).isin("t", "true", "1"), F.lit(True))
        .when(F.lower(F.trim(F.col("is_primary"))).isin("f", "false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),

    )

    snapshot_with_metadata = attach_snapshot_metadata(snapshot_df, "customer_addresses", snapshot_as_of)
    snapshot_valid_condition = address_snapshot_valid_conditions()


    return get_snapshot_valid_data(snapshot_with_metadata, snapshot_valid_condition, "address_id")


def get_reconstructed_cdc_address_state(settings: AtlasSettings, spark: SparkSession, snapshot_as_of: str) -> DataFrame:
    """
    Reconstruct Customer Address Silver CDC state as of the snapshot timestamp.

    Args:
        settings: Validated Atlas application settings.
        spark: Active Spark session.
        snapshot_as_of: Point-in-time used for CDC state reconstruction.

    Returns:
        DataFrame: Current non-deleted customer address state reconstructed
            from Silver CDC history.
    """
    silver_address_history_path = get_silver_paths(settings, "customer", "customer_addresses", "cdc_history")

    return get_cdc_valid_data(spark, silver_address_history_path, snapshot_as_of, "address_id")


def get_customer_address_expected_state(settings: AtlasSettings,spark: SparkSession,snapshot_as_of: str,
                                        reconciliation_run_id: str,) -> DataFrame:
    """
    Bootstrap or incrementally advance the materialized customer address expected state.

    Args:
        settings: Validated Atlas application settings.
        spark: Active Spark session.
        snapshot_as_of: Current reconciliation cutoff.
        reconciliation_run_id: Identifier shared with the reconciliation run.

    Returns:
        DataFrame: Customer address expected state represented by snapshot_as_of.
    """
    expected_state_path = get_reconciliation_paths(settings, "customer", "customer_addresses", "expected_state")
    expected_state_metadata_path = get_reconciliation_paths(settings, "customer", "customer_addresses", "expected_state_metadata")

    if not DeltaTable.isDeltaTable(spark, expected_state_path):
        expected_state_from_history = get_reconstructed_cdc_address_state(settings, spark, snapshot_as_of)

        persist_expected_state(spark, expected_state_from_history, expected_state_path, True, "address_id")

        persist_expected_state_metadata(
            expected_state=expected_state_from_history,
            expected_state_metadata_path=expected_state_metadata_path,
            entity_name="customer_addresses",
            state_as_of=snapshot_as_of,
            reconciliation_run_id=reconciliation_run_id,
            is_first_run=True,
        )

        return expected_state_from_history

    latest_state_as_of_df = get_latest_expected_state_as_of(spark, expected_state_metadata_path)

    silver_address_history_path = get_silver_paths(settings, "customer", "customer_addresses", "cdc_history")
    customer_address_cdc_history = DeltaTable.forPath(spark, silver_address_history_path).toDF()

    incremental_cdc_changes = get_incremental_cdc_changes(customer_address_cdc_history, latest_state_as_of_df, snapshot_as_of)

    latest_incremental_address_changes = get_latest_incremental_cdc_changes(incremental_cdc_changes, "address_id", snapshot_as_of)

    merge_result = persist_expected_state(spark, latest_incremental_address_changes, expected_state_path, False, "address_id")

    expected_state = DeltaTable.forPath(spark, expected_state_path).toDF()

    persist_expected_state_metadata(
        expected_state=expected_state,
        expected_state_metadata_path=expected_state_metadata_path,
        entity_name="customer_addresses",
        state_as_of=snapshot_as_of,
        reconciliation_run_id=reconciliation_run_id,
        is_first_run=False,
        previous_state_as_of=latest_state_as_of_df,
        merge_result=merge_result,
        incremental_cdc_changes=incremental_cdc_changes,
    )

    return expected_state


def run_customer_address_reconciliation(snapshot_as_of: str) -> None:
    """
    Execute end-to-end reconciliation between the authoritative customer address
    snapshot and reconstructed Customer Address Silver CDC state.
    """
    settings, spark = initialize_atlas()

    run_summary_path = get_reconciliation_paths(settings, "customer", "customer_addresses", "run_summary")
    exception_detail_path = get_reconciliation_paths(settings, "customer", "customer_addresses", "exception_detail")

    if not snapshot_as_of:
        raise ValueError("snapshot_as_of is required for customer address reconciliation")

    customer_address_snapshot_path = get_snapshot_paths(settings, "customer_addresses", snapshot_as_of)

    reconciliation_run_id = str(uuid.uuid4())
    bucket_count = settings.reconciliation.bucket_count

    customer_address_reconciliation_columns = ["customer_id","address_type","address_line_1","address_line_2","city","state",
        "postal_code","country","is_primary","updated_at",]

    snapshot_valid_records = get_address_snapshot_valid_records(spark, customer_address_snapshot_path, snapshot_as_of)

    reconstructed_cdc_address_state = get_customer_address_expected_state(settings, spark, snapshot_as_of, reconciliation_run_id)

    normalized_reconciliation_columns = get_normalized_address_reconciliation_columns()

    final_reconciliation_exceptions = get_reconciliation_records(bucket_count,snapshot_valid_records,reconstructed_cdc_address_state,
                                                                 normalized_reconciliation_columns,"address_id",
                                                                 customer_address_reconciliation_columns,)

    final_reconciliation_exceptions = get_final_reconciliation_metadata(final_reconciliation_exceptions,snapshot_as_of,
                                                                        "customer_addresses",reconciliation_run_id,)

    reconciliation_run_metrics = get_reconciliation_run_metrics(snapshot_valid_records,reconstructed_cdc_address_state,
                                                                final_reconciliation_exceptions,reconciliation_run_id,snapshot_as_of,
                                                                "customer_addresses",)

    persist_reconciliation_results(reconciliation_run_metrics,final_reconciliation_exceptions,
                                   run_summary_path,exception_detail_path,)


if __name__ == "__main__":
    run_customer_address_reconciliation(snapshot_as_of="2026-09-14 05:55:00")