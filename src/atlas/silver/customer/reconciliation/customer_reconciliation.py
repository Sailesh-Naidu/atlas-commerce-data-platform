
import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType

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


def get_customer_snapshot_schema() -> StructType:
    """
    Return the explicit schema for the authoritative customer snapshot.

    Returns:
        StructType: Customer snapshot schema used during file ingestion.
    """
    return  StructType([
        StructField("customer_id", LongType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("status", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])

def get_normalized_reconciliation_columns() -> list[Column]:
    """
    Build normalized customer expressions used for deterministic row checksums.

    Returns:
        list[Column]: Ordered Spark expressions representing the normalized
            customer state used during reconciliation.
    """
    return [
    F.coalesce(F.col("customer_id").cast("string"), F.lit("__NULL__")),
    F.coalesce(F.trim(F.col("first_name")),F.lit("__NULL__")),
    F.coalesce(F.trim(F.col("last_name")),F.lit("__NULL__")),
    F.coalesce(F.lower(F.trim(F.col("email"))),F.lit("__NULL__")),
    F.coalesce(F.trim(F.col("phone_number")),F.lit("__NULL__")),
    F.coalesce(F.date_format(F.col("date_of_birth"), "yyyy-MM-dd"),F.lit("__NULL__")),
    F.coalesce(F.upper(F.trim(F.col("status"))),F.lit("__NULL__")),
    F.coalesce(F.upper(F.trim(F.col("segment"))),F.lit("__NULL__")),
    F.coalesce(F.date_format(F.col("updated_at"),"yyyy-MM-dd HH:mm:ss.SSSSSS"),F.lit("__NULL__"))]

def snapshot_valid_conditions(snapshot_as_of: str)-> Column:
    """
    Build customer-specific snapshot validation conditions.

    Args:
        snapshot_as_of: Point-in-time represented by the snapshot.

    Returns:
        Column: Array-valued Spark expression containing customer validation
            failure codes.
    """
    return (F.array(
        F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
        F.when((F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")), F.lit("MISSING_FIRST_NAME")),
        F.when((F.col("last_name").isNull() | (F.trim(F.col("last_name")) == "")), F.lit("MISSING_LAST_NAME")),
        F.when((F.col("email").isNull()| (F.trim(F.col("email")) == ""))&
            (F.col("phone_number").isNull()| (F.trim(F.col("phone_number")) == "")),
            F.lit("MISSING_CONTACT_INFO")),
        F.when(F.col("date_of_birth") > F.to_date(F.lit(snapshot_as_of)), F.lit("FUTURE_DATE_OF_BIRTH")),
        F.when(F.col("status").isNull() | ~F.col("status").isin(["ACTIVE", "INACTIVE", "SUSPENDED"]), F.lit("INVALID_STATUS")),
        F.when(F.col("segment").isNull() |~F.col("segment").isin(["STANDARD", "GOLD", "PREMIUM"]), F.lit("INVALID_SEGMENT"))
    ))

def get_snapshot_valid_records(spark: SparkSession, snapshot_path:str, snapshot_as_of:str) -> DataFrame:
    """
    Load and prepare the authoritative customer snapshot for reconciliation.

    Args:
        spark: Active Spark session.
        snapshot_as_of: Point-in-time represented by the snapshot.
        snapshot_path: snapshot location path.

    Returns:
        DataFrame: Valid, deduplicated customer snapshot records ready
            for reconciliation.
    """
    customer_snapshot_schema = get_customer_snapshot_schema()
    snapshot_df = spark.read.option("header", True).schema(customer_snapshot_schema).csv(snapshot_path)
    snapshot_with_metadata = attach_snapshot_metadata(snapshot_df, "customers", snapshot_as_of)
    snapshot_valid_condition = snapshot_valid_conditions(snapshot_as_of)
    return get_snapshot_valid_data(snapshot_with_metadata, snapshot_valid_condition, "customer_id")

def get_reconstructed_cdc_customer_state(settings: AtlasSettings, spark: SparkSession, snapshot_as_of:str) -> DataFrame:
    """
    Reconstruct Customer Silver CDC state as of the snapshot timestamp.

    Args:
        settings: Validated Atlas application settings.
        spark: Active Spark session.
        snapshot_as_of: Point-in-time used for CDC state reconstruction.

    Returns:
        DataFrame: Current non-deleted customer state reconstructed from
            Silver CDC history.
    """
    silver_customer_history_path = get_silver_paths(settings, "customer", "customers", "cdc_history")
    return get_cdc_valid_data(spark, silver_customer_history_path, snapshot_as_of, "customer_id",)


def get_customer_expected_state(settings: AtlasSettings,spark: SparkSession,snapshot_as_of: str,reconciliation_run_id: str) -> DataFrame:
    """
        Bootstrap or incrementally advance the materialized customer expected state.

        Args:
            settings: Validated Atlas application settings.
            spark: Active Spark session.
            snapshot_as_of: Current reconciliation cutoff.
            reconciliation_run_id: Identifier shared with the reconciliation run.

        Returns:
            DataFrame: Customer expected state represented by snapshot_as_of.
        """

    expected_state_path = get_reconciliation_paths(settings,"customer","customers","expected_state",)

    expected_state_metadata_path = get_reconciliation_paths(settings,"customer","customers","expected_state_metadata",)

    if not DeltaTable.isDeltaTable(spark, expected_state_path):
        expected_state_from_history = get_reconstructed_cdc_customer_state(settings,spark,snapshot_as_of,)
        persist_expected_state(spark,expected_state_from_history,expected_state_path,True,"customer_id")
        persist_expected_state_metadata(
            expected_state=expected_state_from_history,
            expected_state_metadata_path=expected_state_metadata_path,
            entity_name="customers",
            state_as_of=snapshot_as_of,
            reconciliation_run_id=reconciliation_run_id,
            is_first_run=True,
        )
        return expected_state_from_history

    latest_state_as_of_df = get_latest_expected_state_as_of(spark,expected_state_metadata_path,)

    silver_customer_history_path = get_silver_paths(settings,"customer","customers","cdc_history",)

    customer_cdc_history = (DeltaTable.forPath(spark, silver_customer_history_path).toDF())

    incremental_cdc_changes = get_incremental_cdc_changes(customer_cdc_history,latest_state_as_of_df,snapshot_as_of,)

    latest_incremental_customer_changes = (get_latest_incremental_cdc_changes(incremental_cdc_changes,"customer_id",snapshot_as_of,))

    merge_result = persist_expected_state(spark,latest_incremental_customer_changes,expected_state_path,False,"customer_id" )

    expected_state = DeltaTable.forPath(spark, expected_state_path).toDF()

    persist_expected_state_metadata(
        expected_state=expected_state,
        expected_state_metadata_path=expected_state_metadata_path,
        entity_name="customers",
        state_as_of=snapshot_as_of,
        reconciliation_run_id=reconciliation_run_id,
        is_first_run=False,
        previous_state_as_of=latest_state_as_of_df,
        merge_result=merge_result,
        incremental_cdc_changes=incremental_cdc_changes,
    )

    return expected_state


def run_customer_reconciliation(snapshot_as_of:str):
    """
    Execute end-to-end reconciliation between the authoritative customer
    snapshot and reconstructed Customer Silver CDC state.

    Returns:
        tuple[DataFrame, DataFrame]: Reconciliation exception detail and
            run-level reconciliation metrics.
    """

    settings, spark = initialize_atlas()
    run_summary_path = get_reconciliation_paths(settings, "customer", "customers", "run_summary", )

    exception_detail_path = get_reconciliation_paths(settings, "customer", "customers", "exception_detail", )

    if not snapshot_as_of:
        raise ValueError("snapshot_as_of is required for customer reconciliation")

    customer_snapshot_path = get_snapshot_paths(settings, "customer", snapshot_as_of)

    reconciliation_run_id = str(uuid.uuid4())
    bucket_count = settings.reconciliation.bucket_count

    customer_reconciliation_columns = ["first_name", "last_name", "email", "phone_number", "date_of_birth", "status",
                                       "segment", "updated_at"]

    snapshot_valid_records = get_snapshot_valid_records(spark, customer_snapshot_path, snapshot_as_of)

    reconstructed_cdc_customer_state = get_customer_expected_state(settings,spark,snapshot_as_of,reconciliation_run_id,)

    normalized_reconciliation_columns = get_normalized_reconciliation_columns()

    final_reconciliation_exceptions = get_reconciliation_records(bucket_count, snapshot_valid_records, reconstructed_cdc_customer_state,
                                                             normalized_reconciliation_columns, "customer_id",
                                                             customer_reconciliation_columns)

    final_reconciliation_exceptions = get_final_reconciliation_metadata(final_reconciliation_exceptions, snapshot_as_of,
                                                                        "customers", reconciliation_run_id)

    reconciliation_run_metrics = get_reconciliation_run_metrics(snapshot_valid_records, reconstructed_cdc_customer_state,
                                                                final_reconciliation_exceptions,reconciliation_run_id,
                                                                snapshot_as_of, "customers")





    persist_reconciliation_results(reconciliation_run_metrics,final_reconciliation_exceptions,
                                   run_summary_path,exception_detail_path,)


if __name__ == "__main__":
    run_customer_reconciliation(snapshot_as_of="2026-09-14 03:51:00")

