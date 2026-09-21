import time
import uuid

import structlog
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

logger = structlog.get_logger(__name__)

def get_customer_consent_snapshot_schema() -> StructType:
    """
    Return the explicit schema for the authoritative customer consent snapshot.

    Returns:
        StructType: Customer consent snapshot schema used during file ingestion.
    """
    return StructType([
        StructField("consent_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("consent_type", StringType(), False),
        StructField("granted", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])


def normalize_consent_snapshot(snapshot_df: DataFrame) -> DataFrame:
    """
    Normalize source-specific customer consent snapshot values.
    Postgres SQL CSV exports may represent booleans as t/f, so granted is read
    as a string and explicitly converted to a Spark boolean.

    Args:
        snapshot_df: Raw customer consent snapshot DataFrame.

    Returns:
        DataFrame: Snapshot with granted normalized to BooleanType.
    """
    return snapshot_df.withColumn(
        "granted",
        F.when(F.lower(F.trim(F.col("granted"))).isin("t", "true", "1"), F.lit(True))
        .when(F.lower(F.trim(F.col("granted"))).isin("f", "false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )


def get_normalized_consent_reconciliation_columns() -> list[Column]:
    """
    Build normalized customer consent expressions used for deterministic row checksums.

    Returns:
        list[Column]: Ordered Spark expressions representing normalized
            customer consent state used during reconciliation.
    """
    return [
        F.coalesce(F.col("consent_id").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.col("customer_id").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.upper(F.trim(F.col("consent_type"))), F.lit("__NULL__")),
        F.coalesce(F.col("granted").cast("string"), F.lit("__NULL__")),
        F.coalesce(F.date_format(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS"), F.lit("__NULL__")),
    ]


def consent_snapshot_valid_conditions() -> Column:
    """
    Build customer consent-specific snapshot validation conditions.
    Returns:
        Column: Array-valued Spark expression containing customer consent
            validation failure codes.
    """
    return F.array(
        F.when(F.col("consent_id").isNull(), F.lit("MISSING_CONSENT_ID")),
        F.when(F.col("customer_id").isNull(), F.lit("MISSING_CUSTOMER_ID")),
        F.when(
            F.col("consent_type").isNull() | (F.trim(F.col("consent_type")) == ""),
            F.lit("MISSING_CONSENT_TYPE"),
        ),
        F.when(F.col("granted").isNull(), F.lit("MISSING_GRANTED")),
    )


def get_consent_snapshot_valid_records(spark: SparkSession, snapshot_path: str, snapshot_as_of: str) -> DataFrame:
    """
    Load and prepare the authoritative customer consent snapshot for reconciliation.
    Args:
        spark: Active Spark session.
        snapshot_path: Customer consent snapshot location.
        snapshot_as_of: Point-in-time represented by the snapshot.
    Returns:
        DataFrame: Valid, deduplicated customer consent records ready for reconciliation.
    """
    customer_consent_snapshot_schema = get_customer_consent_snapshot_schema()

    snapshot_df = spark.read.option("header", True).schema(customer_consent_snapshot_schema).csv(snapshot_path)
    snapshot_df = normalize_consent_snapshot(snapshot_df)

    snapshot_with_metadata = attach_snapshot_metadata(snapshot_df, "customer_consents", snapshot_as_of)
    snapshot_valid_condition = consent_snapshot_valid_conditions()

    return get_snapshot_valid_data(snapshot_with_metadata, snapshot_valid_condition, "consent_id")


def get_reconstructed_cdc_consent_state(settings: AtlasSettings, spark: SparkSession, snapshot_as_of: str) -> DataFrame:
    """
    Reconstruct Customer Consent Silver CDC state as of the snapshot timestamp.
    Args:
        settings: Validated Atlas application settings.
        spark: Active Spark session.
        snapshot_as_of: Point-in-time used for CDC state reconstruction.
    Returns:
        DataFrame: Current non-deleted Customer consent state reconstructed
            from Silver CDC history.
    """
    silver_consent_history_path = get_silver_paths(settings, "customer", "customer_consents", "cdc_history")

    return get_cdc_valid_data(spark, silver_consent_history_path, snapshot_as_of, "consent_id")


def get_customer_consent_expected_state(settings: AtlasSettings, spark: SparkSession,
                                        snapshot_as_of: str,reconciliation_run_id: str,) -> DataFrame:
    """
    Bootstrap or incrementally advance the materialized Customer consent expected state.

    Args:
        settings: Validated Atlas application settings.
        spark: Active Spark session.
        snapshot_as_of: Current reconciliation cutoff.
        reconciliation_run_id: Identifier shared with the reconciliation run.

    Returns:
        DataFrame: Customer consent expected state represented by snapshot_as_of.
    """
    expected_state_path = get_reconciliation_paths(settings, "customer", "customer_consents", "expected_state")

    expected_state_metadata_path = get_reconciliation_paths(settings,"customer","customer_consents","expected_state_metadata",)

    if not DeltaTable.isDeltaTable(spark, expected_state_path):
        expected_state_from_history = get_reconstructed_cdc_consent_state(settings, spark, snapshot_as_of)

        persist_expected_state(spark,expected_state_from_history, expected_state_path,True,"consent_id",)

        persist_expected_state_metadata(
            expected_state=expected_state_from_history,
            expected_state_metadata_path=expected_state_metadata_path,
            entity_name="customer_consents",
            state_as_of=snapshot_as_of,
            reconciliation_run_id=reconciliation_run_id,
            is_first_run=True,
        )
        return expected_state_from_history

    latest_state_as_of_df = get_latest_expected_state_as_of(spark,expected_state_metadata_path, )

    silver_consent_history_path = get_silver_paths(settings,"customer","customer_consents","cdc_history",)

    customer_consent_cdc_history = DeltaTable.forPath(spark,silver_consent_history_path,).toDF()

    incremental_cdc_changes = get_incremental_cdc_changes(customer_consent_cdc_history,latest_state_as_of_df,snapshot_as_of,)

    latest_incremental_consent_changes = get_latest_incremental_cdc_changes(incremental_cdc_changes,"consent_id",snapshot_as_of,)

    merge_result = persist_expected_state(spark,latest_incremental_consent_changes,expected_state_path,False,"consent_id",)

    expected_state = DeltaTable.forPath(spark, expected_state_path).toDF()

    persist_expected_state_metadata(
        expected_state=expected_state,
        expected_state_metadata_path=expected_state_metadata_path,
        entity_name="customer_consents",
        state_as_of=snapshot_as_of,
        reconciliation_run_id=reconciliation_run_id,
        is_first_run=False,
        previous_state_as_of=latest_state_as_of_df,
        merge_result=merge_result,
        incremental_cdc_changes=incremental_cdc_changes,
    )

    return expected_state


def run_customer_consent_reconciliation(snapshot_as_of: str) -> None:
    """
    Execute end-to-end reconciliation between the authoritative Customer consent
    snapshot and Customer Consent Silver CDC expected state.
    """
    reconciliation_run_id = str(uuid.uuid4())
    start_time = time.perf_counter()

    try:
        settings, spark = initialize_atlas()

        if not snapshot_as_of:
            raise ValueError("snapshot_as_of is required for customer consent reconciliation")

        run_summary_path = get_reconciliation_paths(settings,"customer","customer_consents","run_summary",)

        exception_detail_path = get_reconciliation_paths(settings,"customer","customer_consents","exception_detail",)

        customer_consent_snapshot_path = get_snapshot_paths(settings,"customer_consents",snapshot_as_of,)

        logger.info(
            "reconciliation_started",
            reconciliation_run_id=reconciliation_run_id,
            entity_name="customer_consents",
            snapshot_as_of=snapshot_as_of,
        )

        bucket_count = settings.reconciliation.bucket_count

        customer_consent_reconciliation_columns = ["customer_id","consent_type","granted","updated_at",]

        snapshot_valid_records = get_consent_snapshot_valid_records(spark,customer_consent_snapshot_path,snapshot_as_of,)

        reconstructed_cdc_consent_state = get_customer_consent_expected_state(settings,spark,snapshot_as_of,reconciliation_run_id,)

        normalized_reconciliation_columns = get_normalized_consent_reconciliation_columns()

        final_reconciliation_exceptions = get_reconciliation_records(bucket_count,snapshot_valid_records,reconstructed_cdc_consent_state,
                                                                     normalized_reconciliation_columns,"consent_id",
                                                                     customer_consent_reconciliation_columns,)

        final_reconciliation_exceptions = get_final_reconciliation_metadata(final_reconciliation_exceptions,snapshot_as_of,
                                                                            "customer_consents",reconciliation_run_id,)

        reconciliation_run_metrics = get_reconciliation_run_metrics(snapshot_valid_records,reconstructed_cdc_consent_state,
                                                                    final_reconciliation_exceptions,
                                                                    reconciliation_run_id, snapshot_as_of,"customer_consents",)

        persist_reconciliation_results(reconciliation_run_metrics,final_reconciliation_exceptions,
                                       run_summary_path,exception_detail_path,)

        summary = reconciliation_run_metrics.first()

        logger.info(
            "reconciliation_completed",
            reconciliation_run_id=reconciliation_run_id,
            entity_name="customer_consents",
            snapshot_as_of=snapshot_as_of,
            snapshot_row_count=summary.snapshot_row_count,
            cdc_row_count=summary.cdc_row_count,
            matched_row_count=summary.matched_row_count,
            exception_count=summary.exception_count,
            missing_in_cdc_count=summary.missing_in_cdc_count,
            missing_in_snapshot_count=summary.missing_in_snapshot_count,
            checksum_mismatch_count=summary.checksum_mismatch_count,
            overall_status=summary.overall_status,
            duration_seconds=round(time.perf_counter() - start_time, 3),
        )
    except Exception as exc:
        logger.error(
            "reconciliation_failed",
            reconciliation_run_id=reconciliation_run_id,
            entity_name="customer_consents",
            snapshot_as_of=snapshot_as_of,
            duration_seconds=round(time.perf_counter() - start_time, 3),
            error=str(exc),
        )
        raise




if __name__ == "__main__":
    run_customer_consent_reconciliation(snapshot_as_of="2026-09-14 09:55:00")