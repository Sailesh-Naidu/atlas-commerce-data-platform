from delta import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_reconciliation_status_bucket_condition() -> Column:
    """
    Build the Spark expression used to classify reconciliation status at bucket level.

    Returns:
        Column: A Spark column expression producing one of MATCH,
            MISSING_IN_SNAPSHOT, MISSING_IN_CDC, COUNT_MISMATCH,
            or CHECKSUM_MISMATCH.
    """
    return (
        F.when(
            F.col("snapshot_row_count").isNull()
            & F.col("cdc_row_count").isNotNull(),
            F.lit("MISSING_IN_SNAPSHOT"),
        )
        .when(
            F.col("cdc_row_count").isNull()
            & F.col("snapshot_row_count").isNotNull(),
            F.lit("MISSING_IN_CDC"),
        )
        .when(
            F.col("snapshot_row_count") != F.col("cdc_row_count"),
            F.lit("COUNT_MISMATCH"),
        )
        .when(
            F.col("snapshot_checksum") != F.col("cdc_checksum"),
            F.lit("CHECKSUM_MISMATCH"),
        )
        .otherwise(F.lit("MATCH"))
    )


def get_reconciliation_row_status_condition() -> Column:
    """
    Build the Spark expression used to classify reconciliation status at row level.

    Returns:
        Column: A Spark column expression producing one of MATCH,
            MISSING_IN_SNAPSHOT, MISSING_IN_CDC, or CHECKSUM_MISMATCH.
    """
    return (
        F.when(
            F.col("snapshot_present").isNull()
            & F.col("cdc_present").isNotNull(),
            F.lit("MISSING_IN_SNAPSHOT"),
        )
        .when(
            F.col("cdc_present").isNull()
            & F.col("snapshot_present").isNotNull(),
            F.lit("MISSING_IN_CDC"),
        )
        .when(
            F.col("snapshot_row_checksum") != F.col("cdc_row_checksum"),
            F.lit("CHECKSUM_MISMATCH"),
        )
        .otherwise(F.lit("MATCH"))
    )


def attach_snapshot_metadata(snapshot_dataframe: DataFrame,entity: str,snapshot_as_of: str,) -> DataFrame:
    """
    Attach standard reconciliation metadata to an authoritative snapshot.

    Args:
        snapshot_dataframe: Source snapshot DataFrame.
        entity: Entity name used for batch and source-file metadata.
        snapshot_as_of: Point-in-time represented by the snapshot.

    Returns:
        DataFrame: Snapshot DataFrame enriched with snapshot, source,
            batch, schema-version, and ingestion metadata.
    """
    return (
        snapshot_dataframe
        .withColumn("snapshot_as_of",F.to_timestamp(F.lit(snapshot_as_of)),)
        .withColumn("source_date",F.to_date(F.col("snapshot_as_of")),)
        .withColumn("batch_id",
            F.concat_ws(
                "_",
                F.lit(f"{entity}_snapshot"),
                F.date_format(F.col("snapshot_as_of"), "yyyyMMdd"),
            ),
        )
        .withColumn(
            "source_filename",
            F.concat(
                F.lit(f"{entity}_snapshot_"),
                F.date_format(
                    F.col("snapshot_as_of"),
                    "yyyy-MM-dd",
                ),
                F.lit(".csv"),
            ),
        )
        .withColumn("schema_version", F.lit(1))
        .withColumn("ingested_at", F.current_timestamp())
    )


def get_snapshot_valid_data(snapshot_with_metadata: DataFrame,validation_conditions: Column,entity_key: str,) -> DataFrame:
    """
    Filter invalid and duplicate snapshot records before reconciliation.

    Args:
        snapshot_with_metadata: Snapshot DataFrame containing reconciliation metadata.
        validation_conditions: Array-valued Spark expression containing validation failures.
        entity_key: Business key used to detect duplicate entities.

    Returns:
        DataFrame: Valid snapshot records with duplicate business keys excluded.
    """

    snapshot_filtered = snapshot_with_metadata.withColumn("snapshot_errors",F.array_compact(validation_conditions),)

    snapshot_valid_records = (snapshot_filtered.filter(F.size(F.col("snapshot_errors")) == 0).drop("snapshot_errors"))

    snapshot_duplicate_rows = (snapshot_valid_records.groupBy(entity_key)
                               .agg(F.count("*").alias("total_count")).filter(F.col("total_count") > 1))

    return snapshot_valid_records.join(snapshot_duplicate_rows,entity_key,"left_anti",)


def get_cdc_valid_data(spark: SparkSession,silver_entity_history_path: str,snapshot_as_of: str,entity_key: str,) -> DataFrame:
    """
    Reconstruct the CDC entity state as of the snapshot timestamp.

    The latest CDC record for each business key is selected using source LSN
    ordering, and entities whose latest operation is  delete are excluded.

    Args:
        spark: Active Spark session.
        silver_entity_history_path: Path to the Silver CDC history Delta table.
        snapshot_as_of: Point-in-time for CDC state reconstruction.
        entity_key: Business key used to partition CDC history.

    Returns:
        DataFrame: Reconstructed non-deleted CDC state at snapshot time.
    """

    entity_cdc_history = DeltaTable.forPath(spark, silver_entity_history_path,).toDF()

    cdc_data_at_snapshot_time = entity_cdc_history.filter(F.col("source_timestamp")<= F.to_timestamp(F.lit(snapshot_as_of)))

    entity_as_of_window = Window.partitionBy(entity_key).orderBy(F.col("source_lsn").desc())

    reconstructed_cdc_entity_state = (cdc_data_at_snapshot_time
                                      .withColumn("rn",F.row_number().over(entity_as_of_window),).filter(F.col("rn") == 1)
                                      .drop("rn"))

    return reconstructed_cdc_entity_state.filter(F.col("cdc_operation") != "d")


def get_columns_renamed(reconciliation_columns: list[str],) -> tuple[list[Column], list[Column]]:
    """
    Build snapshot-side and CDC-side projections for reconciliation detail.

    Args:
        reconciliation_columns: Business columns compared during reconciliation.

    Returns:
        tuple[list[Column], list[Column]]: Snapshot and CDC column expressions
            renamed with snapshot_ and cdc_ prefixes.
    """

    snapshot_columns = [F.col(f"s.{col}").alias(f"snapshot_{col}")for col in reconciliation_columns]
    cdc_columns = [F.col(f"c.{col}").alias(f"cdc_{col}")for col in reconciliation_columns]

    return snapshot_columns, cdc_columns

def mismatch_reasons(reconciliation_columns):
    """
    Build field-level mismatch diagnostics for reconciled entity records.

    Columns are compared using null-safe equality so that two null values
    are treated as equal while null versus non-null is treated as a mismatch.

    Args:
        reconciliation_columns: Business columns to compare.

    Returns:
        Column: Array-valued Spark expression containing mismatch reason codes.
    """
    mismatch_reason = []

    for col in reconciliation_columns:
        mismatch_reason.append(
            F.when(~F.col(f"snapshot_{col}").eqNullSafe(F.col(f"cdc_{col}")),F.lit(f"{col.upper()}_MISMATCH")))

    return F.array_compact(F.array(*mismatch_reason))


def get_final_reconciliation_metadata(final_reconciliation_exceptions:DataFrame, snapshot_as_of:str,
                                      entity_name:str, reconciliation_run_id: str) ->DataFrame:
    """
    Attach run-level audit metadata to reconciliation exception records.

    Args:
        final_reconciliation_exceptions: Row-level reconciliation exceptions.
        snapshot_as_of: Point-in-time represented by the authoritative snapshot.
        entity_name: Entity being reconciled.
        reconciliation_run_id: Identifier shared by all records from the run.

    Returns:
        DataFrame: Reconciliation exceptions enriched with run metadata.
    """

    return (final_reconciliation_exceptions.withColumn("reconciliation_run_id",F.lit(reconciliation_run_id))
        .withColumn("snapshot_as_of",F.to_timestamp(F.lit(snapshot_as_of)))
        .withColumn("reconciled_at",F.current_timestamp())
        .withColumn("entity_name",F.lit(entity_name)))


def get_reconciliation_records(bucket_count: int,snapshot_valid_records: DataFrame,
                               reconstructed_cdc_entity_state: DataFrame,normalized_reconciliation_columns: list[Column],
                               entity_key: str,reconciliation_columns: list[str],) -> DataFrame:
    """
    Reconcile an authoritative snapshot against reconstructed CDC entity state.

    The comparison first uses bucket-level counts and checksums to avoid
    row-level comparison of matching buckets. Unmatched buckets are then
    compared by business key and classified at row and field level.

    Args:
        bucket_count: Number of deterministic reconciliation buckets.
        snapshot_valid_records: Reconciliation-ready authoritative snapshot.
        reconstructed_cdc_entity_state: CDC state reconstructed at snapshot time.
        normalized_reconciliation_columns: Normalized Spark expressions used
            to calculate deterministic row checksums.
        entity_key: Business key used for bucketing and row comparison.
        reconciliation_columns: Business fields used for mismatch diagnosis.

    Returns:
        DataFrame: Row-level reconciliation exceptions including presence,
            checksum, source values, and field-level mismatch details.
    """


    snapshot_bucketed = snapshot_valid_records.withColumn("bucket_id",
                                                          F.pmod(F.hash(F.col(entity_key)),F.lit(bucket_count),) + 1,)

    cdc_bucketed = reconstructed_cdc_entity_state.withColumn("bucket_id"
                                                             ,F.pmod(F.hash(F.col(entity_key)),F.lit(bucket_count),) + 1)

    snapshot_with_checksum = snapshot_bucketed.withColumn("row_checksum",
        F.sha2(F.concat_ws("||",*normalized_reconciliation_columns,),256,),)

    cdc_with_checksum = cdc_bucketed.withColumn("row_checksum",
        F.sha2(F.concat_ws("||",*normalized_reconciliation_columns,),256,),)

    snapshot_bucket_checksum = (snapshot_with_checksum.groupBy("bucket_id")
                                .agg(F.sha2(F.concat_ws("||",F.sort_array(F.collect_list("row_checksum")),),256,)
                                     .alias("snapshot_checksum"),F.count(F.col(entity_key)).alias("snapshot_row_count"),))

    cdc_bucket_checksum = (cdc_with_checksum.groupBy("bucket_id")
                           .agg(F.sha2(F.concat_ws("||",F.sort_array(F.collect_list("row_checksum")),),256,)
                                .alias("cdc_checksum"),F.count(F.col(entity_key)).alias("cdc_row_count"),))

    bucket_reconciliation = (snapshot_bucket_checksum
                             .join(cdc_bucket_checksum,on="bucket_id",how="full_outer",)
                             .withColumn("reconciliation_status",get_reconciliation_status_bucket_condition(),))

    unmatched_buckets = bucket_reconciliation.filter(F.col("reconciliation_status") != "MATCH")

    snapshot_unmatched_rows = (snapshot_with_checksum
                               .join(unmatched_buckets.select("bucket_id"),on="bucket_id",how="left_semi",)
                               .withColumnRenamed("row_checksum","snapshot_row_checksum",)
                               .withColumn("snapshot_present",F.lit(True),))

    cdc_unmatched_rows = (cdc_with_checksum
                          .join(unmatched_buckets.select("bucket_id"),on="bucket_id",how="left_semi",)
                          .withColumnRenamed("row_checksum","cdc_row_checksum",)
                          .withColumn("cdc_present",F.lit(True),))

    joined_snapshot_cdc = (snapshot_unmatched_rows.alias("s")
                           .join(cdc_unmatched_rows.alias("c"),on=entity_key,how="full_outer",)
                           .withColumn("reconciliation_status",get_reconciliation_row_status_condition(),))

    reconciled_unmatched_rows = joined_snapshot_cdc.filter(F.col("reconciliation_status") != "MATCH")

    reconciled_matched_rows = joined_snapshot_cdc.filter(F.col("reconciliation_status") == "MATCH")

    snapshot_columns, cdc_columns = get_columns_renamed(reconciliation_columns)

    reconciliation_unmatched_renamed = (
        reconciled_unmatched_rows.select(
            F.col(entity_key),
            F.col("s.snapshot_row_checksum"),
            F.col("c.cdc_row_checksum"),
            F.col("snapshot_present"),
            F.col("cdc_present"),
            F.col("reconciliation_status"),
            *snapshot_columns,
            *cdc_columns,
        )
    )

    mismatch_rows = reconciliation_unmatched_renamed.filter(F.col("reconciliation_status") == "CHECKSUM_MISMATCH")
    missing_rows = reconciliation_unmatched_renamed.filter(F.col("reconciliation_status") != "CHECKSUM_MISMATCH")

    reconciled_entity_mismatch = mismatch_rows.withColumn("mismatch_columns",
                                                                     mismatch_reasons(reconciliation_columns))

    missing_rows = missing_rows.withColumn("mismatch_columns",F.array().cast("array<string>"))

    final_reconciliation_exceptions = missing_rows.unionByName(reconciled_entity_mismatch)

    return  final_reconciliation_exceptions


def get_reconciliation_run_metrics(snapshot_valid_records: DataFrame, reconstructed_cdc_entity_state: DataFrame,
                                   final_reconciliation_exceptions: DataFrame, reconciliation_run_id: str,snapshot_as_of:str,
                                   entity_name:str) ->DataFrame:
    """
    Build run-level reconciliation metrics and overall reconciliation status.

    Args:
        snapshot_valid_records: Reconciliation-ready authoritative snapshot.
        reconstructed_cdc_entity_state: CDC state reconstructed at snapshot time.
        final_reconciliation_exceptions: Row-level reconciliation exceptions.
        reconciliation_run_id: Identifier shared by the reconciliation run.
        snapshot_as_of: Point-in-time represented by the snapshot.
        entity_name: Name of the entity whose reconciliation metrics is being evaluated.

    Returns:
        DataFrame: Single-row summary containing source counts, exception counts,
            matched-row count, reconciliation status, and audit metadata.
    """

    snapshot_run_metrics = snapshot_valid_records.agg(F.count("*").alias("snapshot_row_count"))
    cdc_run_metrics = reconstructed_cdc_entity_state.agg(F.count("*").alias("cdc_row_count"))

    exception_run_metrics = final_reconciliation_exceptions.agg(F.count("*").alias("exception_count"),
        F.coalesce(F.sum(F.when(F.col("reconciliation_status") == "MISSING_IN_CDC", 1).otherwise(0)),F.lit(0))
                                                                .alias("missing_in_cdc_count"),
        F.coalesce(F.sum(F.when(F.col("reconciliation_status") == "MISSING_IN_SNAPSHOT", 1).otherwise(0)), F.lit(0))
                                                                .alias("missing_in_snapshot_count"),
        F.coalesce(F.sum(F.when(F.col("reconciliation_status") == "CHECKSUM_MISMATCH", 1).otherwise(0)), F.lit(0))
                                                                .alias("checksum_mismatch_count"))

    return (snapshot_run_metrics.crossJoin(cdc_run_metrics).crossJoin(exception_run_metrics)
        .withColumn("matched_row_count",F.col("snapshot_row_count")- F.col("missing_in_cdc_count")- F.col("checksum_mismatch_count"))
        .withColumn("overall_status",
            F.when(F.col("exception_count") == 0,F.lit("SUCCESS")).otherwise(F.lit("COMPLETED_WITH_EXCEPTIONS")))
        .withColumn("reconciliation_run_id",F.lit(reconciliation_run_id))
        .withColumn("entity_name",F.lit(entity_name))
        .withColumn("snapshot_as_of",F.to_timestamp(F.lit(snapshot_as_of)))
        .withColumn("reconciled_at",F.current_timestamp()))

def persist_reconciliation_results(reconciliation_run_metrics: DataFrame,final_reconciliation_exceptions: DataFrame,
                                   run_summary_path: str,exception_detail_path: str,) -> None:
    """Persist reconciliation outputs as append-only Delta datasets."""
    final_reconciliation_exceptions.write.format("delta").mode("append").save(exception_detail_path)
    reconciliation_run_metrics.write.format("delta").mode("append").save(run_summary_path)
