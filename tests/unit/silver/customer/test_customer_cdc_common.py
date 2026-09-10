from pyspark.sql import Row
from pyspark.sql import functions as F

from atlas.silver.customer.cdc.jobs.customer_cdc_common import (
    classify_cdc_against_history,
    merge_cdc_canonical_events,
    split_cdc_events,
)


def test_split_cdc_events_deduplicates_exact_kafka_replay(spark):
    """Exact Kafka record replay should appear only once."""

    data = [
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        ),
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        ),
    ]

    input_df = spark.createDataFrame(data)

    orderable, ambiguous = split_cdc_events(
        input_df,
        "customer_id",
    )

    assert orderable.count() == 1
    assert ambiguous.count() == 0


def test_split_cdc_events_detects_cross_partition_ambiguity(spark):
    """Same entity and LSN across partitions should be rejected as ambiguous."""

    data = [
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        ),
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=1,
            kafka_offset=20,
        ),
    ]

    input_df = spark.createDataFrame(data)

    orderable, ambiguous = split_cdc_events(
        input_df,
        "customer_id",
    )

    assert orderable.count() == 0
    assert ambiguous.count() == 2

    statuses = {
        row["cdc_status"]
        for row in ambiguous.select("cdc_status").collect()
    }

    assert statuses == {"AMBIGUOUS_ORDERING"}


def test_split_cdc_events_keeps_normal_events_orderable(spark):
    """Different LSNs for the same entity should remain orderable."""

    data = [
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        ),
        Row(
            customer_id=1,
            source_lsn=110,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=11,
        ),
    ]

    input_df = spark.createDataFrame(data)

    orderable, ambiguous = split_cdc_events(
        input_df,
        "customer_id",
    )

    assert orderable.count() == 2
    assert ambiguous.count() == 0

def test_classify_cdc_against_history_handles_all_classifications(
    spark,
    tmp_path,
):
    """Verify NEW, NEWER, STALE, replay, and ambiguity behavior."""

    history_path = str(tmp_path / "customer_cdc_history")

    history_df = spark.createDataFrame([
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        )
    ])

    history_df.write.format("delta").save(history_path)

    incoming_df = spark.createDataFrame([
        # NEW
        Row(
            customer_id=2,
            source_lsn=110,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=20,
        ),

        # NEWER
        Row(
            customer_id=1,
            source_lsn=120,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=21,
        ),

        # STALE
        Row(
            customer_id=1,
            source_lsn=90,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=22,
        ),

        # Exact previously accepted Kafka event
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        ),

        # Same LSN but different Kafka partition
        Row(
            customer_id=1,
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=1,
            kafka_offset=23,
        ),
    ])

    empty_ambiguous_df = (
        incoming_df.limit(0)
        .withColumn("cdc_status", F.lit(None).cast("string"))
        .withColumn("persisted_source_lsn", F.lit(None).cast("long"))
        .withColumn("persisted_kafka_partition", F.lit(None).cast("int"))
        .withColumn("persisted_kafka_offset", F.lit(None).cast("long"))
        .withColumn("rejected_at", F.current_timestamp())
    )

    eligible, rejected = classify_cdc_against_history(
        spark=spark,
        silver_entity_history_path=history_path,
        entity_key="customer_id",
        entity_incoming_orderable_data=incoming_df,
        entity_incoming_ambiguous_events=empty_ambiguous_df,
    )

    eligible_events = {
        (
            row.customer_id,
            row.source_lsn,
            row.kafka_partition,
            row.kafka_offset,
        )
        for row in eligible.select(
            "customer_id",
            "source_lsn",
            "kafka_partition",
            "kafka_offset",
        ).collect()
    }

    assert eligible_events == {
        (2, 110, 0, 20),   # NEW
        (1, 120, 0, 21),   # NEWER
        (1, 100, 0, 10),   # already accepted replay
    }

    rejected_events = {
        (
            row.customer_id,
            row.source_lsn,
            row.kafka_partition,
            row.kafka_offset,
            row.cdc_status,
        )
        for row in rejected.select(
            "customer_id",
            "source_lsn",
            "kafka_partition",
            "kafka_offset",
            "cdc_status",
        ).collect()
    }

    assert rejected_events == {
        (1, 90, 0, 22, "STALE"),
        (1, 100, 1, 23, "AMBIGUOUS_ORDERING"),
    }

def test_merge_cdc_canonical_events_handles_insert_update_replay_delete(
    spark,
    tmp_path,
):
    """Verify canonical state across insert, update, replay, and delete."""

    canonical_path = str(tmp_path / "customer_canonical")

    # ---------------------------------------------------------
    # 1. INSERT
    # ---------------------------------------------------------

    insert_df = spark.createDataFrame([
        Row(
            customer_id=1,
            first_name="Sailesh",
            status="ACTIVE",
            cdc_operation="c",
            source_lsn=100,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=10,
        )
    ])

    merge_cdc_canonical_events(
        spark=spark,
        silver_entity_canonical_path=canonical_path,
        entity_cdc_accepted_events=insert_df,
        entity_key="customer_id",
    )

    canonical = spark.read.format("delta").load(canonical_path)

    assert canonical.count() == 1

    inserted = canonical.first()

    assert inserted.customer_id == 1
    assert inserted.first_name == "Sailesh"
    assert inserted.status == "ACTIVE"
    assert inserted.source_lsn == 100


    # ---------------------------------------------------------
    # 2. UPDATE
    # ---------------------------------------------------------

    update_df = spark.createDataFrame([
        Row(
            customer_id=1,
            first_name="Sailesh Updated",
            status="SUSPENDED",
            cdc_operation="u",
            source_lsn=200,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=11,
        )
    ])

    merge_cdc_canonical_events(
        spark=spark,
        silver_entity_canonical_path=canonical_path,
        entity_cdc_accepted_events=update_df,
        entity_key="customer_id",
    )

    canonical = spark.read.format("delta").load(canonical_path)

    assert canonical.count() == 1

    updated = canonical.first()

    assert updated.customer_id == 1
    assert updated.first_name == "Sailesh Updated"
    assert updated.status == "SUSPENDED"
    assert updated.source_lsn == 200
    assert updated.kafka_offset == 11


    # ---------------------------------------------------------
    # 3. REPLAY THE SAME ACCEPTED UPDATE
    # ---------------------------------------------------------

    merge_cdc_canonical_events(
        spark=spark,
        silver_entity_canonical_path=canonical_path,
        entity_cdc_accepted_events=update_df,
        entity_key="customer_id",
    )

    canonical = spark.read.format("delta").load(canonical_path)

    assert canonical.count() == 1

    replayed = canonical.first()

    assert replayed.customer_id == 1
    assert replayed.first_name == "Sailesh Updated"
    assert replayed.status == "SUSPENDED"
    assert replayed.source_lsn == 200
    assert replayed.kafka_offset == 11


    # ---------------------------------------------------------
    # 4. DELETE
    # ---------------------------------------------------------

    delete_df = spark.createDataFrame([
        Row(
            customer_id=1,
            first_name=None,
            status=None,
            cdc_operation="d",
            source_lsn=300,
            kafka_topic="atlas.customer.public.customers",
            kafka_partition=0,
            kafka_offset=12,
        )
    ], schema=update_df.schema)

    merge_cdc_canonical_events(
        spark=spark,
        silver_entity_canonical_path=canonical_path,
        entity_cdc_accepted_events=delete_df,
        entity_key="customer_id",
    )

    canonical = spark.read.format("delta").load(canonical_path)

    assert canonical.count() == 0