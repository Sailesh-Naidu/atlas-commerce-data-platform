from datetime import UTC, datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from atlas.silver.customer.cdc.jobs.customer_consents import apply_consent_dq

CONSENT_TEST_SCHEMA = StructType([
    StructField("consent_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("consent_type", StringType(), True),
    StructField("granted", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("cdc_operation", StringType(), True),
    StructField("source_lsn", LongType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
])


def _evaluate_consent_dq(spark, **overrides):
    """Create one normalized Consent record and return its DQ errors."""

    now = datetime.now(UTC)

    consent = {
        "consent_id": 1,
        "customer_id": 1,
        "consent_type": "EMAIL",
        "granted": True,
        "created_at": now,
        "updated_at": now,
        "cdc_operation": "u",
        "source_lsn": 100,
        "kafka_topic": "atlas.customer.public.customer_consents",
        "kafka_partition": 0,
        "kafka_offset": 10,
    }

    consent.update(overrides)

    consent_df = spark.createDataFrame(
        [Row(**consent)],
        schema=CONSENT_TEST_SCHEMA,
    )

    return apply_consent_dq(consent_df).first()["dq_errors"]


def test_valid_consent_has_no_dq_errors(spark):
    assert _evaluate_consent_dq(spark) == []


def test_missing_customer_id_is_rejected_for_update(spark):
    errors = _evaluate_consent_dq(
        spark,
        customer_id=None,
    )

    assert "MISSING_CUSTOMER_ID" in errors


def test_blank_consent_type_is_rejected(spark):
    errors = _evaluate_consent_dq(
        spark,
        consent_type="   ",
    )

    assert "MISSING_CONSENT_TYPE" in errors


def test_missing_granted_is_rejected(spark):
    errors = _evaluate_consent_dq(
        spark,
        granted=None,
    )

    assert "MISSING_GRANTED" in errors


def test_delete_does_not_require_consent_business_attributes(spark):
    errors = _evaluate_consent_dq(
        spark,
        cdc_operation="d",
        customer_id=None,
        consent_type=None,
        granted=None,
    )

    assert errors == []


def test_delete_requires_consent_id(spark):
    errors = _evaluate_consent_dq(
        spark,
        cdc_operation="d",
        consent_id=None,
    )

    assert errors == ["MISSING_CONSENT_ID"]