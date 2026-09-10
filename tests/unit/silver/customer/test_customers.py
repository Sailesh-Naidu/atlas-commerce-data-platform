from datetime import date

from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from atlas.silver.customer.cdc.jobs.customers import apply_customer_dq

CUSTOMER_TEST_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("status", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("cdc_operation", StringType(), True),
    StructField("source_lsn", LongType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
])

def _evaluate_dq(spark, **overrides):
    """Create one normalized Customer record and return its DQ errors."""

    customer = {
        "customer_id": 1,
        "first_name": "Sailesh",
        "last_name": "Naidu",
        "email": "sailesh@example.com",
        "phone_number": "9999999999",
        "date_of_birth": date(1995, 1, 1),
        "status": "ACTIVE",
        "segment": "STANDARD",
        "cdc_operation": "u",
        "source_lsn": 100,
        "kafka_topic": "atlas.customer.public.customers",
        "kafka_partition": 0,
        "kafka_offset": 10,
    }

    customer.update(overrides)

    customer_df = spark.createDataFrame(
        [Row(**customer)],
        schema=CUSTOMER_TEST_SCHEMA,
    )

    result = apply_customer_dq(customer_df).first()

    return result["dq_errors"]


def test_valid_customer_has_no_dq_errors(spark):
    assert _evaluate_dq(spark) == []


def test_blank_first_name_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        first_name="   ",
    )

    assert "MISSING_FIRST_NAME" in errors


def test_missing_contact_information_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        email=None,
        phone_number=None,
    )

    assert "MISSING_CONTACT_INFO" in errors


def test_blank_contact_information_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        email="",
        phone_number="   ",
    )

    assert "MISSING_CONTACT_INFO" in errors


def test_null_status_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        status=None,
    )

    assert "INVALID_STATUS" in errors


def test_invalid_segment_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        segment="VIP",
    )

    assert "INVALID_SEGMENT" in errors


def test_delete_does_not_require_business_attributes(spark):
    errors = _evaluate_dq(
        spark,
        cdc_operation="d",
        first_name=None,
        last_name=None,
        email=None,
        phone_number=None,
        status=None,
        segment=None,
    )

    assert errors == []


def test_delete_requires_customer_id(spark):
    errors = _evaluate_dq(
        spark,
        cdc_operation="d",
        customer_id=None,
    )

    assert errors == ["MISSING_CUSTOMER_ID"]


def test_delete_requires_source_lsn(spark):
    errors = _evaluate_dq(
        spark,
        cdc_operation="d",
        source_lsn=None,
    )

    assert errors == ["MISSING_SOURCE_LSN"]


def test_invalid_cdc_operation_is_rejected(spark):
    errors = _evaluate_dq(
        spark,
        cdc_operation="x",
    )

    assert "INVALID_CDC_OPERATION" in errors