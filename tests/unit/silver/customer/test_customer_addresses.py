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

from atlas.silver.customer.cdc.jobs.customer_addresses import apply_address_dq

ADDRESS_TEST_SCHEMA = StructType([
    StructField("address_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("address_type", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_primary", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("cdc_operation", StringType(), True),
    StructField("source_lsn", LongType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
])


def _evaluate_address_dq(spark, **overrides):
    """Create one normalized Address record and return its DQ errors."""

    now = datetime.now(UTC)

    address = {
        "address_id": 1,
        "customer_id": 1,
        "address_type": "HOME",
        "address_line_1": "123 Atlas Street",
        "address_line_2": None,
        "city": "Hyderabad",
        "state": "Telangana",
        "postal_code": "500001",
        "country": "India",
        "is_primary": True,
        "created_at": now,
        "updated_at": now,
        "cdc_operation": "u",
        "source_lsn": 100,
        "kafka_topic": "atlas.customer.public.customer_addresses",
        "kafka_partition": 0,
        "kafka_offset": 10,
    }

    address.update(overrides)

    address_df = spark.createDataFrame(
        [Row(**address)],
        schema=ADDRESS_TEST_SCHEMA,
    )

    return apply_address_dq(address_df).first()["dq_errors"]


def test_valid_address_has_no_dq_errors(spark):
    assert _evaluate_address_dq(spark) == []


def test_missing_customer_id_is_rejected_for_update(spark):
    errors = _evaluate_address_dq(
        spark,
        customer_id=None,
    )

    assert "MISSING_CUSTOMER_ID" in errors


def test_invalid_address_type_is_rejected(spark):
    errors = _evaluate_address_dq(
        spark,
        address_type="WORK",
    )

    assert "INVALID_ADDRESS_TYPE" in errors


def test_blank_required_address_fields_are_rejected(spark):
    errors = _evaluate_address_dq(
        spark,
        address_line_1="   ",
        city="",
        postal_code="   ",
        country="",
    )

    assert "MISSING_ADDRESS_LINE_1" in errors
    assert "MISSING_CITY" in errors
    assert "MISSING_POSTAL_CODE" in errors
    assert "MISSING_COUNTRY" in errors


def test_delete_does_not_require_address_business_attributes(spark):
    errors = _evaluate_address_dq(
        spark,
        cdc_operation="d",
        customer_id=None,
        address_type=None,
        address_line_1=None,
        city=None,
        postal_code=None,
        country=None,
        is_primary=None,
    )

    assert errors == []


def test_delete_requires_address_id(spark):
    errors = _evaluate_address_dq(
        spark,
        cdc_operation="d",
        address_id=None,
    )

    assert errors == ["MISSING_ADDRESS_ID"]