from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from atlas.common.config.loader import get_settings
from atlas.common.spark.session import get_spark_session
from atlas.common.paths.loader import get_paths

project_root = Path(__file__).resolve().parents[4]


@pytest.fixture
def settings():
    settings = get_settings(
        project_root / "configs" / "base.yaml",
        project_root / "configs" / "local.yaml",
        project_root / "pyproject.toml",
    )
    return settings
@pytest.fixture
def spark(settings):
    spark_session = get_spark_session(settings.spark, settings.storage, settings.application.name)

    yield spark_session

    spark_session.stop()
    get_spark_session.cache_clear()

def test_spark_session_is_created(spark) -> None:
    assert isinstance(spark, SparkSession)


def test_spark_shuffle_partitions(spark) -> None:
    assert spark.conf.get("spark.sql.shuffle.partitions") == "8"


def test_spark_timezone(spark) -> None:
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_spark_aqe_enabled(spark) -> None:
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"


def test_delta_local_write_and_read(tmp_path: Path, spark) -> None:
    data = [{"name":"sailesh", "age":30},{"name":"pola", "age":32}]
    df = spark.createDataFrame(data)
    delta_path = tmp_path / "delta_smoke"

    df.write.format("delta").mode("overwrite").save(str(delta_path))
    actual_df = spark.read.format("delta").load(str(delta_path))

    actual = {
        (row["name"], row["age"])
        for row in actual_df.collect()
    }
    expected = {
        ("sailesh", 30),
        ("pola", 32),
    }

    assert actual == expected

def test_delta_named_table_write_and_read(tmp_path: Path, spark) -> None:
    namespace = "smoke"
    table_name = f"{namespace}.customer_smoke"
    namespace_path = tmp_path / namespace

    data = [
        {"customer_id": 1, "name": "sailesh"},
        {"customer_id": 2, "name": "pola"},
    ]
    df = spark.createDataFrame(data)

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {namespace} "
        f"LOCATION '{namespace_path.as_uri()}'"
    )

    try:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        actual_df = spark.table(table_name)

        actual = {
            (row["customer_id"], row["name"])
            for row in actual_df.collect()
        }

        expected = {
            (1, "sailesh"),
            (2, "pola"),
        }

        assert actual == expected

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"DROP DATABASE IF EXISTS {namespace}")

def test_delta_minio_write_and_read(spark, settings) -> None:
    delta_path = get_paths(settings)
    actual_delta_path = delta_path.bronze_path("customer_smoke")
    data = [
        {"customer_id": 1, "name": "sailesh"},
        {"customer_id": 2, "name": "pola"},
    ]

    df = spark.createDataFrame(data)

    df.write.format("delta").mode("overwrite").save(actual_delta_path)

    actual_df = spark.read.format("delta").load(actual_delta_path)

    actual = {
        (row["customer_id"], row["name"])
        for row in actual_df.collect()
    }

    expected = {
        (1, "sailesh"),
        (2, "pola"),
    }

    assert actual == expected