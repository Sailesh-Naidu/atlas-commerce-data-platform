import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provide a Delta-enabled local SparkSession for tests."""

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("atlas-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark_session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    yield spark_session

    spark_session.stop()