from atlas.bronze.customer.cdc.jobs.customer_cdc_common import (
    customer_cdc_read_stream,
    customer_cdc_write_stream,
    get_customer_paths,
)
from atlas.common.config.loader import get_settings
from atlas.common.spark.session import get_spark_session


def customer_addresses_cdc_bronze() -> None:
    """Run the customer addresses CDC bronze ingestion job.
        Loads application settings, initializes Spark, reads customer CDC events
        from Kafka, and writes the raw events to the bronze storage layer.
        """
    settings = get_settings("configs/base.yaml", "configs/local.yaml", "pyproject.toml")
    spark = get_spark_session(settings.spark, settings.storage, settings.application.name)

    customer_data_bronze = customer_cdc_read_stream(spark, settings.kafka.bootstrap_servers,
                                                    settings.customer.customer_addresses_topic)

    customer_addresses_data_path, customer_addresses_checkpoint_path = get_customer_paths(settings,
                                                                                          "customer_addresses")

    customer_cdc_write_stream(customer_data_bronze, customer_addresses_data_path, customer_addresses_checkpoint_path)

if __name__ == "__main__":
    customer_addresses_cdc_bronze()