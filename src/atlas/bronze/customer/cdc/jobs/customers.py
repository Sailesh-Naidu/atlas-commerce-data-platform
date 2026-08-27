from atlas.bronze.customer.cdc.jobs.customer_cdc_common import (
    customer_cdc_read_stream,
    customer_cdc_write_stream,
)
from atlas.common.paths.get_cdc_paths import get_bronze_paths
from atlas.common.spark.bootstrap_initialization import initialize_atlas


def customer_cdc_bronze() -> None:
    """Run the customer CDC bronze ingestion job.
        Loads application settings, initializes Spark, reads customer CDC events
        from Kafka, and writes the raw events to the bronze storage layer.
        """
    settings, spark = initialize_atlas()

    customer_data_bronze = customer_cdc_read_stream(spark, settings.kafka.bootstrap_servers,
                                                    settings.customer.customers_topic)

    customer_data_path, customer_checkpoint_path = get_bronze_paths(settings, "customer", "customers")
    customer_cdc_write_stream(customer_data_bronze, customer_data_path, customer_checkpoint_path)

if __name__ == "__main__":
    customer_cdc_bronze()