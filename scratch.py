from atlas.common.config.loader import get_settings
from atlas.common.logging.configuration import configure_logging
from atlas.common.logging.logger import get_logger
from atlas.common.spark.session import get_spark_session

settings = get_settings(
    "/Users/saileshpola/Desktop/AtlasProject/configs/base.yaml",
    "/Users/saileshpola/Desktop/AtlasProject/configs/local.yaml",
    "/Users/saileshpola/Desktop/AtlasProject/pyproject.toml",
)

configure_logging(settings.logging)

logger = get_logger(__name__)

spark = get_spark_session(settings.spark, settings.application.name)
logger.info("Spark Session created")
spark.stop()
logger.info("Spark Session stopped")
