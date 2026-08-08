from atlas.common.config.loader import get_settings
from atlas.common.logging.configuration import configure_logging
from atlas.common.logging.logger import get_logger

settings = get_settings(
    "/Users/saileshpola/Desktop/AtlasProject/configs/base.yaml",
    "/Users/saileshpola/Desktop/AtlasProject/configs/local.yaml",
    "/Users/saileshpola/Desktop/AtlasProject/pyproject.toml",
)

configure_logging(settings.logging)

logger = get_logger(__name__)

logger.debug("Debug message")
logger.info("Application started")
logger.warning("Warning message")
logger.error("Something went wrong")
