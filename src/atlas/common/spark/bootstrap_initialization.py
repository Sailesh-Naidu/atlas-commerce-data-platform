from atlas.common.config.loader import get_settings
from atlas.common.spark.session import get_spark_session


def initialize_atlas():
    """Initialize the Atlas application runtime.

    Loads the validated application settings and creates the Spark session
    using the configured Spark, storage, and application settings.

    Returns:
        Tuple containing the validated Atlas settings and initialized Spark session.
    """
    settings = get_settings("configs/base.yaml", "configs/local.yaml", "pyproject.toml")
    return settings, get_spark_session(settings.spark, settings.storage, settings.application.name)
