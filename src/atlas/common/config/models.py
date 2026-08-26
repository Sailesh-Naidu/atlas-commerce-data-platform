from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator


class AtlasBaseSettings(BaseModel):
    """Common configuration behavior shared by all Atlas configuration models."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ApplicationSettings(AtlasBaseSettings):
    """Application-level configuration."""

    name: str = Field(min_length=3)
    environment: Literal["local", "dev", "test", "prod"]
    app_version: str = Field(min_length=3)


class SparkSettings(AtlasBaseSettings):
    """Spark runtime defaults loaded during application startup."""

    master: str = Field(min_length=1)
    shuffle_partitions: int = Field(gt=0)
    session_timezone: str
    broadcast_size_mb: int = Field(gt=10)
    adaptive_query_execution: bool = True


class StorageSettings(AtlasBaseSettings):
    """Storage locations used by Atlas pipelines."""

    mode: Literal["local", "object_store"] = "local"
    lakehouse_root: Path
    checkpoint_root: Path
    quarantine_root: Path

    endpoint: str | None = None
    bucket: str | None = None
    access_key: str | None = None
    secret_key: SecretStr | None = None

    @model_validator(mode="after")
    def validate_object_store_configuration(self):
        if self.mode == "local":
            return self
        if self.mode == "object_store":
            required_fields = {
                "endpoint": self.endpoint,
                "bucket": self.bucket,
                "access_key": self.access_key,
                "secret_key": self.secret_key,
            }
            missing_fields = [field_name for field_name, value in required_fields.items() if value is None]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            return self
        return self


class LoggingSettings(AtlasBaseSettings):
    """Structured logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    format: Literal["json", "text"]
    log_directory: Path
    destination: Literal["console", "file", "both"] = "console"

class KafkaSettings(AtlasBaseSettings):
    """Kafka configuration settings."""
    bootstrap_servers: str

class CustomerSettings(AtlasBaseSettings):
    """Customer-level configuration settings."""
    customers_topic: str
    customer_addresses_topic: str
    customer_consents_topic: str

class AtlasSettings(AtlasBaseSettings):
    """Root configuration object for the Atlas platform."""
    application: ApplicationSettings
    spark: SparkSettings
    storage: StorageSettings
    logging: LoggingSettings
    kafka: KafkaSettings
    customer:CustomerSettings