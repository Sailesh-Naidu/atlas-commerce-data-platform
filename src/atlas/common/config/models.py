from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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

    lakehouse_root: Path
    checkpoint_root: Path
    quarantine_root: Path


class LoggingSettings(AtlasBaseSettings):
    """Structured logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    format: Literal["json", "text"]
    log_directory: Path
    destination: Literal["console", "file", "both"] = "console"


class AtlasSettings(AtlasBaseSettings):
    """Root configuration object for the Atlas platform."""

    application: ApplicationSettings
    spark: SparkSettings
    storage: StorageSettings
    logging: LoggingSettings
