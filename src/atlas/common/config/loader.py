import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


from atlas.common.config.exceptions import (
    ConfigurationFileNotFoundError,
    ConfigurationParseError,
    ConfigurationValidationError,
)
from atlas.common.config.models import AtlasSettings

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

project_root = Path(__file__).resolve().parents[4]


def _load_yaml(yaml_file: Path) -> dict[str, Any]:
    """Load and parse an Atlas YAML configuration file.

    Args:
        yaml_file: Path to the YAML configuration file.

    Returns:
        Parsed configuration as a nested dictionary.

    Raises:
        ConfigurationFileNotFoundError:
            If the configuration file does not exist.

        ConfigurationParseError:
            If the file contains invalid YAML or does not contain a mapping.
    """
    if not yaml_file.exists():
        raise ConfigurationFileNotFoundError(f"configuration file not found: {yaml_file}")
    try:
        with yaml_file.open("r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
            if not isinstance(config, dict):
                raise ConfigurationParseError(f"Configuration file must contain a YAML mapping: {yaml_file}")
            return config

    except yaml.YAMLError as error:
        raise ConfigurationParseError(f"Configuration file is not valid YAML: {yaml_file}") from error


def _merge_configs(base_config: dict[str, Any], env_config: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge environment settings over base settings.

    Nested dictionaries are merged recursively. For all other values,
    the environment-specific value replaces the base value.

    Args:
        base_config: Shared configuration values used as defaults.
        env_config: Environment-specific values that override the defaults.

    Returns:
        A new dictionary containing the merged configuration.
    """
    merged_config = base_config.copy()
    for key, env_value in env_config.items():
        base_value = merged_config.get(key)
        if isinstance(base_value, dict) and isinstance(env_value, dict):
            merged_config[key] = _merge_configs(base_value, env_value)
        else:
            merged_config[key] = env_value
    return merged_config


def _load_project_metadata(pyproject_path: Path) -> dict[str, str]:
    """Load project metadata from pyproject.toml file.
    Args:
        pyproject_path: Path to the pyproject.toml file.

    Returns:
        A dictionary containing the project metadata.

    Raises:
        ConfigurationFileNotFoundError:
            If the configuration file does not exist.

        ConfigurationParseError:
            If the file contains invalid TOML or does not contain a mapping.
    """
    if not pyproject_path.exists():
        raise ConfigurationFileNotFoundError(f"configuration file not found: {pyproject_path}")
    try:
        with pyproject_path.open("rb") as stream:
            toml_data = tomllib.load(stream)
            if not isinstance(toml_data, dict):
                raise ConfigurationParseError(f"Configuration file must contain a toml mapping: {pyproject_path}")
            project_metadata = {"version": toml_data["project"]["version"]}
            return project_metadata

    except tomllib.TOMLDecodeError as error:
        raise ConfigurationParseError(f"Configuration file is not valid TOML: {pyproject_path}") from error

    except KeyError as error:
        raise ConfigurationParseError(f"Required project metadata is missing: {error}") from error

class _EnvironmentSecrets(BaseSettings):
    """Secrets loaded from the local environment file or process environment."""

    model_config = SettingsConfigDict(extra="ignore")

    minio_access_key: str = Field(
        validation_alias="MINIO_ROOT_USER"
    )
    minio_secret_key: SecretStr = Field(
        validation_alias="MINIO_ROOT_PASSWORD"
    )

@lru_cache(maxsize=8)
def _get_settings_cached(base_yaml: Path, env_yaml: Path, pyproject_path: Path) -> AtlasSettings:
    """Load, merge, validate, and cache Atlas configuration.

    Args:
        base_yaml: Path to the shared base configuration.
        env_yaml: Path to the environment-specific configuration.
        pyproject_path: Path to the pyproject.toml file.

    Returns:
        Validated and immutable Atlas settings.

    Raises:
        ConfigurationError:
            If either file cannot be loaded or parsed.

        ConfigurationValidationError:
            If the merged configuration violates the Atlas settings contract.

    """
    project_metadata = _load_project_metadata(pyproject_path)
    base_config = _load_yaml(base_yaml)
    env_config = _load_yaml(env_yaml)
    atlas_config = _merge_configs(base_config, env_config)
    storage_mode = atlas_config.get("storage", {}).get("mode", "local")
    if storage_mode == "object_store":
        environment_secrets = _EnvironmentSecrets(
            _env_file=pyproject_path.parent / ".env",
            _env_file_encoding="utf-8",
        )
        atlas_config["storage"]["access_key"] = environment_secrets.minio_access_key
        atlas_config["storage"]["secret_key"] = environment_secrets.minio_secret_key
    atlas_config["application"]["app_version"] = project_metadata["version"]
    try:
        return AtlasSettings(**atlas_config)
    except ValidationError as error:
        error_messages = [f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}" for item in error.errors()]
        raise ConfigurationValidationError(
            "Configuration values failed validation:\n" + "\n".join(error_messages)
        ) from error


def get_settings(base_yaml: str | Path, env_yaml: str | Path, pyproject_path: str | Path) -> AtlasSettings:
    """Return cached Atlas settings for the supplied configuration files.

    The supplied paths are normalized to absolute paths before being used
    as cache keys.

    Args:
        base_yaml: Path to the shared base configuration.
        env_yaml: Path to the environment-specific configuration.
        pyproject_path: Path to the pyproject.toml file.

    Returns:
        Validated and immutable Atlas settings.

    """
    return _get_settings_cached(Path(base_yaml).resolve(), Path(env_yaml).resolve(), Path(pyproject_path).resolve())

