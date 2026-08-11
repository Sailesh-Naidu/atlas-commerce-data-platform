from pathlib import Path

import pytest

from atlas.common.config.exceptions import ConfigurationFileNotFoundError
from atlas.common.config.loader import _merge_configs, get_settings
from atlas.common.config.exceptions import ConfigurationValidationError


def test_merge_configs_recursively_overrides_environment_values() -> None:
    """Environment values should override base values without removing defaults."""
    base_config = {
        "spark": {
            "shuffle_partitions": 200,
            "session_timezone": "UTC",
        }
    }

    env_config = {
        "spark": {
            "shuffle_partitions": 8,
        }
    }

    expected = {
        "spark": {
            "shuffle_partitions": 8,
            "session_timezone": "UTC",
        }
    }

    result = _merge_configs(base_config, env_config)

    assert result == expected


def test_merge_configs_does_not_modify_input_dictionaries() -> None:
    """Merging configurations should not mutate the input dictionaries."""
    base_config = {
        "spark": {
            "shuffle_partitions": 200,
        }
    }

    env_config = {
        "spark": {
            "shuffle_partitions": 8,
        }
    }

    _merge_configs(base_config, env_config)

    assert base_config == {
        "spark": {
            "shuffle_partitions": 200,
        }
    }

    assert env_config == {
        "spark": {
            "shuffle_partitions": 8,
        }
    }


def test_get_settings_raises_when_base_config_is_missing() -> None:
    """Loading settings should fail if the base configuration file is missing."""
    with pytest.raises(ConfigurationFileNotFoundError):
        get_settings(
            "missing.yaml",
            "local.yaml",
            "pyproject.toml",
        )


def _write_test_configs(
    tmp_path: Path,
    storage_mode: str,
    bucket: str | None = "atlas-lakehouse",
) -> tuple[Path, Path, Path]:

    base_path = tmp_path / "base.yaml"
    local_path = tmp_path / "local.yaml"
    pyproject_path = tmp_path / "pyproject.toml"

    base_path.write_text(
        """
application:
  name: Atlas

spark:
  session_timezone: UTC
""".strip()
    )

    local_path.write_text(
        f"""
application:
  environment: local

spark:
  master: local[*]
  shuffle_partitions: 8
  broadcast_size_mb: 20
  adaptive_query_execution: true

storage:
  mode: {storage_mode}
  lakehouse_root: data/lakehouse
  checkpoint_root: data/checkpoint
  quarantine_root: data/quarantine
  endpoint: http://localhost:9000
  bucket: {bucket}

logging:
  level: INFO
  format: text
  log_directory: ./log
  destination: console
""".strip()
    )

    pyproject_path.write_text(
        """
[project]
name = "atlas"
version = "0.1.0"
""".strip()
    )

    return base_path, local_path, pyproject_path


def test_local_storage_does_not_require_object_store_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)

    base_path, local_path, pyproject_path = _write_test_configs(
        tmp_path,
        "local",
    )

    settings = get_settings(
        base_path,
        local_path,
        pyproject_path,
    )

    assert settings.storage.mode == "local"
    assert settings.storage.access_key is None
    assert settings.storage.secret_key is None


def test_object_store_loads_credentials_from_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "atlas_test")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test_password")

    base_path, local_path, pyproject_path = _write_test_configs(
        tmp_path,
        "object_store",
    )

    settings = get_settings(
        base_path,
        local_path,
        pyproject_path,
    )

    assert settings.storage.mode == "object_store"
    assert settings.storage.access_key == "atlas_test"
    assert settings.storage.secret_key is not None
    assert settings.storage.secret_key.get_secret_value() == "test_password"


def test_object_store_requires_bucket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MINIO_ROOT_USER", "atlas_test")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test_password")

    base_path, local_path, pyproject_path = _write_test_configs(
        tmp_path,
        "object_store",
        bucket="",
    )

    with pytest.raises(
        ConfigurationValidationError,
        match="bucket",
    ):
        get_settings(
            base_path,
            local_path,
            pyproject_path,
        )
