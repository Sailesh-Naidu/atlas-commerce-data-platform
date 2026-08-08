import pytest

from atlas.common.config.exceptions import ConfigurationFileNotFoundError
from atlas.common.config.loader import _merge_configs, get_settings


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
