import pytest
from atlas.common.paths.loader import get_paths
from atlas.common.config.loader import get_settings
from pathlib import Path

project_root = Path(__file__).resolve().parents[4]


@pytest.fixture
def local_paths():
    atlas_settings = get_settings(
        Path(project_root / "configs" / "base.yaml"),
        Path(project_root / "configs" / "local.yaml"),
        Path(project_root / "pyproject.toml"),
    )

    local_settings = atlas_settings.model_copy(
        update={"storage": atlas_settings.storage.model_copy(update={"mode": "local"})}
    )

    return get_paths(local_settings)


@pytest.fixture
def object_store_paths():
    atlas_settings = get_settings(
        Path(project_root / "configs" / "base.yaml"),
        Path(project_root / "configs" / "local.yaml"),
        Path(project_root / "pyproject.toml"),
    )

    object_store_settings = atlas_settings.model_copy(
        update={
            "storage": atlas_settings.storage.model_copy(
                update={
                    "mode": "object_store",
                    "bucket": "atlas-lakehouse",
                }
            )
        }
    )

    return get_paths(object_store_settings)


@pytest.mark.parametrize(
    ("layer", "expected"),
    [
        ("bronze", "data/lakehouse/bronze/customer"),
        ("silver", "data/lakehouse/silver/customer"),
        ("gold", "data/lakehouse/gold/customer"),
    ],
)
def test_lakehouse_path(layer, expected, local_paths) -> None:

    path_method = getattr(local_paths, f"{layer}_path")
    actual_path = path_method("customer")
    assert actual_path == expected


def test_quarantine_path(local_paths) -> None:
    actual_path = local_paths.quarantine_path("customer")
    assert actual_path == "data/quarantine/customer"


def test_checkpoint_path(local_paths) -> None:
    actual_path = local_paths.checkpoint_path("customer")
    assert actual_path == "data/checkpoint/customer"


@pytest.mark.parametrize(
    ("layer", "expected"),
    [
        ("bronze", "s3a://atlas-lakehouse/bronze/customer"),
        ("silver", "s3a://atlas-lakehouse/silver/customer"),
        ("gold", "s3a://atlas-lakehouse/gold/customer"),
    ],
)
def test_object_store_lakehouse_path(layer, expected, object_store_paths) -> None:

    path_method = getattr(object_store_paths, f"{layer}_path")
    actual_path = path_method("customer")
    assert actual_path == expected


def test_object_store_checkpoint_path(object_store_paths) -> None:
    actual_path = object_store_paths.checkpoint_path("customer")
    assert actual_path == "s3a://atlas-lakehouse/checkpoints/customer"


def test_object_store_quarantine_path(object_store_paths) -> None:
    actual_path = object_store_paths.quarantine_path("customer")
    assert actual_path == "s3a://atlas-lakehouse/quarantine/customer"
