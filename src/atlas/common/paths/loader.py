from functools import lru_cache

from atlas.common.config.models import AtlasSettings
from atlas.common.paths.paths import AtlasPaths


@lru_cache(maxsize=8)
def get_paths(settings: AtlasSettings) -> AtlasPaths:
    """Return the cached Atlas paths manager.

    Args:
        settings: Validated Atlas application settings.

    Returns:
        Cached Atlas paths manager.
    """
    return AtlasPaths(
        lakehouse_root=settings.storage.lakehouse_root,
        checkpoint_root=settings.storage.checkpoint_root,
        quarantine_root=settings.storage.quarantine_root,
    )
