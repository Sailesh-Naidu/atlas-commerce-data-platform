from atlas.common.config.models import AtlasSettings
from atlas.common.paths.loader import get_paths


def get_bronze_paths(settings: AtlasSettings, domain: str, sub_domain: str)  -> tuple[str, str]:
    """Build storage paths for the  bronze job.
    Args:
        settings: Validated Atlas application settings.
        domain: Validated Atlas domain name.
        sub_domain: Name of entity folder to write data

    Returns:
        Tuple containing the customer bronze data path and checkpoint path.



    """
    paths = get_paths(settings)
    return (paths.bronze_path(f"{domain}/cdc/{sub_domain}/job"),
            paths.checkpoint_path(f"{domain}/cdc/{sub_domain}/job"))
