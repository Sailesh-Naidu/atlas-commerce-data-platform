from atlas.common.config.models import AtlasSettings
from atlas.common.paths.loader import get_paths


def get_bronze_paths(settings: AtlasSettings, domain: str, sub_domain: str)  -> tuple[str, str]:
    """Build storage paths for the  bronze job.
    Args:
        settings: Validated Atlas application settings.
        domain: Validated Atlas domain name.
        sub_domain: Name of entity folder to write data

    Returns:
        Tuple containing the domains bronze data path and checkpoint path.



    """
    paths = get_paths(settings)
    return (paths.bronze_path(f"{domain}/cdc/{sub_domain}/job"),
            paths.checkpoint_path("bronze",f"{domain}/cdc/{sub_domain}/job"))

def get_silver_paths(settings: AtlasSettings, domain: str, sub_domain: str, event_type:str)  -> str:
    """Build the storage path for a Silver CDC dataset.

    Args:
        settings: Validated Atlas application settings.
        domain: Atlas domain name.
        sub_domain: Entity or sub-domain name.
        event_type: Silver CDC dataset type.

    Returns:
        Silver dataset path.
    """
    paths = get_paths(settings)
    return paths.silver_path(f"{domain}/cdc/{sub_domain}/{event_type}/job")

def get_reconciliation_paths(settings: AtlasSettings, domain: str, sub_domain:str,dataset:str) -> str:
    """Build storage paths for the  reconciliation job.
    Args:
        settings: Validated Atlas application settings.
        domain: Atlas domain name.
        sub_domain: Entity or sub-domain name.
        dataset: Name of dataset to write data
    Returns:
        Reconciliation dataset path.
        """
    paths = get_paths(settings)
    return paths.reconciliation_path(domain,sub_domain,dataset,)

def get_snapshot_paths(settings: AtlasSettings, domain: str, snapshot_as_of: str) -> str:
    """Build postgres snapshot paths
    Args:
        settings: Validated Atlas application settings.
        domain: Validated Atlas domain name.
        snapshot_as_of: snapshot as of date
    Returns:
         postgres snapshot path

    """
    paths = get_paths(settings)
    return paths.snapshot_path(f"{domain}/{snapshot_as_of}.csv",)

def get_silver_checkpoint_path(settings: AtlasSettings,domain: str,sub_domain: str,) -> str:
    """Build the checkpoint path for a Silver CDC streaming job.
    Args:
        settings: Validated Atlas application settings.
        domain: Validated Atlas domain name.
        sub_domain: Name of entity folder to write data


    Returns:
        silver checkpoint path."""

    paths = get_paths(settings)

    return paths.checkpoint_path("silver",f"{domain}/cdc/{sub_domain}/job",)

