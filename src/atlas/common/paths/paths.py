from pathlib import Path


class AtlasPaths:
    """Construct and manage Atlas filesystem paths."""

    def __init__(self, lakehouse_root: Path, checkpoint_root: Path, quarantine_root: Path):
        """Initialize the Atlas path manager."""
        self._lakehouse_root = lakehouse_root
        self._checkpoint_root = checkpoint_root
        self._quarantine_root = quarantine_root

    def _lakehouse_path(self, layer: str, domain: str) -> Path:
        """Return the path for a lakehouse layer and domain."""
        return self._lakehouse_root / layer / domain

    def bronze_path(self, domain: str) -> Path:
        """Return the lakehouse path for the specified bronze domain."""
        return self._lakehouse_path("bronze", domain)

    def silver_path(self, domain: str) -> Path:
        """Return the lakehouse path for the specified silver domain."""
        return self._lakehouse_path("silver", domain)

    def gold_path(self, domain: str) -> Path:
        """Return the lakehouse path for the specified gold domain."""
        return self._lakehouse_path("gold", domain)

    def checkpoint_path(self, name: str) -> Path:
        """Return the checkpoint path for the specified domain."""
        return self._checkpoint_root / name

    def quarantine_path(self, name: str) -> Path:
        """Return the quarantine path for the specified domain."""
        return self._quarantine_root / name
