from pathlib import Path


class AtlasPaths:
    """Construct and manage Atlas filesystem paths."""

    def __init__(self, lakehouse_root: Path | str, checkpoint_root: Path | str, quarantine_root: Path | str):
        """Initialize the Atlas path manager."""
        self._lakehouse_root = lakehouse_root
        self._checkpoint_root = checkpoint_root
        self._quarantine_root = quarantine_root

    def _join_path(self, root: Path | str, *parts: str) -> str:
        """Join local filesystem paths and object-store URIs."""
        if isinstance(root, Path):
            return str(root.joinpath(*parts))

        return "/".join([root.rstrip("/"), *parts])

    def _lakehouse_path(self, layer: str, domain: str) -> str:
        """Return the path for a lakehouse layer and domain."""
        return self._join_path(self._lakehouse_root, layer, domain)

    def bronze_path(self, domain: str) -> str:
        """Return the lakehouse path for the specified bronze domain."""
        return self._lakehouse_path("bronze", domain)

    def silver_path(self, domain: str) -> str:
        """Return the lakehouse path for the specified silver domain."""
        return self._lakehouse_path("silver", domain)

    def gold_path(self, domain: str) -> str:
        """Return the lakehouse path for the specified gold domain."""
        return self._lakehouse_path("gold", domain)

    def checkpoint_path(self, layer: str, name: str) -> str:
        """Return the checkpoint path for the specified domain."""
        return self._join_path(self._checkpoint_root, layer, name)

    def quarantine_path(self, name: str) -> str:
        """Return the quarantine path for the specified domain."""
        return self._join_path(self._quarantine_root, name)
