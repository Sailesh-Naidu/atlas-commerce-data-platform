import structlog


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a logger for the given module name."""
    return structlog.get_logger(name)
