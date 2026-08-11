class AtlasError(Exception):
    """Base exception for all Atlas-specific errors."""


class ConfigurationError(AtlasError):
    """Base exception for configuration-related errors."""


class ConfigurationFileNotFoundError(ConfigurationError):
    """Exception raised for configuration file not found"""


class ConfigurationParseError(ConfigurationError):
    """Raised when a configuration file cannot be parsed."""


class ConfigurationValidationError(ConfigurationError):
    """Exception raised for validation errors in the configuration"""
