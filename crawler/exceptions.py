"""Custom exceptions for the RFB crawler."""


class RFBConnectionError(IOError):
    """Raised when a network request to the RFB portal fails or times out."""


class DiskFullError(OSError):
    """Raised when there is insufficient disk space to write a downloaded file."""


class SchemaChangeError(RuntimeError):
    """Raised when the portal's directory structure no longer matches expectations."""
