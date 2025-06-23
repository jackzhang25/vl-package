"""Custom exceptions for the Visual Layer SDK."""


class VisualLayerException(Exception):
    """Base exception for Visual Layer SDK errors."""

    pass


class AuthenticationError(VisualLayerException):
    """Raised when authentication fails."""

    pass


class APIError(VisualLayerException):
    """Raised when the API returns an error."""

    pass


class ValidationError(VisualLayerException):
    """Raised when input validation fails."""

    pass
