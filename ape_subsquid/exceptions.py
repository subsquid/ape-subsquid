from ape.exceptions import ApeException


class ApeSubsquidError(ApeException):
    """
    A base exception in the ape-subsquid plugin.
    """


class NotReadyToServeError(ApeSubsquidError):
    """
    Raised when archive isn't ready to serve a specific block
    or a network isn't supported.
    """


class DataIsNotAvailable(ApeSubsquidError):
    """
    Raised when a specific worker has no requested data.
    """
