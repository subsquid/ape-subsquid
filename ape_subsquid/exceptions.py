from ape.exceptions import ApeException


class ApeSubsquidError(ApeException):
    """
    A base exception in the ape-subsquid plugin.
    """


class NotReadyToServeError(ApeSubsquidError):
    """
    Raised when subsquid network isn't ready to serve a specific block.
    """


class DataIsNotAvailable(ApeSubsquidError):
    """
    Raised when a specific worker has no requested data.
    """


class DataRangeIsNotAvailable(ApeSubsquidError):
    """
    Raised when subsquid network doesn't cover the requested data range.
    """

    def __init__(self, range: tuple[int, int], height: int) -> None:
        super().__init__(f"Range {range} isn't covered. Last available block is {height}.")
