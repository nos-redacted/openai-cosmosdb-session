from __future__ import annotations


class CosmosSessionError(RuntimeError):
    """Base exception for Cosmos DB session failures."""


class CosmosSessionConflictError(CosmosSessionError):
    """Raised when a reserved message ID belongs to a different document."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        mismatched_fields: tuple[str, ...],
    ) -> None:
        super().__init__(message)
        self.code = code
        self.mismatched_fields = mismatched_fields


class CosmosSessionCorruptDocumentError(CosmosSessionError):
    """Raised when a stored session message fails schema or payload validation."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        invalid_fields: tuple[str, ...],
    ) -> None:
        super().__init__(message)
        self.code = code
        self.invalid_fields = invalid_fields


class CosmosSessionOperationError(CosmosSessionError):
    """Raised when an operation ID cannot be safely resumed."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        mismatched_fields: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.mismatched_fields = mismatched_fields


class CosmosSessionConfigurationError(CosmosSessionError):
    """Raised when a session or Cosmos container configuration is invalid."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        invalid_fields: tuple[str, ...],
    ) -> None:
        super().__init__(message)
        self.code = code
        self.invalid_fields = invalid_fields
