"""Backend-neutral observability contracts."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True)
class CosmosOperationEvent:
    """Sanitized, bounded-cardinality facts about one session operation."""

    operation: str
    outcome: Literal["success", "failure", "retry"]
    duration_seconds: float
    request_charge: float | None = None
    status_class: str | None = None
    retry_attempt: int | None = None
    retry_reason: str | None = None
    retry_delay_seconds: float | None = None
    throttled: bool = False
    code: str | None = None


class CosmosSessionObserver(Protocol):
    """Receives sanitized session events; implementations may be sync or async."""

    def on_event(
        self,
        event: CosmosOperationEvent,
    ) -> None | Awaitable[None]: ...
