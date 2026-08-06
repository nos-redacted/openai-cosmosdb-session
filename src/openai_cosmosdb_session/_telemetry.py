"""Safe event construction and observer dispatch."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from math import isfinite
from time import monotonic
from typing import Any, TypeVar

from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.cosmos.exceptions import CosmosHttpResponseError

from .exceptions import CosmosSessionError
from .telemetry import CosmosOperationEvent, CosmosSessionObserver

T = TypeVar("T")


class ResponseMetricsCapture:
    """Collect numeric response metrics without retaining headers or bodies."""

    def __init__(self) -> None:
        self._request_charge = 0.0
        self._has_request_charge = False

    def __call__(self, headers: Mapping[str, str], _: Any) -> None:
        for name, value in headers.items():
            if name.lower() != "x-ms-request-charge":
                continue
            try:
                request_charge = float(value)
            except (TypeError, ValueError):
                return
            if isfinite(request_charge) and request_charge >= 0:
                self._request_charge += request_charge
                self._has_request_charge = True
            return

    @property
    def request_charge(self) -> float | None:
        if not self._has_request_charge:
            return None
        return self._request_charge


async def emit_event(
    observer: CosmosSessionObserver | None,
    event: CosmosOperationEvent,
) -> None:
    if observer is None:
        return
    try:
        result = observer.on_event(event)
        if inspect.isawaitable(result):
            await result
    except Exception:
        return


async def observe_operation(
    observer: CosmosSessionObserver | None,
    *,
    operation: str,
    action: Callable[[], Awaitable[T]],
    metrics: ResponseMetricsCapture | None = None,
) -> T:
    started_at = monotonic()
    try:
        result = await action()
    except Exception as error:
        await emit_event(
            observer,
            failure_event(
                operation=operation,
                duration_seconds=monotonic() - started_at,
                error=error,
                request_charge=(metrics.request_charge if metrics is not None else None),
            ),
        )
        raise
    await emit_event(
        observer,
        CosmosOperationEvent(
            operation=operation,
            outcome="success",
            duration_seconds=monotonic() - started_at,
            request_charge=(metrics.request_charge if metrics is not None else None),
        ),
    )
    return result


def failure_event(
    *,
    operation: str,
    duration_seconds: float,
    error: BaseException,
    request_charge: float | None = None,
) -> CosmosOperationEvent:
    status_code = getattr(error, "status_code", None)
    status_class = (
        f"{status_code // 100}xx"
        if isinstance(status_code, int) and 100 <= status_code <= 599
        else None
    )
    if isinstance(error, CosmosSessionError):
        code = getattr(error, "code", "session_error")
    elif isinstance(error, CosmosHttpResponseError):
        code = "cosmos_http_error"
    elif isinstance(error, (ServiceRequestError, ServiceResponseError)):
        code = "cosmos_transport_error"
    else:
        code = "unexpected_error"
    return CosmosOperationEvent(
        operation=operation,
        outcome="failure",
        duration_seconds=max(0.0, duration_seconds),
        request_charge=request_charge,
        status_class=status_class,
        throttled=status_code == 429,
        code=code,
    )


def retry_event(
    *,
    operation: str,
    error: BaseException,
    retry_attempt: int,
    retry_delay_seconds: float,
) -> CosmosOperationEvent:
    status_code = getattr(error, "status_code", None)
    if status_code == 429:
        reason = "throttled"
    elif status_code == 408:
        reason = "timeout"
    elif isinstance(status_code, int) and 500 <= status_code <= 599:
        reason = "server_error"
    elif isinstance(error, (ServiceRequestError, ServiceResponseError)):
        reason = "transport_error"
    else:
        reason = "retryable_error"
    return CosmosOperationEvent(
        operation=operation,
        outcome="retry",
        duration_seconds=0.0,
        status_class=(
            f"{status_code // 100}xx"
            if isinstance(status_code, int) and 100 <= status_code <= 599
            else None
        ),
        retry_attempt=retry_attempt,
        retry_reason=reason,
        retry_delay_seconds=max(0.0, retry_delay_seconds),
        throttled=status_code == 429,
        code="write_retry",
    )
