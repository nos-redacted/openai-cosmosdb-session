"""Retry timing for safe deterministic document replays."""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


@dataclass(frozen=True)
class RetryDelayPolicy:
    """Combine server retry hints with a bounded full-jitter client delay."""

    base_delay_seconds: float
    max_delay_seconds: float
    random_value: Callable[[], float] = field(
        default=random.random,
        repr=False,
        compare=False,
    )
    now: Callable[[], datetime] = field(
        default=lambda: datetime.now(timezone.utc),
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must not be negative")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds must not be negative")

    def delay_seconds(self, error: BaseException, retry_number: int) -> float:
        if retry_number < 0:
            raise ValueError("retry_number must not be negative")

        server_hint = _server_retry_hint_seconds(error, now=self.now)
        try:
            exponential_cap = math.ldexp(self.base_delay_seconds, retry_number)
        except OverflowError:
            exponential_cap = self.max_delay_seconds
        client_cap = min(exponential_cap, self.max_delay_seconds)
        remaining_below_max = max(0.0, self.max_delay_seconds - server_hint)
        jitter_cap = min(client_cap, remaining_below_max)
        random_fraction = min(1.0, max(0.0, self.random_value()))
        return server_hint + (random_fraction * jitter_cap)


def _server_retry_hint_seconds(
    error: BaseException,
    *,
    now: Callable[[], datetime],
) -> float:
    headers = getattr(error, "headers", None)
    if not isinstance(headers, Mapping):
        return 0.0

    retry_after_ms = _get_header(headers, "x-ms-retry-after-ms")
    if retry_after_ms is not None:
        try:
            milliseconds = float(retry_after_ms)
        except (TypeError, ValueError):
            pass
        else:
            if milliseconds >= 0:
                return milliseconds / 1000.0

    retry_after = _get_header(headers, "retry-after")
    if retry_after is None:
        return 0.0
    try:
        seconds = float(retry_after)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(retry_after)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - now()).total_seconds())
    return max(0.0, seconds)


def _get_header(headers: Mapping[object, object], expected_name: str) -> str | None:
    for name, value in headers.items():
        if (
            isinstance(name, str)
            and name.lower() == expected_name
            and isinstance(value, str)
        ):
            return value
    return None
