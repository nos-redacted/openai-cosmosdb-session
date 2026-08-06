"""Operation-local Cosmos session-token capture."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from azure.cosmos.aio import ContainerProxy

ResponseHook = Callable[[Mapping[str, str], Any], None]


def combine_response_hooks(*hooks: ResponseHook | None) -> ResponseHook | None:
    active_hooks = tuple(hook for hook in hooks if hook is not None)
    if not active_hooks:
        return None

    def combined(headers: Mapping[str, str], result: Any) -> None:
        for hook in active_hooks:
            hook(headers, result)

    return combined


class SessionTokenCapture:
    """Collect response tokens without consulting shared client state."""

    def __init__(
        self,
        container: ContainerProxy,
        feed_range: dict[str, Any],
    ) -> None:
        self._container = container
        self._feed_range = feed_range
        self._tokens: list[str] = []

    @classmethod
    async def create(
        cls,
        container: ContainerProxy,
        session_id: str,
    ) -> SessionTokenCapture:
        feed_range = await container.feed_range_from_partition_key(session_id)
        return cls(container, feed_range)

    def __call__(self, headers: Mapping[str, str], _: Any) -> None:
        for name, value in headers.items():
            if name.lower() == "x-ms-session-token" and isinstance(value, str) and value:
                self._tokens.append(value)
                return

    async def merged_token(self) -> str | None:
        if not self._tokens:
            return None
        if len(self._tokens) == 1:
            return self._tokens[0]
        token_sources = [(self._feed_range, token) for token in self._tokens]
        try:
            return await self._container.get_latest_session_token(
                token_sources,
                self._feed_range,
            )
        except ValueError:
            return self._tokens[-1]
