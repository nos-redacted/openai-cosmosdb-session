from __future__ import annotations

import unittest
from typing import Any

from openai_cosmosdb_session._tokens import SessionTokenCapture


class _FakeTokenContainer:
    def __init__(self) -> None:
        self.feed_range = {"min": "AA", "max": "BB"}
        self.merge_calls: list[tuple[list[tuple[dict[str, Any], str]], dict[str, Any]]] = []

    async def feed_range_from_partition_key(self, partition_key: str) -> dict[str, Any]:
        self.partition_key = partition_key
        return self.feed_range

    async def get_latest_session_token(
        self,
        feed_ranges_to_session_tokens: list[tuple[dict[str, Any], str]],
        target_feed_range: dict[str, Any],
    ) -> str:
        self.merge_calls.append((feed_ranges_to_session_tokens, target_feed_range))
        return "merged-token"


class SessionTokenCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_captures_case_insensitively_and_merges_with_feed_range(self) -> None:
        container = _FakeTokenContainer()
        capture = await SessionTokenCapture.create(container, "session-1")  # type: ignore[arg-type]

        capture({"X-MS-SESSION-TOKEN": "token-1"}, {})
        capture({"x-ms-session-token": "token-2"}, {})

        self.assertEqual(await capture.merged_token(), "merged-token")
        self.assertEqual(container.partition_key, "session-1")
        self.assertEqual(
            container.merge_calls,
            [
                (
                    [
                        (container.feed_range, "token-1"),
                        (container.feed_range, "token-2"),
                    ],
                    container.feed_range,
                )
            ],
        )

    async def test_missing_session_token_returns_none_without_merging(self) -> None:
        container = _FakeTokenContainer()
        capture = await SessionTokenCapture.create(container, "session-1")  # type: ignore[arg-type]

        capture({"x-ms-request-charge": "1.0"}, {})

        self.assertIsNone(await capture.merged_token())
        self.assertEqual(container.merge_calls, [])


if __name__ == "__main__":
    unittest.main()
