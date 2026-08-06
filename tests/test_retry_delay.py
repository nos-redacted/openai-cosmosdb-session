from __future__ import annotations

import unittest
from datetime import datetime, timezone

from azure.core.exceptions import ServiceRequestError
from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session import _write_pipeline
from openai_cosmosdb_session._retry import RetryDelayPolicy


def _cosmos_error(headers: dict[str, str]) -> CosmosHttpResponseError:
    error = CosmosHttpResponseError(status_code=429, message="throttled")
    error.headers = headers
    return error


class RetryDelayPolicyTests(unittest.TestCase):
    def test_millisecond_hint_takes_precedence_and_adds_bounded_jitter(self) -> None:
        policy = RetryDelayPolicy(
            base_delay_seconds=0.2,
            max_delay_seconds=5,
            random_value=lambda: 0.5,
        )
        error = _cosmos_error(
            {"x-ms-retry-after-ms": "1500", "Retry-After": "9"}
        )

        self.assertEqual(policy.delay_seconds(error, retry_number=0), 1.6)

    def test_retry_after_supports_seconds_and_http_date(self) -> None:
        now = datetime(2025, 1, 1, tzinfo=timezone.utc)
        seconds_policy = RetryDelayPolicy(
            base_delay_seconds=1,
            max_delay_seconds=10,
            random_value=lambda: 0.25,
            now=lambda: now,
        )
        date_policy = RetryDelayPolicy(
            base_delay_seconds=0,
            max_delay_seconds=10,
            random_value=lambda: 1,
            now=lambda: now,
        )

        self.assertEqual(
            seconds_policy.delay_seconds(
                _cosmos_error({"Retry-After": "2"}),
                retry_number=1,
            ),
            2.5,
        )
        self.assertEqual(
            date_policy.delay_seconds(
                _cosmos_error({"Retry-After": "Wed, 01 Jan 2025 00:00:03 GMT"}),
                retry_number=0,
            ),
            3,
        )

    def test_malformed_headers_fall_back_to_client_jitter(self) -> None:
        policy = RetryDelayPolicy(
            base_delay_seconds=2,
            max_delay_seconds=10,
            random_value=lambda: 0.5,
        )
        error = _cosmos_error(
            {"x-ms-retry-after-ms": "invalid", "Retry-After": "also-invalid"}
        )

        self.assertEqual(policy.delay_seconds(error, retry_number=0), 1)

    def test_server_hint_is_never_capped_below_its_value(self) -> None:
        policy = RetryDelayPolicy(
            base_delay_seconds=10,
            max_delay_seconds=2,
            random_value=lambda: 1,
        )

        self.assertEqual(
            policy.delay_seconds(
                _cosmos_error({"x-ms-retry-after-ms": "5000"}),
                retry_number=4,
            ),
            5,
        )


class RetryLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_transport_failure_uses_attempt_numbers_and_never_sleeps_last(self) -> None:
        attempts = 0
        retry_numbers: list[int] = []
        delays: list[float] = []

        async def create_document(_: dict[str, object]) -> None:
            nonlocal attempts
            attempts += 1
            raise ServiceRequestError("offline")

        def delay_seconds(_: BaseException, retry_number: int) -> float:
            retry_numbers.append(retry_number)
            return float(retry_number + 1)

        async def sleep(delay: float) -> None:
            delays.append(delay)

        with self.assertRaises(ServiceRequestError):
            await _write_pipeline.write_reserved_documents_with_retry(
                documents=({},),
                max_write_attempts=3,
                delay_seconds=delay_seconds,
                sleep=sleep,
                create_document=create_document,
            )

        self.assertEqual(attempts, 3)
        self.assertEqual(retry_numbers, [0, 1])
        self.assertEqual(delays, [1, 2])


if __name__ == "__main__":
    unittest.main()
