from __future__ import annotations

import unittest
from dataclasses import fields
from typing import Any

from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session import (
    CosmosDBSession,
    CosmosOperationEvent,
    _telemetry,
    _write_pipeline,
)
from openai_cosmosdb_session.exceptions import CosmosSessionOperationError


class _TelemetryDatabase:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def get_container_client(self, _: str) -> object:
        return object()

    async def read(self, **kwargs: Any) -> dict[str, str]:
        response_hook = kwargs.get("response_hook")
        if response_hook is not None:
            response_hook({"x-ms-request-charge": "0.75"}, {})
        if self.fail:
            raise CosmosHttpResponseError(
                status_code=403,
                message="SENTINEL-SERVICE-DETAIL",
            )
        return {"id": "agents"}


class _TelemetryClient:
    def __init__(self, database: _TelemetryDatabase) -> None:
        self.database = database

    def get_database_client(self, _: str) -> _TelemetryDatabase:
        return self.database


class _ChargeContainer:
    def __init__(self) -> None:
        self.incarnation = "a" * 32

    async def read_item(self, **kwargs: Any) -> dict[str, Any]:
        response_hook = kwargs.get("response_hook")
        if response_hook is not None:
            response_hook({"X-MS-REQUEST-CHARGE": "1.25"}, {})
        return {
            "id": "__session_meta__",
            "sessionId": "SENTINEL-SESSION-ID",
            "type": "meta",
            "schema_version": 2,
            "incarnation": self.incarnation,
            "last_seq": 1,
        }

    def query_items(self, **kwargs: Any) -> Any:
        response_hook = kwargs.get("response_hook")

        async def iterate() -> Any:
            if response_hook is not None:
                response_hook({"x-ms-request-charge": "2.5"}, {})
            yield {
                "id": f"{self.incarnation}:000000000000",
                "sessionId": "SENTINEL-SESSION-ID",
                "type": "message",
                "schema_version": 2,
                "incarnation": self.incarnation,
                "seq": 0,
                "message_data": '{"role":"user","content":"hello"}',
            }

        return iterate()


class _ChargeDatabase(_TelemetryDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.container = _ChargeContainer()

    def get_container_client(self, _: str) -> _ChargeContainer:
        return self.container


class _RecordingObserver:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[CosmosOperationEvent] = []
        self.fail = fail

    async def on_event(self, event: CosmosOperationEvent) -> None:
        self.events.append(event)
        if self.fail:
            raise RuntimeError("observer failed")


class _FailingAppendSession(CosmosDBSession):
    async def _append_items(self, items: list[Any], **_: Any) -> None:
        raise CosmosSessionOperationError(
            "The operation ID is already associated with another request.",
            code="operation_id_mismatch",
            mismatched_fields=("payload_fingerprint",),
        )


class TelemetryTests(unittest.IsolatedAsyncioTestCase):
    def _session(
        self,
        *,
        fail: bool = False,
        observer: Any,
    ) -> CosmosDBSession:
        return CosmosDBSession(
            "SENTINEL-SESSION-ID",
            client=_TelemetryClient(_TelemetryDatabase(fail=fail)),  # type: ignore[arg-type]
            observer=observer,
        )

    async def test_success_event_contains_only_bounded_safe_fields(self) -> None:
        observer = _RecordingObserver()

        self.assertTrue(await self._session(observer=observer).ping())

        self.assertEqual(len(observer.events), 1)
        event = observer.events[0]
        self.assertEqual(event.operation, "ping")
        self.assertEqual(event.outcome, "success")
        self.assertEqual(event.request_charge, 0.75)
        self.assertGreaterEqual(event.duration_seconds, 0)
        self.assertEqual(
            {field.name for field in fields(event)},
            {
                "operation",
                "outcome",
                "duration_seconds",
                "request_charge",
                "status_class",
                "retry_attempt",
                "retry_reason",
                "retry_delay_seconds",
                "throttled",
                "code",
            },
        )
        self.assertNotIn("SENTINEL", repr(event))

    async def test_cosmos_failure_is_classified_without_retaining_exception(self) -> None:
        observer = _RecordingObserver()

        self.assertFalse(await self._session(fail=True, observer=observer).ping())

        event = observer.events[0]
        self.assertEqual(event.outcome, "failure")
        self.assertEqual(event.request_charge, 0.75)
        self.assertEqual(event.status_class, "4xx")
        self.assertEqual(event.code, "cosmos_http_error")
        self.assertNotIn("SENTINEL", repr(event))
        self.assertNotIn("SENTINEL", repr(vars(event)))

    async def test_observer_failure_cannot_fail_storage_operation(self) -> None:
        observer = _RecordingObserver(fail=True)

        self.assertTrue(await self._session(observer=observer).ping())
        self.assertEqual(len(observer.events), 1)

    async def test_append_failure_emits_stable_code_without_payload(self) -> None:
        observer = _RecordingObserver()
        session = _FailingAppendSession(
            "SENTINEL-SESSION-ID",
            client=_TelemetryClient(_TelemetryDatabase()),  # type: ignore[arg-type]
            observer=observer,
        )

        with self.assertRaises(CosmosSessionOperationError):
            await session.add_items(
                [{"role": "user", "content": "SENTINEL-MESSAGE-CONTENT"}]
            )

        event = observer.events[0]
        self.assertEqual(event.operation, "add_items")
        self.assertEqual(event.outcome, "failure")
        self.assertEqual(event.code, "operation_id_mismatch")
        self.assertNotIn("SENTINEL", repr(event))

    async def test_retry_event_is_bounded_and_observer_failure_is_swallowed(self) -> None:
        observer = _RecordingObserver(fail=True)
        attempts = 0

        async def create_document(_: dict[str, Any]) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                error = CosmosHttpResponseError(status_code=429, message="SENTINEL")
                error.headers = {"x-ms-retry-after-ms": "25"}
                raise error

        async def on_retry(error: BaseException, attempt: int, delay: float) -> None:
            await _telemetry.emit_event(
                observer,
                _telemetry.retry_event(
                    operation="write_documents",
                    error=error,
                    retry_attempt=attempt,
                    retry_delay_seconds=delay,
                ),
            )

        await _write_pipeline.write_reserved_documents_with_retry(
            documents=({},),
            max_write_attempts=2,
            delay_seconds=lambda _error, _attempt: 0.025,
            create_document=create_document,
            sleep=_no_sleep,
            on_retry=on_retry,
        )

        event = observer.events[0]
        self.assertEqual(event.outcome, "retry")
        self.assertEqual(event.retry_attempt, 1)
        self.assertEqual(event.retry_reason, "throttled")
        self.assertEqual(event.retry_delay_seconds, 0.025)
        self.assertTrue(event.throttled)
        self.assertNotIn("SENTINEL", repr(event))

    async def test_read_event_sums_operation_local_request_charge(self) -> None:
        observer = _RecordingObserver()
        session = CosmosDBSession(
            "SENTINEL-SESSION-ID",
            client=_TelemetryClient(_ChargeDatabase()),  # type: ignore[arg-type]
            observer=observer,
        )

        self.assertEqual(
            await session.get_items(),
            [{"role": "user", "content": "hello"}],
        )

        event = observer.events[0]
        self.assertEqual(event.operation, "get_items")
        self.assertEqual(event.request_charge, 3.75)
        self.assertNotIn("SENTINEL", repr(event))

    async def test_request_charge_collector_ignores_malformed_values(self) -> None:
        metrics = _telemetry.ResponseMetricsCapture()

        for value in ("invalid", "nan", "inf", "-1"):
            metrics({"x-ms-request-charge": value}, {})
        metrics({"x-ms-request-charge": "1.5"}, {})

        self.assertEqual(metrics.request_charge, 1.5)


async def _no_sleep(_: float) -> None:
    return


if __name__ == "__main__":
    unittest.main()
