from __future__ import annotations

import asyncio
import copy
import unittest
from collections.abc import Sequence
from typing import Any

from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session._write_pipeline import SequenceReservation
from openai_cosmosdb_session.exceptions import (
    CosmosSessionConflictError,
    CosmosSessionOperationError,
)
from openai_cosmosdb_session.session import CosmosDBSession

TEST_INCARNATION = "a" * 32


class _FakeContainer:
    def __init__(self, *, transient_seq: int | None = 11) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.create_calls: list[dict[str, Any]] = []
        self.patch_calls = 0
        self.patch_kwargs: list[dict[str, Any]] = []
        self.failed_once = False
        self.transient_seq = transient_seq
        self.fail_next_message_status: int | None = None
        self.fail_patch_after_increment_status: int | None = None
        self.fail_next_batch_before_status: int | None = None
        self.fail_next_batch_after_status: int | None = None
        self.fail_operation_create_after_status: int | None = None
        self.fail_completion_after_status: int | None = None
        self.batch_calls = 0
        self.batch_commits = 0
        self.etag = 0

    def _next_etag(self) -> str:
        self.etag += 1
        return str(self.etag)

    async def create_item(self, *, body: dict[str, Any], **_: Any) -> dict[str, Any]:
        self.create_calls.append(body)
        if body.get("type") == "message" and self.fail_next_message_status is not None:
            status_code = self.fail_next_message_status
            self.fail_next_message_status = None
            raise CosmosHttpResponseError(status_code=status_code, message="write failed")
        if (
            body.get("type") == "message"
            and body["seq"] == self.transient_seq
            and not self.failed_once
        ):
            self.failed_once = True
            raise CosmosHttpResponseError(status_code=503, message="transient")
        if body["id"] in self.items:
            raise CosmosHttpResponseError(status_code=409, message="conflict")
        self.items[body["id"]] = copy.deepcopy(body)
        self.items[body["id"]]["_etag"] = self._next_etag()
        if (
            body.get("type") == "operation"
            and self.fail_operation_create_after_status is not None
        ):
            status_code = self.fail_operation_create_after_status
            self.fail_operation_create_after_status = None
            raise CosmosHttpResponseError(
                status_code=status_code,
                message="ambiguous operation create",
            )
        return copy.deepcopy(self.items[body["id"]])

    async def read_item(
        self,
        *,
        item: str,
        partition_key: str,
        **_: Any,
    ) -> dict[str, Any]:
        assert partition_key == "session-1"
        if item not in self.items:
            raise CosmosHttpResponseError(status_code=404, message="missing")
        return copy.deepcopy(self.items[item])

    async def patch_item(
        self,
        *,
        item: str,
        partition_key: str,
        patch_operations: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        assert partition_key == "session-1"
        self.patch_calls += 1
        self.patch_kwargs.append(kwargs)
        if item not in self.items:
            raise CosmosHttpResponseError(status_code=404, message="missing")
        metadata = self.items[item]
        for operation in patch_operations:
            field = operation["path"].removeprefix("/")
            if operation["op"] == "incr":
                metadata[field] += operation["value"]
            else:
                metadata[field] = operation["value"]
        if self.fail_patch_after_increment_status is not None:
            status_code = self.fail_patch_after_increment_status
            self.fail_patch_after_increment_status = None
            raise CosmosHttpResponseError(status_code=status_code, message="ambiguous patch")
        metadata["_etag"] = self._next_etag()
        if (
            metadata.get("type") == "operation"
            and metadata.get("status") == "completed"
            and self.fail_completion_after_status is not None
        ):
            status_code = self.fail_completion_after_status
            self.fail_completion_after_status = None
            raise CosmosHttpResponseError(
                status_code=status_code,
                message="ambiguous completion",
            )
        return copy.deepcopy(metadata)

    async def execute_item_batch(
        self,
        *,
        batch_operations: list[Any],
        partition_key: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        assert partition_key == "session-1"
        self.batch_calls += 1
        if self.fail_next_batch_before_status is not None:
            status_code = self.fail_next_batch_before_status
            self.fail_next_batch_before_status = None
            raise CosmosHttpResponseError(status_code=status_code, message="batch failed")
        snapshot = copy.deepcopy(self.items)
        try:
            for operation_type, arguments, options in batch_operations:
                assert operation_type == "patch"
                item_id, patch_operations = arguments
                document = self.items[item_id]
                if document.get("_etag") != options["if_match_etag"]:
                    raise CosmosHttpResponseError(status_code=412, message="stale")
                for operation in patch_operations:
                    field = operation["path"].removeprefix("/")
                    if operation["op"] == "incr":
                        document[field] += operation["value"]
                    else:
                        document[field] = operation["value"]
                document["_etag"] = self._next_etag()
        except Exception:
            self.items = snapshot
            raise
        self.batch_commits += 1
        if self.fail_next_batch_after_status is not None:
            status_code = self.fail_next_batch_after_status
            self.fail_next_batch_after_status = None
            raise CosmosHttpResponseError(status_code=status_code, message="ambiguous batch")
        return []

    def query_items(
        self,
        *,
        query: str,
        parameters: list[dict[str, Any]],
        partition_key: str,
        **_: Any,
    ) -> Any:
        assert partition_key == "session-1"
        parameter_values = {
            parameter["name"]: parameter["value"] for parameter in parameters
        }
        values = [
            copy.deepcopy(item)
            for item in self.items.values()
            if item.get("type") == "message"
            and item.get("incarnation") == parameter_values["@incarnation"]
        ]
        values.sort(key=lambda item: item["seq"], reverse="DESC" in query)

        async def iterate() -> Any:
            for value in values:
                yield value

        return iterate()

    async def feed_range_from_partition_key(self, partition_key: str) -> dict[str, str]:
        assert partition_key == "session-1"
        return {"min": "AA", "max": "BB"}

    async def get_latest_session_token(
        self,
        feed_ranges_to_session_tokens: list[tuple[dict[str, Any], str]],
        target_feed_range: dict[str, Any],
    ) -> str:
        assert target_feed_range == {"min": "AA", "max": "BB"}
        return feed_ranges_to_session_tokens[-1][1]


class _ConcurrentBatchContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__(transient_seq=None)
        self.batch_waiters = 0
        self.batches_ready = asyncio.Event()
        self.release_batches = asyncio.Event()

    async def execute_item_batch(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.batch_waiters += 1
        if self.batch_waiters == 2:
            self.batches_ready.set()
        await self.release_batches.wait()
        return await super().execute_item_batch(**kwargs)


class _TokenResponseContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__(transient_seq=None)
        self.response_number = 0
        self.merge_inputs: list[str] = []

    def _respond(self, response_hook: Any) -> None:
        if response_hook is None:
            return
        self.response_number += 1
        response_hook(
            {"x-ms-session-token": f"token-{self.response_number}"},
            {},
        )

    async def feed_range_from_partition_key(self, partition_key: str) -> dict[str, str]:
        assert partition_key == "session-1"
        return {"min": "AA", "max": "BB"}

    async def get_latest_session_token(
        self,
        feed_ranges_to_session_tokens: list[tuple[dict[str, Any], str]],
        target_feed_range: dict[str, Any],
    ) -> str:
        assert target_feed_range == {"min": "AA", "max": "BB"}
        self.merge_inputs = [token for _, token in feed_ranges_to_session_tokens]
        return "merged-token"

    async def create_item(self, **kwargs: Any) -> dict[str, Any]:
        result = await super().create_item(**kwargs)
        self._respond(kwargs.get("response_hook"))
        return result

    async def patch_item(self, **kwargs: Any) -> dict[str, Any]:
        result = await super().patch_item(**kwargs)
        self._respond(kwargs.get("response_hook"))
        return result

    async def read_item(self, **kwargs: Any) -> dict[str, Any]:
        result = await super().read_item(**kwargs)
        self._respond(kwargs.get("response_hook"))
        return result

    async def execute_item_batch(self, **kwargs: Any) -> list[dict[str, Any]]:
        result = await super().execute_item_batch(**kwargs)
        self._respond(kwargs.get("response_hook"))
        return result


class _FakeDatabase:
    def __init__(self, container: _FakeContainer) -> None:
        self.container = container

    def get_container_client(self, _: str) -> _FakeContainer:
        return self.container


class _FakeClient:
    def __init__(self, container: _FakeContainer) -> None:
        self.container = container

    def get_database_client(self, _: str) -> _FakeDatabase:
        return _FakeDatabase(self.container)


class _ObservedSession(CosmosDBSession):
    def __init__(self, container: _FakeContainer) -> None:
        super().__init__(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        self.serialize_calls = 0
        self.reserve_calls = 0
        self.events: list[str] = []

    async def _serialize_item(self, item: Any) -> str:
        self.serialize_calls += 1
        self.events.append("serialize")
        return await super()._serialize_item(item)

    async def _reserve_sequence_block(
        self,
        count: int,
        **_: Any,
    ) -> SequenceReservation:
        self.reserve_calls += 1
        self.events.append("reserve")
        return SequenceReservation(
            incarnation=TEST_INCARNATION,
            first_seq=10,
            count=count,
        )

    def _build_message_document(
        self,
        *,
        seq: int,
        message_data: str,
        incarnation: str,
    ) -> dict[str, Any]:
        self.events.append("build")
        return super()._build_message_document(
            seq=seq,
            message_data=message_data,
            incarnation=incarnation,
        )

    async def _write_reserved_documents_with_retry(
        self,
        documents: Sequence[dict[str, Any]],
        *,
        response_hook: Any = None,
    ) -> None:
        self.events.append("write")
        await super()._write_reserved_documents_with_retry(
            documents,
            response_hook=response_hook,
        )


class RetryBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_internal_retry_reuses_one_serialization_and_reservation(self) -> None:
        container = _FakeContainer()
        session = _ObservedSession(container)

        await session.add_items(
            [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
            ]
        )

        self.assertEqual(session.serialize_calls, 2)
        self.assertEqual(session.reserve_calls, 1)
        self.assertEqual(
            session.events,
            ["serialize", "serialize", "reserve", "build", "build", "write"],
        )
        self.assertEqual(
            set(container.items),
            {
                f"{TEST_INCARNATION}:000000000010",
                f"{TEST_INCARNATION}:000000000011",
            },
        )
        first_document_calls = [
            body for body in container.create_calls if body.get("seq") == 10
        ]
        self.assertEqual(len(first_document_calls), 2)
        self.assertIs(first_document_calls[0], first_document_calls[1])

    async def test_explicit_write_receipt_uses_operation_local_session_tokens(self) -> None:
        container = _TokenResponseContainer()
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )

        receipt = await session.add_items_with_receipt(
            [{"role": "user", "content": "hello"}]
        )

        self.assertEqual(receipt.incarnation, container.items["__session_meta__"]["incarnation"])
        self.assertEqual(receipt.first_seq, 0)
        self.assertEqual(receipt.count, 1)
        self.assertEqual(receipt.session_token, "merged-token")
        self.assertGreaterEqual(len(container.merge_inputs), 2)

    async def test_identical_document_on_conflict_is_accepted(self) -> None:
        container = _FakeContainer(transient_seq=None)
        item_id = f"{TEST_INCARNATION}:000000000010"
        container.items[item_id] = {
            "id": item_id,
            "sessionId": "session-1",
            "type": "message",
            "schema_version": 2,
            "incarnation": TEST_INCARNATION,
            "seq": 10,
            "message_data": '{"role":"user","content":"hello"}',
            "_etag": "service-managed",
        }
        session = _ObservedSession(container)

        await session.add_items([{"role": "user", "content": "hello"}])

        self.assertEqual(session.reserve_calls, 1)
        self.assertEqual(len(container.items), 1)

    async def test_conflict_mismatch_raises_for_each_integrity_field(self) -> None:
        matching = {
            "id": f"{TEST_INCARNATION}:000000000010",
            "sessionId": "session-1",
            "type": "message",
            "schema_version": 2,
            "incarnation": TEST_INCARNATION,
            "seq": 10,
            "message_data": '{"role":"user","content":"hello"}',
        }
        mismatches = {
            "sessionId": "different-session",
            "seq": 99,
            "message_data": '{"role":"user","content":"different"}',
        }

        for field, mismatched_value in mismatches.items():
            with self.subTest(field=field):
                container = _FakeContainer(transient_seq=None)
                existing = matching.copy()
                existing[field] = mismatched_value
                container.items[matching["id"]] = existing
                session = _ObservedSession(container)

                with self.assertRaises(CosmosSessionConflictError):
                    await session.add_items([{"role": "user", "content": "hello"}])

    async def test_conflict_error_does_not_retain_message_content(self) -> None:
        secret = "SENTINEL-CONVERSATION-CONTENT"
        item_id = f"{TEST_INCARNATION}:000000000010"
        container = _FakeContainer(transient_seq=None)
        container.items[item_id] = {
            "id": item_id,
            "sessionId": "session-1",
            "type": "message",
            "schema_version": 2,
            "incarnation": TEST_INCARNATION,
            "seq": 10,
            "message_data": secret,
        }
        session = _ObservedSession(container)

        with self.assertRaises(CosmosSessionConflictError) as raised:
            await session.add_items([{"role": "user", "content": "hello"}])

        error_state = repr(vars(raised.exception))
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn(secret, error_state)
        self.assertFalse(hasattr(raised.exception, "expected"))
        self.assertFalse(hasattr(raised.exception, "existing"))

    async def test_failed_write_after_reservation_leaves_a_valid_gap(self) -> None:
        container = _FakeContainer(transient_seq=None)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        container.fail_next_message_status = 400

        with self.assertRaises(CosmosHttpResponseError):
            await session.add_items([{"role": "user", "content": "first"}])
        await session.add_items([{"role": "user", "content": "second"}])

        message_sequences = {
            item["seq"] for item in container.items.values() if item.get("type") == "message"
        }
        self.assertEqual(message_sequences, {1})
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 2)

    async def test_ambiguous_reservation_failure_is_not_retried(self) -> None:
        container = _FakeContainer(transient_seq=None)
        container.items["__session_meta__"] = {
            "id": "__session_meta__",
            "sessionId": "session-1",
            "type": "meta",
            "schema_version": 2,
            "incarnation": TEST_INCARNATION,
            "last_seq": 0,
        }
        container.fail_patch_after_increment_status = 503
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )

        with self.assertRaises(CosmosHttpResponseError):
            await session.add_items([{"role": "user", "content": "hello"}])

        self.assertEqual(container.patch_calls, 1)
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 1)
        self.assertFalse(
            any(item.get("type") == "message" for item in container.items.values())
        )

    async def test_separate_add_calls_reserve_new_ranges(self) -> None:
        container = _FakeContainer(transient_seq=None)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        item = {"role": "user", "content": "same logical item"}

        await session.add_items([item])
        await session.add_items([item])

        message_sequences = {
            document["seq"] for document in container.items.values()
            if document.get("type") == "message"
        }
        self.assertEqual(message_sequences, {0, 1})
        self.assertEqual(container.patch_calls, 3)
        self.assertTrue(
            all(
                kwargs["no_response"] is False and kwargs["retry_write"] == 0
                for kwargs in container.patch_kwargs
            )
        )

    async def test_operation_id_replay_reuses_one_reserved_range(self) -> None:
        container = _FakeContainer(transient_seq=None)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        items = [{"role": "user", "content": "only once"}]

        first = await session.add_items_once(items, operation_id="request-123")
        replay = await session.add_items_once(items, operation_id="request-123")

        self.assertEqual(first, replay)
        self.assertEqual(container.batch_calls, 1)
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 1)
        self.assertEqual(
            sum(item.get("type") == "message" for item in container.items.values()),
            1,
        )
        ledger = next(
            item for item in container.items.values() if item.get("type") == "operation"
        )
        self.assertEqual(ledger["status"], "completed")
        self.assertNotIn("request-123", repr(ledger))

    async def test_concurrent_duplicate_operation_ids_share_one_range(self) -> None:
        container = _ConcurrentBatchContainer()
        first_session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        second_session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        items = [{"role": "user", "content": "only once"}]

        first = asyncio.create_task(
            first_session.add_items_once(items, operation_id="request-123")
        )
        second = asyncio.create_task(
            second_session.add_items_once(items, operation_id="request-123")
        )
        await container.batches_ready.wait()
        container.release_batches.set()
        receipts = await asyncio.gather(first, second)

        self.assertEqual(receipts[0], receipts[1])
        self.assertEqual(container.batch_calls, 2)
        self.assertEqual(container.batch_commits, 1)
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 1)
        self.assertEqual(
            sum(item.get("type") == "message" for item in container.items.values()),
            1,
        )

    async def test_operation_retry_resumes_after_partial_document_write(self) -> None:
        container = _FakeContainer(transient_seq=1)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            max_write_attempts=1,
            retry_backoff_seconds=0,
        )
        items = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "second"},
        ]

        with self.assertRaises(CosmosHttpResponseError):
            await session.add_items_once(items, operation_id="request-123")
        receipt = await session.add_items_once(items, operation_id="request-123")

        self.assertEqual(receipt.count, 2)
        self.assertEqual(container.batch_commits, 1)
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 2)
        self.assertEqual(
            {item["seq"] for item in container.items.values() if item.get("type") == "message"},
            {0, 1},
        )

    async def test_ambiguous_operation_batch_is_resolved_without_double_reservation(self) -> None:
        for timing in ("before", "after"):
            with self.subTest(timing=timing):
                container = _FakeContainer(transient_seq=None)
                if timing == "before":
                    container.fail_next_batch_before_status = 503
                else:
                    container.fail_next_batch_after_status = 503
                session = CosmosDBSession(
                    "session-1",
                    client=_FakeClient(container),  # type: ignore[arg-type]
                    retry_backoff_seconds=0,
                )

                await session.add_items_once(
                    [{"role": "user", "content": "hello"}],
                    operation_id="request-123",
                )

                self.assertEqual(container.batch_commits, 1)
                self.assertEqual(container.items["__session_meta__"]["last_seq"], 1)

    async def test_ambiguous_ledger_create_is_resolved_by_point_read(self) -> None:
        container = _FakeContainer(transient_seq=None)
        container.fail_operation_create_after_status = 503
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )

        await session.add_items_once(
            [{"role": "user", "content": "hello"}],
            operation_id="request-123",
        )

        self.assertEqual(container.batch_commits, 1)
        self.assertEqual(container.items["__session_meta__"]["last_seq"], 1)

    async def test_retry_resolves_ambiguous_operation_completion(self) -> None:
        container = _FakeContainer(transient_seq=None)
        container.fail_completion_after_status = 503
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        items = [{"role": "user", "content": "hello"}]

        with self.assertRaises(CosmosHttpResponseError):
            await session.add_items_once(items, operation_id="request-123")
        replay = await session.add_items_once(items, operation_id="request-123")

        self.assertEqual(replay.count, 1)
        self.assertEqual(container.batch_commits, 1)
        self.assertEqual(
            sum(item.get("type") == "message" for item in container.items.values()),
            1,
        )

    async def test_operation_id_mismatch_is_sanitized(self) -> None:
        container = _FakeContainer(transient_seq=None)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        await session.add_items_once(
            [{"role": "user", "content": "SENTINEL-FIRST"}],
            operation_id="SENTINEL-OPERATION-ID",
        )

        with self.assertRaises(CosmosSessionOperationError) as raised:
            await session.add_items_once(
                [{"role": "user", "content": "SENTINEL-SECOND"}],
                operation_id="SENTINEL-OPERATION-ID",
            )

        error = raised.exception
        self.assertEqual(error.code, "operation_id_mismatch")
        self.assertEqual(error.mismatched_fields, ("payload_fingerprint",))
        rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
        self.assertNotIn("SENTINEL", rendered)

    async def test_operation_ledger_uses_session_ttl(self) -> None:
        container = _FakeContainer(transient_seq=None)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            ttl_seconds=600,
            retry_backoff_seconds=0,
        )

        await session.add_items_once(
            [{"role": "user", "content": "hello"}],
            operation_id="request-123",
        )

        self.assertTrue(container.items)
        self.assertTrue(all(item["ttl"] == 600 for item in container.items.values()))

    async def test_reserved_operation_retry_after_clear_stays_in_old_incarnation(self) -> None:
        container = _FakeContainer(transient_seq=0)
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            max_write_attempts=1,
            retry_backoff_seconds=0,
        )
        items = [{"role": "user", "content": "stale"}]

        with self.assertRaises(CosmosHttpResponseError):
            await session.add_items_once(items, operation_id="request-123")
        old_incarnation = container.items["__session_meta__"]["incarnation"]
        new_incarnation = "b" * 32
        metadata = container.items["__session_meta__"]
        metadata["incarnation"] = new_incarnation
        metadata["last_seq"] = 0
        metadata["_etag"] = container._next_etag()

        receipt = await session.add_items_once(items, operation_id="request-123")

        self.assertEqual(receipt.incarnation, old_incarnation)
        self.assertEqual(await session.get_items(), [])
        self.assertEqual(metadata["incarnation"], new_incarnation)
        self.assertEqual(metadata["last_seq"], 0)

    async def test_negative_sequence_metadata_is_rejected(self) -> None:
        container = _FakeContainer(transient_seq=None)
        container.items["__session_meta__"] = {
            "id": "__session_meta__",
            "sessionId": "session-1",
            "type": "meta",
            "schema_version": 2,
            "incarnation": TEST_INCARNATION,
            "last_seq": -2,
        }
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )

        with self.assertRaises(CosmosSessionConflictError):
            await session.add_items([{"role": "user", "content": "hello"}])


if __name__ == "__main__":
    unittest.main()
