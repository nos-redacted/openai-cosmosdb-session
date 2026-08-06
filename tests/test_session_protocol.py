from __future__ import annotations

import asyncio
import copy
import unittest
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from agents.memory.session import Session
from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session import (
    CosmosDBSession as ExportedCosmosDBSession,
)
from openai_cosmosdb_session import (
    CosmosSessionWriteReceipt,
)
from openai_cosmosdb_session.exceptions import CosmosSessionCorruptDocumentError
from openai_cosmosdb_session.session import CosmosDBSession


class _FakeContainer:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.query_calls = 0
        self.deleted_batches: list[list[str]] = []
        self.etag = 0

    def _next_etag(self) -> str:
        self.etag += 1
        return str(self.etag)

    async def create_item(self, *, body: dict[str, Any], **_: Any) -> dict[str, Any]:
        if body["id"] in self.items:
            raise CosmosHttpResponseError(status_code=409, message="conflict")
        self.items[body["id"]] = copy.deepcopy(body)
        self.items[body["id"]]["_etag"] = self._next_etag()
        return copy.deepcopy(body)

    async def patch_item(
        self,
        *,
        item: str,
        partition_key: str,
        patch_operations: list[dict[str, Any]],
        **_: Any,
    ) -> dict[str, Any]:
        assert partition_key == "session-1"
        if item not in self.items:
            raise CosmosHttpResponseError(status_code=404, message="missing")
        document = self.items[item]
        for operation in patch_operations:
            field = operation["path"].removeprefix("/")
            if operation["op"] == "incr":
                document[field] += operation["value"]
            else:
                document[field] = operation["value"]
        document["_etag"] = self._next_etag()
        return copy.deepcopy(document)

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

    async def delete_item(
        self,
        *,
        item: str,
        partition_key: str,
        etag: str | None = None,
        **_: Any,
    ) -> None:
        assert partition_key == "session-1"
        if item not in self.items:
            raise CosmosHttpResponseError(status_code=404, message="missing")
        if etag is not None and self.items[item].get("_etag") != etag:
            raise CosmosHttpResponseError(status_code=412, message="stale")
        del self.items[item]

    def query_items(
        self,
        *,
        query: str,
        parameters: list[dict[str, Any]] | None = None,
        partition_key: str,
        **_: Any,
    ) -> Any:
        assert partition_key == "session-1"
        self.query_calls += 1
        parameter_values = {
            parameter["name"]: parameter["value"] for parameter in parameters or []
        }
        if query == "SELECT VALUE c.id FROM c":
            values = list(self.items)
        elif query == "SELECT c.id, c.incarnation FROM c WHERE c.type = 'message'":
            values: list[Any] = [
                {"id": item["id"], "incarnation": item.get("incarnation")}
                for item in self.items.values()
                if item.get("type") == "message"
            ]
        else:
            values = [
                copy.deepcopy(item)
                for item in self.items.values()
                if item.get("type") == "message"
                and (
                    "@incarnation" not in parameter_values
                    or item.get("incarnation") == parameter_values["@incarnation"]
                )
            ]
            descending = "DESC" in query
            values.sort(key=lambda item: item["seq"], reverse=descending)
            if query.startswith("SELECT TOP 1"):
                values = values[:1]
            elif "@limit" in parameter_values:
                values = values[: parameter_values["@limit"]]

        async def iterate() -> Any:
            for value in values:
                yield value

        return iterate()

    async def execute_item_batch(
        self,
        *,
        batch_operations: list[tuple[str, tuple[str]]],
        partition_key: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        assert partition_key == "session-1"
        ids = [arguments[0] for _, arguments in batch_operations]
        self.deleted_batches.append(ids)
        for item_id in ids:
            del self.items[item_id]
        return []


class _SessionTokenContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__()
        self.token_calls: list[tuple[str, str]] = []

    def _record(self, operation: str, session_token: str | None) -> None:
        if session_token is not None:
            self.token_calls.append((operation, session_token))

    async def read_item(
        self,
        *,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._record("read", session_token)
        return await super().read_item(**kwargs)

    def query_items(
        self,
        *,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> Any:
        self._record("query", session_token)
        return super().query_items(**kwargs)

    async def patch_item(
        self,
        *,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._record("patch", session_token)
        return await super().patch_item(**kwargs)

    async def delete_item(
        self,
        *,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._record("delete", session_token)
        await super().delete_item(**kwargs)

    async def execute_item_batch(
        self,
        *,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self._record("batch", session_token)
        return await super().execute_item_batch(**kwargs)


class _ConcurrentPopContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__()
        self.pop_query_waiters = 0
        self.pop_queries_ready = asyncio.Event()
        self.release_pop_queries = asyncio.Event()
        self.pause_pop_queries = True

    def query_items(self, **kwargs: Any) -> Any:
        documents = super().query_items(**kwargs)
        if not kwargs["query"].startswith("SELECT TOP 1") or not self.pause_pop_queries:
            return documents

        async def iterate() -> Any:
            values = [document async for document in documents]
            self.pop_query_waiters += 1
            if self.pop_query_waiters == 2:
                self.pause_pop_queries = False
                self.pop_queries_ready.set()
            await self.release_pop_queries.wait()
            for value in values:
                yield value

        return iterate()


class _PausedDeleteContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__()
        self.delete_started = asyncio.Event()
        self.release_delete = asyncio.Event()
        self.pause_next_delete = True
        self.delete_attempts: list[str] = []

    async def delete_item(self, *, item: str, **kwargs: Any) -> None:
        self.delete_attempts.append(item)
        if self.pause_next_delete:
            self.pause_next_delete = False
            self.delete_started.set()
            await self.release_delete.wait()
        await super().delete_item(item=item, **kwargs)


class _PausedBatchContainer(_FakeContainer):
    def __init__(self) -> None:
        super().__init__()
        self.batch_started = asyncio.Event()
        self.release_batch = asyncio.Event()
        self.pause_next_batch = True

    async def execute_item_batch(self, **kwargs: Any) -> list[dict[str, Any]]:
        if self.pause_next_batch:
            self.pause_next_batch = False
            self.batch_started.set()
            await self.release_batch.wait()
        return await super().execute_item_batch(**kwargs)


class _FakeDatabase:
    def __init__(self, container: _FakeContainer) -> None:
        self.container = container

    def get_container_client(self, _: str) -> _FakeContainer:
        return self.container

    async def read(self, **_: Any) -> dict[str, str]:
        return {"id": "agents"}


class _FakeClient:
    def __init__(self, container: _FakeContainer) -> None:
        self.database = _FakeDatabase(container)
        self.closed = False

    def get_database_client(self, _: str) -> _FakeDatabase:
        return self.database

    async def close(self) -> None:
        self.closed = True


@dataclass
class _Settings:
    limit: int | None = None


class _ObservedDeserializeSession(CosmosDBSession):
    def __init__(self, client: _FakeClient) -> None:
        super().__init__(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        self.deserialize_calls = 0

    async def _deserialize_item(self, message_data: str) -> Any:
        self.deserialize_calls += 1
        return await super()._deserialize_item(message_data)


class _DelayedWriteSession(CosmosDBSession):
    def __init__(self, client: _FakeClient) -> None:
        super().__init__(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        self.reserved = asyncio.Event()
        self.release_write = asyncio.Event()

    async def _write_reserved_documents_with_retry(
        self,
        documents: Sequence[dict[str, Any]],
        *,
        response_hook: Any = None,
    ) -> None:
        self.reserved.set()
        await self.release_write.wait()
        await super()._write_reserved_documents_with_retry(
            documents,
            response_hook=response_hook,
        )


class SessionProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.container = _FakeContainer()
        self.client = _FakeClient(self.container)
        self.session = CosmosDBSession(
            "session-1",
            client=self.client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )

    def test_implements_agents_session_protocol(self) -> None:
        self.assertIsInstance(self.session, Session)

    def test_package_export_is_session_class(self) -> None:
        self.assertIs(ExportedCosmosDBSession, CosmosDBSession)

    async def test_get_items_uses_deserialization_hook(self) -> None:
        session = _ObservedDeserializeSession(self.client)
        item = {"role": "user", "content": "hello"}
        await session.add_items([item])

        self.assertEqual(await session.get_items(), [item])
        self.assertEqual(session.deserialize_calls, 1)

    async def test_receipt_aware_read_forwards_session_token_to_point_and_query(self) -> None:
        container = _SessionTokenContainer()
        client = _FakeClient(container)
        writer = CosmosDBSession(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        reader = CosmosDBSession(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        item = {"role": "user", "content": "hello"}
        await writer.add_items([item])
        receipt = CosmosSessionWriteReceipt(
            incarnation=container.items["__session_meta__"]["incarnation"],
            first_seq=0,
            count=1,
            session_token="opaque-session-token",
        )
        self.assertNotIn("opaque-session-token", repr(receipt))

        self.assertEqual(await reader.get_items_with_receipt(receipt), [item])

        self.assertEqual(
            container.token_calls,
            [
                ("read", "opaque-session-token"),
                ("query", "opaque-session-token"),
            ],
        )

    async def test_receipt_aware_pop_forwards_session_token_through_delete(self) -> None:
        container = _SessionTokenContainer()
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        item = {"role": "user", "content": "hello"}
        await session.add_items([item])
        receipt = CosmosSessionWriteReceipt(
            incarnation=container.items["__session_meta__"]["incarnation"],
            first_seq=0,
            count=1,
            session_token="opaque-session-token",
        )

        self.assertEqual(await session.pop_item_with_receipt(receipt), item)

        self.assertEqual(
            container.token_calls,
            [
                ("read", "opaque-session-token"),
                ("query", "opaque-session-token"),
                ("delete", "opaque-session-token"),
                ("read", "opaque-session-token"),
            ],
        )

    async def test_receipt_aware_clear_forwards_session_token_through_cleanup(self) -> None:
        container = _SessionTokenContainer()
        session = CosmosDBSession(
            "session-1",
            client=_FakeClient(container),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        await session.add_items([{"role": "user", "content": "hello"}])
        receipt = CosmosSessionWriteReceipt(
            incarnation=container.items["__session_meta__"]["incarnation"],
            first_seq=0,
            count=1,
            session_token="opaque-session-token",
        )

        await session.clear_session_with_receipt(receipt)

        self.assertEqual(
            container.token_calls,
            [
                ("patch", "opaque-session-token"),
                ("query", "opaque-session-token"),
                ("batch", "opaque-session-token"),
            ],
        )

    async def test_add_get_and_latest_limit_are_chronological(self) -> None:
        items = [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "two"},
            {"role": "user", "content": "three"},
        ]

        await self.session.add_items(items)

        self.assertEqual(await self.session.get_items(), items)
        self.assertEqual(await self.session.get_items(limit=2), items[-2:])

    async def test_non_positive_limit_does_not_query(self) -> None:
        self.assertEqual(await self.session.get_items(limit=0), [])
        self.assertEqual(await self.session.get_items(limit=-1), [])
        self.assertEqual(self.container.query_calls, 0)

    async def test_settings_limit_and_explicit_override(self) -> None:
        session = CosmosDBSession(
            "session-1",
            client=self.client,  # type: ignore[arg-type]
            session_settings=_Settings(limit=1),  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        items = [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "two"},
        ]
        await session.add_items(items)

        self.assertEqual(await session.get_items(), items[-1:])
        self.assertEqual(await session.get_items(limit=2), items)

    async def test_pop_preserves_corrupt_latest_item_and_raises(self) -> None:
        items = [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "two"},
        ]
        await self.session.add_items(items)
        latest = next(
            document
            for document in self.container.items.values()
            if document.get("type") == "message" and document.get("seq") == 1
        )
        latest["message_data"] = "not json"

        with self.assertRaises(CosmosSessionCorruptDocumentError) as raised:
            await self.session.pop_item()

        self.assertEqual(raised.exception.code, "invalid_message_payload")
        self.assertEqual(raised.exception.invalid_fields, ("message_data",))
        self.assertIn(latest["id"], self.container.items)
        with self.assertRaises(CosmosSessionCorruptDocumentError):
            await self.session.get_items(limit=1)

    async def test_clear_rotates_metadata_and_removes_old_messages(self) -> None:
        await self.session.add_items([{"role": "user", "content": "hello"}])
        previous_incarnation = self.container.items["__session_meta__"]["incarnation"]

        await self.session.clear_session()

        metadata = self.container.items["__session_meta__"]
        self.assertEqual(metadata["schema_version"], 2)
        self.assertEqual(metadata["last_seq"], 0)
        self.assertNotEqual(metadata["incarnation"], previous_incarnation)
        self.assertFalse(
            any(
                document.get("type") == "message"
                for document in self.container.items.values()
            )
        )

    async def test_clear_hides_a_write_reserved_before_the_clear(self) -> None:
        session = _DelayedWriteSession(self.client)
        append = asyncio.create_task(
            session.add_items([{"role": "user", "content": "stale"}])
        )
        await session.reserved.wait()

        await self.session.clear_session()
        session.release_write.set()
        await append

        self.assertEqual(await self.session.get_items(), [])

    async def test_two_concurrent_pops_remove_distinct_items(self) -> None:
        container = _ConcurrentPopContainer()
        client = _FakeClient(container)
        session = CosmosDBSession(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        items = [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "second"},
        ]
        await session.add_items(items)

        first_pop = asyncio.create_task(session.pop_item())
        second_pop = asyncio.create_task(session.pop_item())
        await container.pop_queries_ready.wait()
        container.release_pop_queries.set()
        popped = await asyncio.gather(first_pop, second_pop)

        self.assertCountEqual(popped, items)
        self.assertEqual(await session.get_items(), [])

    async def test_stale_pop_reference_cannot_delete_a_post_clear_replacement(self) -> None:
        container = _PausedDeleteContainer()
        client = _FakeClient(container)
        session = CosmosDBSession(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        await session.add_items([{"role": "user", "content": "old"}])

        pop = asyncio.create_task(session.pop_item())
        await container.delete_started.wait()
        await session.clear_session()
        new_item = {"role": "assistant", "content": "new"}
        await session.add_items([new_item])
        container.release_delete.set()

        self.assertEqual(await pop, new_item)
        self.assertEqual(len(container.delete_attempts), 2)
        self.assertNotEqual(container.delete_attempts[0], container.delete_attempts[1])
        self.assertEqual(await session.get_items(), [])

    async def test_clear_cleans_old_messages_in_batches_of_one_hundred(self) -> None:
        items = [
            {"role": "user", "content": f"item-{index}"}
            for index in range(205)
        ]
        await self.session.add_items(items)

        await self.session.clear_session()

        self.assertEqual([len(batch) for batch in self.container.deleted_batches], [100, 100, 5])
        self.assertEqual(await self.session.get_items(), [])

    async def test_new_append_survives_cleanup_of_the_old_incarnation(self) -> None:
        container = _PausedBatchContainer()
        client = _FakeClient(container)
        session = CosmosDBSession(
            "session-1",
            client=client,  # type: ignore[arg-type]
            retry_backoff_seconds=0,
        )
        await session.add_items([{"role": "user", "content": "old"}])

        clear = asyncio.create_task(session.clear_session())
        await container.batch_started.wait()
        new_item = {"role": "assistant", "content": "new"}
        await session.add_items([new_item])
        container.release_batch.set()
        await clear

        self.assertEqual(await session.get_items(), [new_item])

    async def test_delete_session_physically_removes_a_quiescent_partition(self) -> None:
        await self.session.add_items([{"role": "user", "content": "hello"}])

        await self.session.delete_session()

        self.assertEqual(self.container.items, {})
        self.assertEqual(self.container.deleted_batches[-1][-1], "__session_meta__")

    async def test_close_does_not_close_injected_client(self) -> None:
        self.assertTrue(await self.session.ping())

        await self.session.close()

        self.assertFalse(self.client.closed)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await self.session.get_items()


if __name__ == "__main__":
    unittest.main()
