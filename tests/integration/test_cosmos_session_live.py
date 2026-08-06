from __future__ import annotations

import asyncio
from typing import Any

import pytest
from azure.core.exceptions import ServiceRequestError

from openai_cosmosdb_session import CosmosDBSession
from openai_cosmosdb_session._write_pipeline import SequenceReservation

pytestmark = pytest.mark.live_cosmos


class _PostCreateTransientSession(CosmosDBSession):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.reserve_calls = 0
        self.injected_failure = False

    async def _reserve_sequence_block(
        self,
        count: int,
        **kwargs: Any,
    ) -> SequenceReservation:
        self.reserve_calls += 1
        return await super()._reserve_sequence_block(count, **kwargs)

    async def _create_reserved_document(
        self,
        expected_document: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        if not self.injected_failure:
            await self._container.create_item(
                body=expected_document,
                retry_write=0,
                response_hook=kwargs.get("response_hook"),
            )
            self.injected_failure = True
            raise ServiceRequestError("synthetic post-create transport failure")
        await super()._create_reserved_document(expected_document, **kwargs)


async def test_live_smoke_and_cross_session_isolation(live_cosmos: Any) -> None:
    first_id = live_cosmos.new_session_id("smoke-a")
    second_id = live_cosmos.new_session_id("smoke-b")
    first = live_cosmos.session(first_id)
    second = live_cosmos.session(second_id)
    first_items = [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "second"},
    ]
    second_item = {"role": "user", "content": "isolated"}

    assert await first.ping()
    await first.add_items(first_items)
    await second.add_items([second_item])

    assert await first.get_items() == first_items
    assert await first.get_items(limit=1) == first_items[-1:]
    assert await second.get_items() == [second_item]
    assert await first.pop_item() == first_items[-1]
    assert await first.get_items() == first_items[:-1]

    await first.clear_session()
    assert await first.get_items() == []
    assert await second.get_items() == [second_item]


async def test_concurrent_live_appends_have_unique_dense_sequences(live_cosmos: Any) -> None:
    session_id = live_cosmos.new_session_id("concurrent")
    items = [
        {"role": "user", "content": f"concurrent-{index}"}
        for index in range(8)
    ]
    writers = [live_cosmos.session(session_id) for _ in items]

    await asyncio.gather(
        *(writer.add_items([item]) for writer, item in zip(writers, items, strict=True))
    )

    history = await live_cosmos.session(session_id).get_items()
    assert {item["content"] for item in history} == {item["content"] for item in items}
    documents = live_cosmos.container.query_items(
        query="SELECT c.seq FROM c WHERE c.type = 'message' ORDER BY c.seq ASC",
        partition_key=session_id,
    )
    sequences = [document["seq"] async for document in documents]
    assert sequences == list(range(len(items)))


async def test_live_container_validation(live_cosmos: Any) -> None:
    session_id = live_cosmos.new_session_id("validation")

    result = await live_cosmos.session(session_id).validate_container()

    assert result.is_valid, result.issue_codes


async def test_independent_client_reads_with_write_receipt(live_cosmos: Any) -> None:
    session_id = live_cosmos.new_session_id("receipt")
    writer = await live_cosmos.independent_session(session_id)
    reader = await live_cosmos.independent_session(session_id)
    item = {"role": "user", "content": "cross-client receipt"}

    receipt = await writer.add_items_with_receipt([item])

    assert receipt.session_token
    assert await reader.get_items_with_receipt(receipt) == [item]


async def test_concurrent_duplicate_operation_id_writes_once(live_cosmos: Any) -> None:
    session_id = live_cosmos.new_session_id("operation")
    first = await live_cosmos.independent_session(session_id)
    second = await live_cosmos.independent_session(session_id)
    item = {"role": "user", "content": "deduplicated"}

    receipts = await asyncio.gather(
        first.add_items_once([item], operation_id="shared-live-operation"),
        second.add_items_once([item], operation_id="shared-live-operation"),
    )

    assert receipts[0].incarnation == receipts[1].incarnation
    assert receipts[0].first_seq == receipts[1].first_seq
    assert await live_cosmos.session(session_id).get_items() == [item]


async def test_post_create_failure_replays_real_409_without_reserving_again(
    live_cosmos: Any,
) -> None:
    session_id = live_cosmos.new_session_id("replay")
    session = _PostCreateTransientSession(
        session_id,
        client=live_cosmos.client,
        database=live_cosmos.database_name,
        container=live_cosmos.container_name,
        max_write_attempts=2,
        retry_backoff_seconds=0,
    )
    item = {"role": "user", "content": "real 409 replay"}

    await session.add_items([item])

    assert session.reserve_calls == 1
    assert session.injected_failure
    assert await session.get_items() == [item]


async def test_ttl_fields_are_written_when_container_supports_ttl(live_cosmos: Any) -> None:
    if live_cosmos.container_properties.get("defaultTtl") is None:
        pytest.skip("container TTL is disabled; provision it with default_ttl=-1")

    session_id = live_cosmos.new_session_id("ttl")
    ttl_seconds = 300
    session = live_cosmos.session(session_id, ttl_seconds=ttl_seconds)
    await session.add_items([{"role": "user", "content": "ttl"}])

    metadata = await live_cosmos.container.read_item(
        item="__session_meta__",
        partition_key=session_id,
    )
    message = await live_cosmos.container.read_item(
        item=f"{metadata['incarnation']}:000000000000",
        partition_key=session_id,
    )
    assert message["ttl"] == ttl_seconds
    assert metadata["ttl"] == ttl_seconds
