"""Conversation-history reads and destructive operations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

from azure.core import MatchConditions
from azure.cosmos.aio import ContainerProxy
from azure.cosmos.exceptions import CosmosHttpResponseError

from . import _documents
from ._tokens import ResponseHook
from .exceptions import CosmosSessionConflictError, CosmosSessionCorruptDocumentError

if TYPE_CHECKING:
    from agents.items import TResponseInputItem


async def get_items(
    *,
    container: ContainerProxy,
    session_id: str,
    limit: int | None,
    deserialize: Callable[
        [Mapping[str, Any]], Awaitable[TResponseInputItem]
    ],
    session_token: str | None = None,
    response_hook: ResponseHook | None = None,
) -> list[TResponseInputItem]:
    if limit is not None and limit <= 0:
        return []

    incarnation = await _read_current_incarnation(
        container,
        session_id,
        session_token=session_token,
        response_hook=response_hook,
    )
    if incarnation is None:
        return []

    parameters: list[dict[str, Any]] = [
        {"name": "@incarnation", "value": incarnation}
    ]
    if limit is None:
        query = (
            "SELECT * FROM c WHERE c.type = 'message' "
            "AND c.incarnation = @incarnation ORDER BY c.seq ASC"
        )
    else:
        query = (
            "SELECT * FROM c WHERE c.type = 'message' AND c.incarnation = @incarnation "
            "ORDER BY c.seq DESC OFFSET 0 LIMIT @limit"
        )
        parameters.append({"name": "@limit", "value": limit})

    items: list[TResponseInputItem] = []
    documents = container.query_items(
        query=query,
        parameters=parameters,
        partition_key=session_id,
        session_token=session_token,
        response_hook=response_hook,
    )
    async for document in documents:
        item = await deserialize(document)
        items.append(item)

    if limit is not None:
        items.reverse()
    return items


async def pop_item(
    *,
    container: ContainerProxy,
    session_id: str,
    deserialize: Callable[
        [Mapping[str, Any]], Awaitable[TResponseInputItem]
    ],
    session_token: str | None = None,
    response_hook: ResponseHook | None = None,
) -> TResponseInputItem | None:
    query = (
        "SELECT TOP 1 * FROM c WHERE c.type = 'message' "
        "AND c.incarnation = @incarnation ORDER BY c.seq DESC"
    )

    while True:
        incarnation = await _read_current_incarnation(
            container,
            session_id,
            session_token=session_token,
            response_hook=response_hook,
        )
        if incarnation is None:
            return None
        latest = None
        documents = container.query_items(
            query=query,
            parameters=[{"name": "@incarnation", "value": incarnation}],
            partition_key=session_id,
            session_token=session_token,
            response_hook=response_hook,
        )
        async for document in documents:
            latest = document
            break
        if latest is None:
            return None

        item_id = latest.get("id")
        if not isinstance(item_id, str):
            raise CosmosSessionCorruptDocumentError(
                "The newest message document has an invalid id.",
                code="invalid_message_document",
                invalid_fields=("id",),
            )
        etag = latest.get("_etag")
        if not isinstance(etag, str):
            raise CosmosSessionCorruptDocumentError(
                "The newest message document has an invalid ETag.",
                code="invalid_message_document",
                invalid_fields=("_etag",),
            )
        item = await deserialize(latest)
        try:
            await container.delete_item(
                item=item_id,
                partition_key=session_id,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
                retry_write=0,
                session_token=session_token,
                response_hook=response_hook,
            )
        except CosmosHttpResponseError as error:
            if error.status_code in {404, 412}:
                continue
            raise

        if (
            await _read_current_incarnation(
                container,
                session_id,
                session_token=session_token,
                response_hook=response_hook,
            )
            != incarnation
        ):
            continue
        return item


async def clear_session(
    *,
    container: ContainerProxy,
    session_id: str,
    current_incarnation: str,
    session_token: str | None = None,
    response_hook: ResponseHook | None = None,
) -> None:
    results = container.query_items(
        query="SELECT c.id, c.incarnation FROM c WHERE c.type = 'message'",
        partition_key=session_id,
        session_token=session_token,
        response_hook=response_hook,
    )
    item_ids: list[str] = []
    async for result in results:
        item_id = result.get("id") if isinstance(result, Mapping) else None
        incarnation = result.get("incarnation") if isinstance(result, Mapping) else None
        if isinstance(item_id, str) and incarnation != current_incarnation:
            item_ids.append(item_id)

    for offset in range(0, len(item_ids), 100):
        batch_ids = item_ids[offset : offset + 100]
        operations = [("delete", (item_id,)) for item_id in batch_ids]
        await container.execute_item_batch(
            batch_operations=operations,
            partition_key=session_id,
            retry_write=0,
            session_token=session_token,
            response_hook=response_hook,
        )


async def delete_session(
    *,
    container: ContainerProxy,
    session_id: str,
    response_hook: ResponseHook | None = None,
) -> None:
    results = container.query_items(
        query="SELECT VALUE c.id FROM c",
        partition_key=session_id,
        response_hook=response_hook,
    )
    item_ids: list[str] = []
    async for result in results:
        item_id: Any = result
        if isinstance(item_id, str):
            item_ids.append(item_id)
    item_ids.sort(key=lambda item_id: item_id == _documents.META_ID)

    for offset in range(0, len(item_ids), 100):
        batch_ids = item_ids[offset : offset + 100]
        operations = [("delete", (item_id,)) for item_id in batch_ids]
        await container.execute_item_batch(
            batch_operations=operations,
            partition_key=session_id,
            retry_write=0,
            response_hook=response_hook,
        )


async def _read_current_incarnation(
    container: ContainerProxy,
    session_id: str,
    *,
    session_token: str | None = None,
    response_hook: ResponseHook | None = None,
) -> str | None:
    try:
        metadata = await container.read_item(
            item=_documents.META_ID,
            partition_key=session_id,
            session_token=session_token,
            response_hook=response_hook,
        )
    except CosmosHttpResponseError as error:
        if error.status_code == 404:
            return None
        raise

    incarnation = metadata.get("incarnation")
    if (
        metadata.get("sessionId") != session_id
        or metadata.get("type") != "meta"
        or metadata.get("schema_version") != _documents.SCHEMA_VERSION
        or not isinstance(incarnation, str)
        or not incarnation
    ):
        raise CosmosSessionConflictError(
            "The session metadata is not valid schema-v2 metadata.",
            code="invalid_session_metadata",
            mismatched_fields=("sessionId", "type", "schema_version", "incarnation"),
        )
    return incarnation
