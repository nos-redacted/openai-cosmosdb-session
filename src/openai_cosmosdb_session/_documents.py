"""Message serialization and Cosmos DB document schema helpers."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from .exceptions import CosmosSessionCorruptDocumentError

if TYPE_CHECKING:
    from agents.items import TResponseInputItem

SCHEMA_VERSION = 2
META_ID = "__session_meta__"
OPERATION_ID_PREFIX = "__operation__:"


async def serialize_item(item: TResponseInputItem) -> str:
    return json.dumps(item, ensure_ascii=False, separators=(",", ":"))


async def deserialize_item(message_data: str) -> TResponseInputItem:
    return cast("TResponseInputItem", json.loads(message_data))


async def deserialize_document(
    document: Mapping[str, Any],
    deserialize: Callable[[str], Awaitable[TResponseInputItem]],
) -> TResponseInputItem:
    invalid_fields: list[str] = []
    document_id = document.get("id")
    session_id = document.get("sessionId")
    incarnation = document.get("incarnation")
    seq = document.get("seq")
    message_data = document.get("message_data")
    if not isinstance(document_id, str):
        invalid_fields.append("id")
    if not isinstance(session_id, str) or not session_id:
        invalid_fields.append("sessionId")
    if document.get("type") != "message":
        invalid_fields.append("type")
    if document.get("schema_version") != SCHEMA_VERSION:
        invalid_fields.append("schema_version")
    if not isinstance(incarnation, str) or not incarnation:
        invalid_fields.append("incarnation")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
        invalid_fields.append("seq")
    if not isinstance(message_data, str):
        invalid_fields.append("message_data")
    if (
        isinstance(document_id, str)
        and isinstance(incarnation, str)
        and isinstance(seq, int)
        and not isinstance(seq, bool)
        and document_id != f"{incarnation}:{seq:012d}"
    ):
        invalid_fields.append("id")
    if invalid_fields:
        raise CosmosSessionCorruptDocumentError(
            "A stored session message failed schema validation.",
            code="invalid_message_document",
            invalid_fields=tuple(dict.fromkeys(invalid_fields)),
        )

    assert isinstance(message_data, str)
    try:
        item = await deserialize(message_data)
    except (json.JSONDecodeError, TypeError, ValueError):
        raise CosmosSessionCorruptDocumentError(
            "A stored session message payload could not be decoded.",
            code="invalid_message_payload",
            invalid_fields=("message_data",),
        ) from None
    if not isinstance(item, Mapping):
        raise CosmosSessionCorruptDocumentError(
            "A stored session message payload has an invalid shape.",
            code="invalid_message_shape",
            invalid_fields=("message_data",),
        )
    return item


def build_message_document(
    *,
    seq: int,
    message_data: str,
    session_id: str,
    incarnation: str,
    ttl_seconds: int | None,
    created_at: str | None = None,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "id": f"{incarnation}:{seq:012d}",
        "sessionId": session_id,
        "type": "message",
        "schema_version": SCHEMA_VERSION,
        "incarnation": incarnation,
        "seq": seq,
        "message_data": message_data,
        "created_at": created_at or utc_now(),
    }
    if ttl_seconds is not None:
        document["ttl"] = ttl_seconds
    return document


def operation_ledger_id(operation_id: str) -> str:
    digest = sha256(operation_id.encode("utf-8")).hexdigest()
    return f"{OPERATION_ID_PREFIX}{digest}"


def payload_fingerprint(serialized_items: Sequence[str]) -> str:
    digest = sha256()
    for message_data in serialized_items:
        encoded = message_data.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def build_pending_operation_ledger(
    *,
    ledger_id: str,
    session_id: str,
    payload_hash: str,
    count: int,
    ttl_seconds: int | None,
    now: str,
) -> dict[str, Any]:
    ledger: dict[str, Any] = {
        "id": ledger_id,
        "sessionId": session_id,
        "type": "operation",
        "schema_version": SCHEMA_VERSION,
        "payload_fingerprint": payload_hash,
        "count": count,
        "status": "pending",
        "created_at": now,
        "updated_at": now,
    }
    if ttl_seconds is not None:
        ledger["ttl"] = ttl_seconds
    return ledger


def documents_match(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return not document_mismatched_fields(existing, expected)


def document_mismatched_fields(
    existing: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> tuple[str, ...]:
    return tuple(
        field
        for field in ("sessionId", "incarnation", "seq", "message_data")
        if existing.get(field) != expected.get(field)
    )


def new_incarnation() -> str:
    return uuid4().hex


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
