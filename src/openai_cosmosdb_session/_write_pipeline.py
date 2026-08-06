"""Safe sequence reservation and deterministic document-write retries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.cosmos.aio import ContainerProxy
from azure.cosmos.exceptions import CosmosBatchOperationError, CosmosHttpResponseError

from . import _documents
from ._tokens import ResponseHook
from .exceptions import CosmosSessionConflictError, CosmosSessionOperationError

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class SequenceReservation:
    incarnation: str
    first_seq: int
    count: int


@dataclass(frozen=True)
class OperationReservation:
    sequence: SequenceReservation
    ledger_id: str
    message_created_at: str
    completed: bool


async def reserve_operation_sequence_block(
    *,
    container: ContainerProxy,
    session_id: str,
    ttl_seconds: int | None,
    count: int,
    operation_id: str,
    payload_hash: str,
    create_metadata: Callable[[str], Awaitable[None]],
    response_hook: ResponseHook | None = None,
) -> OperationReservation:
    ledger_id = _documents.operation_ledger_id(operation_id)
    now = _documents.utc_now()
    pending_ledger = _documents.build_pending_operation_ledger(
        ledger_id=ledger_id,
        session_id=session_id,
        payload_hash=payload_hash,
        count=count,
        ttl_seconds=ttl_seconds,
        now=now,
    )
    await _create_operation_ledger_if_missing(
        container=container,
        session_id=session_id,
        pending_ledger=pending_ledger,
        response_hook=response_hook,
    )

    for _ in range(16):
        ledger = await container.read_item(
            item=ledger_id,
            partition_key=session_id,
            response_hook=response_hook,
        )
        existing_reservation = _validate_operation_ledger(
            ledger,
            session_id=session_id,
            payload_hash=payload_hash,
            count=count,
        )
        if existing_reservation is not None:
            return existing_reservation

        try:
            metadata = await container.read_item(
                item=_documents.META_ID,
                partition_key=session_id,
                response_hook=response_hook,
            )
        except CosmosHttpResponseError as error:
            if error.status_code != 404:
                raise
            await create_metadata(_documents.utc_now())
            continue

        incarnation, first_seq, metadata_etag = _validate_reservable_metadata(
            metadata,
            session_id=session_id,
        )
        ledger_etag = ledger.get("_etag")
        if not isinstance(ledger_etag, str) or not ledger_etag:
            raise CosmosSessionOperationError(
                "The operation ledger cannot be conditionally updated.",
                code="invalid_operation_ledger",
                mismatched_fields=("_etag",),
            )

        reserved_at = _documents.utc_now()
        metadata_patch: list[dict[str, Any]] = [
            {"op": "incr", "path": "/last_seq", "value": count},
            {"op": "set", "path": "/updated_at", "value": reserved_at},
        ]
        if ttl_seconds is not None:
            metadata_patch.append(
                {"op": "set", "path": "/ttl", "value": ttl_seconds}
            )
        ledger_patch: list[dict[str, Any]] = [
            {"op": "set", "path": "/status", "value": "reserved"},
            {"op": "set", "path": "/incarnation", "value": incarnation},
            {"op": "set", "path": "/first_seq", "value": first_seq},
            {"op": "set", "path": "/message_created_at", "value": reserved_at},
            {"op": "set", "path": "/updated_at", "value": reserved_at},
        ]
        try:
            await container.execute_item_batch(
                batch_operations=[
                    (
                        "patch",
                        (_documents.META_ID, metadata_patch),
                        {"if_match_etag": metadata_etag},
                    ),
                    (
                        "patch",
                        (ledger_id, ledger_patch),
                        {"if_match_etag": ledger_etag},
                    ),
                ],
                partition_key=session_id,
                retry_write=0,
                response_hook=response_hook,
            )
        except (
            CosmosBatchOperationError,
            CosmosHttpResponseError,
            ServiceRequestError,
            ServiceResponseError,
        ) as error:
            status_code = getattr(error, "status_code", None)
            if not isinstance(error, (ServiceRequestError, ServiceResponseError)) and (
                status_code not in RETRYABLE_STATUS_CODES | {409, 412}
            ):
                raise
            continue

        return OperationReservation(
            sequence=SequenceReservation(
                incarnation=incarnation,
                first_seq=first_seq,
                count=count,
            ),
            ledger_id=ledger_id,
            message_created_at=reserved_at,
            completed=False,
        )

    raise CosmosSessionOperationError(
        "The operation could not acquire a sequence range due to contention.",
        code="operation_reservation_exhausted",
    )


async def complete_operation(
    *,
    container: ContainerProxy,
    session_id: str,
    ledger_id: str,
    response_hook: ResponseHook | None = None,
) -> None:
    await container.patch_item(
        item=ledger_id,
        partition_key=session_id,
        patch_operations=[
            {"op": "set", "path": "/status", "value": "completed"},
            {"op": "set", "path": "/updated_at", "value": _documents.utc_now()},
        ],
        no_response=False,
        retry_write=0,
        response_hook=response_hook,
    )


async def _create_operation_ledger_if_missing(
    *,
    container: ContainerProxy,
    session_id: str,
    pending_ledger: dict[str, Any],
    response_hook: ResponseHook | None,
) -> None:
    try:
        await container.create_item(
            body=pending_ledger,
            retry_write=0,
            response_hook=response_hook,
        )
    except CosmosHttpResponseError as error:
        if error.status_code == 409:
            return
        if error.status_code not in RETRYABLE_STATUS_CODES:
            raise
        await _resolve_ambiguous_operation_ledger_create(
            container=container,
            session_id=session_id,
            ledger_id=pending_ledger["id"],
            original_error=error,
            response_hook=response_hook,
        )
    except (ServiceRequestError, ServiceResponseError) as error:
        await _resolve_ambiguous_operation_ledger_create(
            container=container,
            session_id=session_id,
            ledger_id=pending_ledger["id"],
            original_error=error,
            response_hook=response_hook,
        )


async def _resolve_ambiguous_operation_ledger_create(
    *,
    container: ContainerProxy,
    session_id: str,
    ledger_id: str,
    original_error: BaseException,
    response_hook: ResponseHook | None,
) -> None:
    try:
        await container.read_item(
            item=ledger_id,
            partition_key=session_id,
            response_hook=response_hook,
        )
    except CosmosHttpResponseError as read_error:
        if read_error.status_code == 404:
            raise original_error from read_error
        raise


def _validate_operation_ledger(
    ledger: Mapping[str, Any],
    *,
    session_id: str,
    payload_hash: str,
    count: int,
) -> OperationReservation | None:
    mismatched_fields = tuple(
        field
        for field, expected in (
            ("sessionId", session_id),
            ("payload_fingerprint", payload_hash),
            ("count", count),
        )
        if ledger.get(field) != expected
    )
    if mismatched_fields:
        raise CosmosSessionOperationError(
            "The operation ID is already associated with a different request.",
            code="operation_id_mismatch",
            mismatched_fields=mismatched_fields,
        )

    status = ledger.get("status")
    invalid_fields: list[str] = []
    if ledger.get("type") != "operation":
        invalid_fields.append("type")
    if ledger.get("schema_version") != _documents.SCHEMA_VERSION:
        invalid_fields.append("schema_version")
    if status not in {"pending", "reserved", "completed"}:
        invalid_fields.append("status")
    if invalid_fields:
        raise CosmosSessionOperationError(
            "The stored operation ledger is invalid.",
            code="invalid_operation_ledger",
            mismatched_fields=tuple(invalid_fields),
        )
    if status == "pending":
        return None

    incarnation = ledger.get("incarnation")
    first_seq = ledger.get("first_seq")
    message_created_at = ledger.get("message_created_at")
    reserved_fields: list[str] = []
    if not isinstance(incarnation, str) or not incarnation:
        reserved_fields.append("incarnation")
    if isinstance(first_seq, bool) or not isinstance(first_seq, int) or first_seq < 0:
        reserved_fields.append("first_seq")
    if not isinstance(message_created_at, str) or not message_created_at:
        reserved_fields.append("message_created_at")
    if reserved_fields:
        raise CosmosSessionOperationError(
            "The stored operation reservation is invalid.",
            code="invalid_operation_ledger",
            mismatched_fields=tuple(reserved_fields),
        )

    assert isinstance(incarnation, str)
    assert isinstance(first_seq, int)
    assert isinstance(message_created_at, str)
    ledger_id = ledger.get("id")
    if not isinstance(ledger_id, str):
        raise CosmosSessionOperationError(
            "The stored operation ledger has an invalid ID.",
            code="invalid_operation_ledger",
            mismatched_fields=("id",),
        )
    return OperationReservation(
        sequence=SequenceReservation(
            incarnation=incarnation,
            first_seq=first_seq,
            count=count,
        ),
        ledger_id=ledger_id,
        message_created_at=message_created_at,
        completed=status == "completed",
    )


def _validate_reservable_metadata(
    metadata: Mapping[str, Any],
    *,
    session_id: str,
) -> tuple[str, int, str]:
    incarnation = metadata.get("incarnation")
    last_seq = metadata.get("last_seq")
    etag = metadata.get("_etag")
    invalid_fields: list[str] = []
    if metadata.get("sessionId") != session_id:
        invalid_fields.append("sessionId")
    if metadata.get("type") != "meta":
        invalid_fields.append("type")
    if metadata.get("schema_version") != _documents.SCHEMA_VERSION:
        invalid_fields.append("schema_version")
    if not isinstance(incarnation, str) or not incarnation:
        invalid_fields.append("incarnation")
    if isinstance(last_seq, bool) or not isinstance(last_seq, int) or last_seq < 0:
        invalid_fields.append("last_seq")
    if not isinstance(etag, str) or not etag:
        invalid_fields.append("_etag")
    if invalid_fields:
        raise CosmosSessionConflictError(
            "The session metadata cannot be conditionally reserved.",
            code="invalid_session_metadata",
            mismatched_fields=tuple(invalid_fields),
        )
    assert isinstance(incarnation, str)
    assert isinstance(last_seq, int)
    assert isinstance(etag, str)
    return incarnation, last_seq, etag


async def reserve_sequence_block(
    *,
    container: ContainerProxy,
    session_id: str,
    ttl_seconds: int | None,
    count: int,
    create_metadata: Callable[[str], Awaitable[None]],
    response_hook: ResponseHook | None = None,
) -> SequenceReservation:
    while True:
        now = _documents.utc_now()
        patch_operations: list[dict[str, Any]] = [
            {"op": "incr", "path": "/last_seq", "value": count},
            {"op": "set", "path": "/updated_at", "value": now},
        ]
        if ttl_seconds is not None:
            patch_operations.append(
                {"op": "set", "path": "/ttl", "value": ttl_seconds}
            )
        try:
            metadata = await container.patch_item(
                item=_documents.META_ID,
                partition_key=session_id,
                patch_operations=patch_operations,
                no_response=False,
                retry_write=0,
                response_hook=response_hook,
            )
        except CosmosHttpResponseError as error:
            if error.status_code != 404:
                raise
            await create_metadata(now)
            continue

        last_seq = metadata.get("last_seq")
        incarnation = metadata.get("incarnation")
        if (
            metadata.get("schema_version") != _documents.SCHEMA_VERSION
            or not isinstance(incarnation, str)
            or not incarnation
            or isinstance(last_seq, bool)
            or not isinstance(last_seq, int)
            or last_seq < count
        ):
            raise CosmosSessionConflictError(
                "The session metadata is not valid schema-v2 metadata.",
                code="invalid_session_metadata",
                mismatched_fields=("schema_version", "incarnation", "last_seq"),
            )
        return SequenceReservation(
            incarnation=incarnation,
            first_seq=last_seq - count,
            count=count,
        )


async def rotate_session_incarnation(
    *,
    container: ContainerProxy,
    session_id: str,
    ttl_seconds: int | None,
    session_token: str | None = None,
    response_hook: ResponseHook | None = None,
) -> str:
    incarnation = _documents.new_incarnation()
    while True:
        now = _documents.utc_now()
        patch_operations: list[dict[str, Any]] = [
            {"op": "set", "path": "/schema_version", "value": _documents.SCHEMA_VERSION},
            {"op": "set", "path": "/incarnation", "value": incarnation},
            {"op": "set", "path": "/last_seq", "value": 0},
            {"op": "set", "path": "/updated_at", "value": now},
        ]
        if ttl_seconds is not None:
            patch_operations.append(
                {"op": "set", "path": "/ttl", "value": ttl_seconds}
            )
        try:
            await container.patch_item(
                item=_documents.META_ID,
                partition_key=session_id,
                patch_operations=patch_operations,
                no_response=False,
                retry_write=0,
                session_token=session_token,
                response_hook=response_hook,
            )
            return incarnation
        except CosmosHttpResponseError as error:
            if error.status_code != 404:
                raise
            created = await create_metadata_if_missing(
                container=container,
                session_id=session_id,
                ttl_seconds=ttl_seconds,
                now=now,
                incarnation=incarnation,
                session_token=session_token,
                response_hook=response_hook,
            )
            if created:
                return incarnation


async def create_metadata_if_missing(
    *,
    container: ContainerProxy,
    session_id: str,
    ttl_seconds: int | None,
    now: str,
    incarnation: str | None = None,
    response_hook: ResponseHook | None = None,
    session_token: str | None = None,
) -> bool:
    metadata: dict[str, Any] = {
        "id": _documents.META_ID,
        "sessionId": session_id,
        "type": "meta",
        "schema_version": _documents.SCHEMA_VERSION,
        "incarnation": incarnation or _documents.new_incarnation(),
        "last_seq": 0,
        "created_at": now,
        "updated_at": now,
    }
    if ttl_seconds is not None:
        metadata["ttl"] = ttl_seconds

    try:
        await container.create_item(
            body=metadata,
            retry_write=0,
            response_hook=response_hook,
            session_token=session_token,
        )
        return True
    except CosmosHttpResponseError as error:
        if error.status_code != 409:
            raise
        existing = await container.read_item(
            item=_documents.META_ID,
            partition_key=session_id,
            response_hook=response_hook,
            session_token=session_token,
        )
        last_seq = existing.get("last_seq")
        existing_incarnation = existing.get("incarnation")
        if (
            existing.get("sessionId") != session_id
            or existing.get("type") != "meta"
            or existing.get("schema_version") != _documents.SCHEMA_VERSION
            or not isinstance(existing_incarnation, str)
            or not existing_incarnation
            or isinstance(last_seq, bool)
            or not isinstance(last_seq, int)
            or last_seq < 0
        ):
            raise CosmosSessionConflictError(
                "The reserved metadata ID exists but is not valid schema-v2 metadata.",
                code="invalid_session_metadata",
                mismatched_fields=(
                    "sessionId",
                    "type",
                    "schema_version",
                    "incarnation",
                    "last_seq",
                ),
            ) from error
        return False


async def write_reserved_documents_with_retry(
    *,
    documents: Sequence[dict[str, Any]],
    max_write_attempts: int,
    delay_seconds: Callable[[BaseException, int], float],
    create_document: Callable[[dict[str, Any]], Awaitable[None]],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    on_retry: Callable[[BaseException, int, float], Awaitable[None]] | None = None,
) -> None:
    for attempt in range(max_write_attempts):
        try:
            for document in documents:
                await create_document(document)
            return
        except (CosmosHttpResponseError, ServiceRequestError, ServiceResponseError) as error:
            if attempt + 1 == max_write_attempts or not is_retryable_write_error(error):
                raise
            delay = delay_seconds(error, attempt)
            if on_retry is not None:
                await on_retry(error, attempt + 1, delay)
            await sleep(delay)

    raise AssertionError("write retry loop exited unexpectedly")


async def create_reserved_document(
    *,
    container: ContainerProxy,
    session_id: str,
    expected_document: dict[str, Any],
    response_hook: ResponseHook | None = None,
) -> None:
    try:
        await container.create_item(
            body=expected_document,
            retry_write=0,
            response_hook=response_hook,
        )
        return
    except CosmosHttpResponseError as error:
        if error.status_code != 409:
            raise

    existing = await container.read_item(
        item=expected_document["id"],
        partition_key=session_id,
        response_hook=response_hook,
    )
    if _documents.documents_match(existing, expected_document):
        return

    raise CosmosSessionConflictError(
        "A document with the reserved ID exists but does not match the item being retried.",
        code="reserved_message_mismatch",
        mismatched_fields=_documents.document_mismatched_fields(
            existing,
            expected_document,
        ),
    )


def is_retryable_write_error(error: BaseException) -> bool:
    if isinstance(error, (ServiceRequestError, ServiceResponseError)):
        return True
    return (
        isinstance(error, CosmosHttpResponseError)
        and error.status_code in RETRYABLE_STATUS_CODES
    )
