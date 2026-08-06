"""Public OpenAI Agents Session facade backed by Azure Cosmos DB."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING, Any

from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.cosmos.aio import ContainerProxy, CosmosClient, DatabaseProxy
from azure.cosmos.exceptions import CosmosHttpResponseError

from . import _documents, _history, _telemetry, _validation, _write_pipeline
from ._retry import RetryDelayPolicy
from ._tokens import ResponseHook, SessionTokenCapture, combine_response_hooks
from ._validation import ContainerValidationResult
from .exceptions import CosmosSessionConfigurationError
from .telemetry import CosmosOperationEvent, CosmosSessionObserver

if TYPE_CHECKING:
    from agents.items import TResponseInputItem
    from agents.memory.session_settings import SessionSettings

_SUPPORTED_CONSISTENCY_LEVELS = frozenset(
    {"Strong", "BoundedStaleness", "Session", "ConsistentPrefix", "Eventual"}
)


@dataclass(frozen=True)
class CosmosSessionWriteReceipt:
    """Opaque consistency and reservation metadata for a completed append."""

    incarnation: str
    first_seq: int
    count: int
    session_token: str | None = field(default=None, repr=False)


class CosmosDBSession:
    """Azure Cosmos DB implementation of the OpenAI Agents Session protocol."""

    session_id: str
    session_settings: SessionSettings | None

    def __init__(
        self,
        session_id: str,
        *,
        client: CosmosClient,
        database: str = "agents",
        container: str = "agent_sessions",
        session_settings: SessionSettings | None = None,
        ttl_seconds: int | None = None,
        max_write_attempts: int = 3,
        retry_backoff_seconds: float = 0.1,
        max_retry_delay_seconds: float = 30.0,
        observer: CosmosSessionObserver | None = None,
    ) -> None:
        if not session_id:
            raise ValueError("session_id must not be empty")
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_write_attempts < 1:
            raise ValueError("max_write_attempts must be at least 1")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must not be negative")
        if max_retry_delay_seconds < 0:
            raise ValueError("max_retry_delay_seconds must not be negative")

        self.session_id = session_id
        self.session_settings = session_settings
        self._client = client
        self._database: DatabaseProxy = client.get_database_client(database)
        self._container: ContainerProxy = self._database.get_container_client(container)
        self._ttl_seconds = ttl_seconds
        self._max_write_attempts = max_write_attempts
        self._retry_delay_policy = RetryDelayPolicy(
            base_delay_seconds=retry_backoff_seconds,
            max_delay_seconds=max_retry_delay_seconds,
        )
        self._observer = observer
        self._owns_client = False
        self._closed = False

    @classmethod
    async def from_connection_string(
        cls,
        session_id: str,
        connection_string: str,
        *,
        credential: str | dict[str, str] | None = None,
        consistency_level: str = "Session",
        validate_on_create: bool = True,
        database: str = "agents",
        container: str = "agent_sessions",
        session_settings: SessionSettings | None = None,
        ttl_seconds: int | None = None,
        max_write_attempts: int = 3,
        retry_backoff_seconds: float = 0.1,
        max_retry_delay_seconds: float = 30.0,
        observer: CosmosSessionObserver | None = None,
    ) -> CosmosDBSession:
        """Create a session that owns an async client built from a connection string."""
        consistency_level = _validate_consistency_level(consistency_level)
        client = CosmosClient.from_connection_string(
            connection_string,
            credential=credential,
            consistency_level=consistency_level,
        )
        await client.__aenter__()
        try:
            session = cls(
                session_id,
                client=client,
                database=database,
                container=container,
                session_settings=session_settings,
                ttl_seconds=ttl_seconds,
                max_write_attempts=max_write_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                max_retry_delay_seconds=max_retry_delay_seconds,
                observer=observer,
            )
            if validate_on_create:
                validation = await session.validate_container()
                validation.raise_for_invalid()
        except Exception:
            await client.close()
            raise
        session._owns_client = True
        return session

    @classmethod
    async def from_token_credential(
        cls,
        session_id: str,
        endpoint: str,
        credential: AsyncTokenCredential,
        *,
        consistency_level: str = "Session",
        validate_on_create: bool = True,
        database: str = "agents",
        container: str = "agent_sessions",
        session_settings: SessionSettings | None = None,
        ttl_seconds: int | None = None,
        max_write_attempts: int = 3,
        retry_backoff_seconds: float = 0.1,
        max_retry_delay_seconds: float = 30.0,
        observer: CosmosSessionObserver | None = None,
    ) -> CosmosDBSession:
        """Create a session that owns a client authenticated by an async token credential."""
        return await cls.from_credential(
            session_id,
            endpoint,
            credential,
            consistency_level=consistency_level,
            validate_on_create=validate_on_create,
            database=database,
            container=container,
            session_settings=session_settings,
            ttl_seconds=ttl_seconds,
            max_write_attempts=max_write_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            max_retry_delay_seconds=max_retry_delay_seconds,
            observer=observer,
        )

    @classmethod
    async def from_credential(
        cls,
        session_id: str,
        endpoint: str,
        credential: str | dict[str, str] | AsyncTokenCredential,
        *,
        consistency_level: str = "Session",
        validate_on_create: bool = True,
        database: str = "agents",
        container: str = "agent_sessions",
        session_settings: SessionSettings | None = None,
        ttl_seconds: int | None = None,
        max_write_attempts: int = 3,
        retry_backoff_seconds: float = 0.1,
        max_retry_delay_seconds: float = 30.0,
        observer: CosmosSessionObserver | None = None,
    ) -> CosmosDBSession:
        """Create a session that owns an async client built from an endpoint and credential."""
        consistency_level = _validate_consistency_level(consistency_level)
        client = CosmosClient(
            endpoint,
            credential,
            consistency_level=consistency_level,
        )
        await client.__aenter__()
        try:
            session = cls(
                session_id,
                client=client,
                database=database,
                container=container,
                session_settings=session_settings,
                ttl_seconds=ttl_seconds,
                max_write_attempts=max_write_attempts,
                retry_backoff_seconds=retry_backoff_seconds,
                max_retry_delay_seconds=max_retry_delay_seconds,
                observer=observer,
            )
            if validate_on_create:
                validation = await session.validate_container()
                validation.raise_for_invalid()
        except Exception:
            await client.close()
            raise
        session._owns_client = True
        return session

    async def get_items(self, limit: int | None = None) -> list[TResponseInputItem]:
        """Return the latest items in chronological order."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="get_items",
            action=lambda: self._get_items(limit=limit, response_hook=metrics),
            metrics=metrics,
        )

    async def get_items_with_receipt(
        self,
        receipt: CosmosSessionWriteReceipt,
        limit: int | None = None,
    ) -> list[TResponseInputItem]:
        """Read history using a prior write's Session consistency token."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="get_items_with_receipt",
            action=lambda: self._get_items(
                limit=limit,
                session_token=receipt.session_token,
                response_hook=metrics,
            ),
            metrics=metrics,
        )

    async def _get_items(
        self,
        *,
        limit: int | None,
        session_token: str | None = None,
        response_hook: ResponseHook | None = None,
    ) -> list[TResponseInputItem]:
        return await _history.get_items(
            container=self._container,
            session_id=self.session_id,
            limit=self._resolve_limit(limit),
            deserialize=self._deserialize_document,
            session_token=session_token,
            response_hook=response_hook,
        )

    async def add_items(self, items: list[TResponseInputItem]) -> None:
        """Append items, retrying only writes made from one reserved sequence block."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        await _telemetry.observe_operation(
            self._observer,
            operation="add_items",
            action=lambda: self._append_items(items, response_hook=metrics),
            metrics=metrics,
        )

    async def add_items_with_receipt(
        self,
        items: list[TResponseInputItem],
    ) -> CosmosSessionWriteReceipt:
        """Append items and return an operation-local Session consistency receipt."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="add_items_with_receipt",
            action=lambda: self._add_items_with_receipt(items, metrics=metrics),
            metrics=metrics,
        )

    async def _add_items_with_receipt(
        self,
        items: list[TResponseInputItem],
        *,
        metrics: _telemetry.ResponseMetricsCapture,
    ) -> CosmosSessionWriteReceipt:
        if not items:
            raise ValueError("items must not be empty when requesting a write receipt")

        capture = await SessionTokenCapture.create(self._container, self.session_id)
        response_hook = combine_response_hooks(capture, metrics)
        reservation = await self._append_items(items, response_hook=response_hook)
        assert reservation is not None
        return await self._build_write_receipt(reservation, capture)

    async def _append_items(
        self,
        items: list[TResponseInputItem],
        *,
        response_hook: ResponseHook | None = None,
    ) -> _write_pipeline.SequenceReservation | None:
        if not items:
            return None

        serialized_items = [await self._serialize_item(item) for item in items]
        if response_hook is None:
            reservation = await self._reserve_sequence_block(len(serialized_items))
        else:
            reservation = await self._reserve_sequence_block(
                len(serialized_items),
                response_hook=response_hook,
            )
        documents = tuple(
            self._build_message_document(
                seq=reservation.first_seq + index,
                message_data=message_data,
                incarnation=reservation.incarnation,
            )
            for index, message_data in enumerate(serialized_items)
        )

        if response_hook is None:
            await self._write_reserved_documents_with_retry(documents)
        else:
            await self._write_reserved_documents_with_retry(
                documents,
                response_hook=response_hook,
            )
        return reservation

    async def add_items_once(
        self,
        items: list[TResponseInputItem],
        *,
        operation_id: str,
    ) -> CosmosSessionWriteReceipt:
        """Append items once for a caller-provided operation ID."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="add_items_once",
            action=lambda: self._add_items_once(
                items,
                operation_id=operation_id,
                metrics=metrics,
            ),
            metrics=metrics,
        )

    async def _add_items_once(
        self,
        items: list[TResponseInputItem],
        *,
        operation_id: str,
        metrics: _telemetry.ResponseMetricsCapture,
    ) -> CosmosSessionWriteReceipt:
        if not isinstance(operation_id, str) or not operation_id.strip():
            raise ValueError("operation_id must not be empty")
        if not items:
            raise ValueError("items must not be empty for an operation-ID append")

        capture = await SessionTokenCapture.create(self._container, self.session_id)
        response_hook = combine_response_hooks(capture, metrics)
        serialized_items = [await self._serialize_item(item) for item in items]
        operation = await _write_pipeline.reserve_operation_sequence_block(
            container=self._container,
            session_id=self.session_id,
            ttl_seconds=self._ttl_seconds,
            count=len(serialized_items),
            operation_id=operation_id,
            payload_hash=_documents.payload_fingerprint(serialized_items),
            create_metadata=lambda now: self._create_metadata_if_missing(
                now,
                response_hook=response_hook,
            ),
            response_hook=response_hook,
        )
        sequence = operation.sequence
        if operation.completed:
            return await self._build_write_receipt(sequence, capture)

        documents = tuple(
            self._build_message_document(
                seq=sequence.first_seq + index,
                message_data=message_data,
                incarnation=sequence.incarnation,
                created_at=operation.message_created_at,
            )
            for index, message_data in enumerate(serialized_items)
        )
        await self._write_reserved_documents_with_retry(
            documents,
            response_hook=response_hook,
        )
        await _write_pipeline.complete_operation(
            container=self._container,
            session_id=self.session_id,
            ledger_id=operation.ledger_id,
            response_hook=response_hook,
        )
        return await self._build_write_receipt(sequence, capture)

    async def pop_item(self) -> TResponseInputItem | None:
        """Remove and return the most recent valid item."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="pop_item",
            action=lambda: self._pop_item(response_hook=metrics),
            metrics=metrics,
        )

    async def pop_item_with_receipt(
        self,
        receipt: CosmosSessionWriteReceipt,
    ) -> TResponseInputItem | None:
        """Pop history using a prior write's Session consistency token."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="pop_item_with_receipt",
            action=lambda: self._pop_item(
                session_token=receipt.session_token,
                response_hook=metrics,
            ),
            metrics=metrics,
        )

    async def _pop_item(
        self,
        *,
        session_token: str | None = None,
        response_hook: ResponseHook | None = None,
    ) -> TResponseInputItem | None:
        return await _history.pop_item(
            container=self._container,
            session_id=self.session_id,
            deserialize=self._deserialize_document,
            session_token=session_token,
            response_hook=response_hook,
        )

    async def clear_session(self) -> None:
        """Fence off current messages, then clean up documents from old incarnations."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        await _telemetry.observe_operation(
            self._observer,
            operation="clear_session",
            action=lambda: self._clear_session(response_hook=metrics),
            metrics=metrics,
        )

    async def clear_session_with_receipt(
        self,
        receipt: CosmosSessionWriteReceipt,
    ) -> None:
        """Clear history using a prior write's Session consistency token."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        await _telemetry.observe_operation(
            self._observer,
            operation="clear_session_with_receipt",
            action=lambda: self._clear_session(
                session_token=receipt.session_token,
                response_hook=metrics,
            ),
            metrics=metrics,
        )

    async def _clear_session(
        self,
        *,
        session_token: str | None = None,
        response_hook: ResponseHook | None = None,
    ) -> None:
        incarnation = await _write_pipeline.rotate_session_incarnation(
            container=self._container,
            session_id=self.session_id,
            ttl_seconds=self._ttl_seconds,
            session_token=session_token,
            response_hook=response_hook,
        )
        await _history.clear_session(
            container=self._container,
            session_id=self.session_id,
            current_incarnation=incarnation,
            session_token=session_token,
            response_hook=response_hook,
        )

    async def delete_session(self) -> None:
        """Physically delete a quiescent session, including its fence metadata."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        await _telemetry.observe_operation(
            self._observer,
            operation="delete_session",
            action=lambda: _history.delete_session(
                container=self._container,
                session_id=self.session_id,
                response_hook=metrics,
            ),
            metrics=metrics,
        )

    async def close(self) -> None:
        """Close a client created by a factory and mark this session closed."""
        if self._closed:
            return
        if self._owns_client:
            await self._client.close()
        self._closed = True

    async def ping(self) -> bool:
        """Return whether the configured database can be read."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        started_at = monotonic()
        try:
            await self._database.read(response_hook=metrics)
        except (
            CosmosHttpResponseError,
            ServiceRequestError,
            ServiceResponseError,
        ) as error:
            await _telemetry.emit_event(
                self._observer,
                _telemetry.failure_event(
                    operation="ping",
                    duration_seconds=monotonic() - started_at,
                    error=error,
                    request_charge=metrics.request_charge,
                ),
            )
            return False
        await _telemetry.emit_event(
            self._observer,
            CosmosOperationEvent(
                operation="ping",
                outcome="success",
                duration_seconds=monotonic() - started_at,
                request_charge=metrics.request_charge,
            ),
        )
        return True

    async def validate_container(self) -> ContainerValidationResult:
        """Validate the configured container without writing application data."""
        self._check_not_closed()
        metrics = _telemetry.ResponseMetricsCapture()
        return await _telemetry.observe_operation(
            self._observer,
            operation="validate_container",
            action=lambda: _validation.validate_container(
                database=self._database,
                container=self._container,
                ttl_seconds=self._ttl_seconds,
                response_hook=metrics,
            ),
            metrics=metrics,
        )

    async def _serialize_item(self, item: TResponseInputItem) -> str:
        return await _documents.serialize_item(item)

    async def _deserialize_item(self, message_data: str) -> TResponseInputItem:
        return await _documents.deserialize_item(message_data)

    async def _deserialize_document(
        self,
        document: Mapping[str, Any],
    ) -> TResponseInputItem:
        return await _documents.deserialize_document(document, self._deserialize_item)

    def _resolve_limit(self, explicit_limit: int | None) -> int | None:
        if explicit_limit is not None:
            return explicit_limit
        if self.session_settings is not None:
            return self.session_settings.limit
        return None

    def _check_not_closed(self) -> None:
        if self._closed:
            raise RuntimeError("CosmosDBSession is closed")

    async def _reserve_sequence_block(
        self,
        count: int,
        *,
        response_hook: ResponseHook | None = None,
    ) -> _write_pipeline.SequenceReservation:
        return await _write_pipeline.reserve_sequence_block(
            container=self._container,
            session_id=self.session_id,
            ttl_seconds=self._ttl_seconds,
            count=count,
            create_metadata=(
                self._create_metadata_if_missing
                if response_hook is None
                else lambda now: self._create_metadata_if_missing(
                    now,
                    response_hook=response_hook,
                )
            ),
            response_hook=response_hook,
        )

    async def _create_metadata_if_missing(
        self,
        now: str,
        *,
        response_hook: ResponseHook | None = None,
    ) -> None:
        await _write_pipeline.create_metadata_if_missing(
            container=self._container,
            session_id=self.session_id,
            ttl_seconds=self._ttl_seconds,
            now=now,
            response_hook=response_hook,
        )

    def _build_message_document(
        self,
        *,
        seq: int,
        message_data: str,
        incarnation: str,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        return _documents.build_message_document(
            seq=seq,
            message_data=message_data,
            session_id=self.session_id,
            incarnation=incarnation,
            ttl_seconds=self._ttl_seconds,
            created_at=created_at,
        )

    async def _write_reserved_documents_with_retry(
        self,
        documents: Sequence[dict[str, Any]],
        *,
        response_hook: ResponseHook | None = None,
    ) -> None:
        async def create_document(document: dict[str, Any]) -> None:
            await self._create_reserved_document(
                document,
                response_hook=response_hook,
            )

        await _write_pipeline.write_reserved_documents_with_retry(
            documents=documents,
            max_write_attempts=self._max_write_attempts,
            delay_seconds=self._retry_delay_policy.delay_seconds,
            create_document=create_document,
            on_retry=self._emit_write_retry,
        )

    async def _emit_write_retry(
        self,
        error: BaseException,
        retry_attempt: int,
        retry_delay_seconds: float,
    ) -> None:
        await _telemetry.emit_event(
            self._observer,
            _telemetry.retry_event(
                operation="write_documents",
                error=error,
                retry_attempt=retry_attempt,
                retry_delay_seconds=retry_delay_seconds,
            ),
        )

    async def _create_reserved_document(
        self,
        expected_document: dict[str, Any],
        *,
        response_hook: ResponseHook | None = None,
    ) -> None:
        await _write_pipeline.create_reserved_document(
            container=self._container,
            session_id=self.session_id,
            expected_document=expected_document,
            response_hook=response_hook,
        )

    async def _build_write_receipt(
        self,
        reservation: _write_pipeline.SequenceReservation,
        capture: SessionTokenCapture,
    ) -> CosmosSessionWriteReceipt:
        return CosmosSessionWriteReceipt(
            incarnation=reservation.incarnation,
            first_seq=reservation.first_seq,
            count=reservation.count,
            session_token=await capture.merged_token(),
        )


def _validate_consistency_level(consistency_level: str) -> str:
    if consistency_level not in _SUPPORTED_CONSISTENCY_LEVELS:
        raise CosmosSessionConfigurationError(
            "The Cosmos DB consistency level is not supported.",
            code="invalid_consistency_level",
            invalid_fields=("consistency_level",),
        )
    return consistency_level
