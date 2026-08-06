"""Read-only validation of the Cosmos container contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.cosmos.aio import ContainerProxy, DatabaseProxy
from azure.cosmos.exceptions import CosmosHttpResponseError

from ._tokens import ResponseHook
from .exceptions import CosmosSessionConfigurationError

_REQUIRED_INDEX_PATHS = frozenset({"/type/?", "/incarnation/?", "/seq/?"})


@dataclass(frozen=True)
class ContainerValidationResult:
    """Sanitized readiness findings for one Cosmos container."""

    issue_codes: tuple[str, ...] = ()

    @property
    def is_valid(self) -> bool:
        return not self.issue_codes

    def raise_for_invalid(self) -> None:
        if self.is_valid:
            return
        raise CosmosSessionConfigurationError(
            "The Cosmos DB container does not satisfy the session contract.",
            code="container_validation_failed",
            invalid_fields=self.issue_codes,
        )


async def validate_container(
    *,
    database: DatabaseProxy,
    container: ContainerProxy,
    ttl_seconds: int | None,
    response_hook: ResponseHook | None = None,
) -> ContainerValidationResult:
    try:
        await database.read(response_hook=response_hook)
    except (CosmosHttpResponseError, ServiceRequestError, ServiceResponseError):
        return ContainerValidationResult(("database_unreachable",))

    try:
        properties = await container.read(response_hook=response_hook)
    except (CosmosHttpResponseError, ServiceRequestError, ServiceResponseError):
        return ContainerValidationResult(("container_unreachable",))

    issues: list[str] = []
    partition_key = properties.get("partitionKey")
    partition_paths = (
        partition_key.get("paths")
        if isinstance(partition_key, Mapping)
        else None
    )
    if partition_paths != ["/sessionId"]:
        issues.append("invalid_partition_key")

    if not _has_required_index_paths(properties.get("indexingPolicy")):
        issues.append("invalid_indexing_policy")

    if ttl_seconds is not None and properties.get("defaultTtl") is None:
        issues.append("ttl_not_enabled")

    if "invalid_partition_key" not in issues and "invalid_indexing_policy" not in issues:
        query_issue = await _validate_query_shapes(
            container,
            response_hook=response_hook,
        )
        if query_issue is not None:
            issues.append(query_issue)

    return ContainerValidationResult(tuple(issues))


async def _validate_query_shapes(
    container: ContainerProxy,
    *,
    response_hook: ResponseHook | None,
) -> str | None:
    validation_partition = f"__openai_cosmosdb_validation__:{uuid4().hex}"
    queries = (
        "SELECT * FROM c WHERE c.type = 'message' "
        "AND c.incarnation = @incarnation ORDER BY c.seq ASC",
        "SELECT * FROM c WHERE c.type = 'message' AND c.incarnation = @incarnation "
        "ORDER BY c.seq DESC OFFSET 0 LIMIT @limit",
    )
    try:
        for query in queries:
            results = container.query_items(
                query=query,
                parameters=[
                    {"name": "@incarnation", "value": "validation"},
                    {"name": "@limit", "value": 1},
                ],
                partition_key=validation_partition,
                response_hook=response_hook,
            )
            async for _ in results:
                break
    except (CosmosHttpResponseError, ServiceRequestError, ServiceResponseError):
        return "query_incompatible"
    return None


def _has_required_index_paths(indexing_policy: object) -> bool:
    if not isinstance(indexing_policy, Mapping):
        return False
    indexing_mode = indexing_policy.get("indexingMode")
    if isinstance(indexing_mode, str) and indexing_mode.lower() == "none":
        return False

    included = _extract_paths(indexing_policy.get("includedPaths"))
    excluded = _extract_paths(indexing_policy.get("excludedPaths"))
    for required_path in _REQUIRED_INDEX_PATHS:
        if required_path in included:
            continue
        if "/*" in included and required_path not in excluded and "/*" not in excluded:
            continue
        return False
    return True


def _extract_paths(configured_paths: object) -> set[str]:
    if not isinstance(configured_paths, list):
        return set()
    paths: set[str] = set()
    for configured_path in configured_paths:
        if not isinstance(configured_path, Mapping):
            continue
        path: Any = configured_path.get("path")
        if isinstance(path, str):
            paths.add(path)
    return paths
