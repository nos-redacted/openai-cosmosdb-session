from __future__ import annotations

import copy
import unittest
from typing import Any

from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session import CosmosDBSession
from openai_cosmosdb_session.exceptions import CosmosSessionConfigurationError


class _ValidationContainer:
    def __init__(self) -> None:
        self.properties: dict[str, Any] = {
            "partitionKey": {"paths": ["/sessionId"]},
            "defaultTtl": -1,
            "indexingPolicy": {
                "indexingMode": "consistent",
                "includedPaths": [
                    {"path": "/type/?"},
                    {"path": "/incarnation/?"},
                    {"path": "/seq/?"},
                ],
                "excludedPaths": [{"path": "/*"}],
            },
        }
        self.queries: list[tuple[str, str]] = []
        self.fail_queries = False

    async def read(self, **_: Any) -> dict[str, Any]:
        return copy.deepcopy(self.properties)

    def query_items(
        self,
        *,
        query: str,
        partition_key: str,
        **_: Any,
    ) -> Any:
        self.queries.append((query, partition_key))

        async def iterate() -> Any:
            if self.fail_queries:
                raise CosmosHttpResponseError(
                    status_code=400,
                    message="SENTINEL-SERVICE-DETAIL",
                )
            if False:
                yield None

        return iterate()


class _ValidationDatabase:
    def __init__(self, container: _ValidationContainer) -> None:
        self.container = container

    async def read(self, **_: Any) -> dict[str, str]:
        return {"id": "agents"}

    def get_container_client(self, _: str) -> _ValidationContainer:
        return self.container


class _ValidationClient:
    def __init__(self, container: _ValidationContainer) -> None:
        self.database = _ValidationDatabase(container)

    def get_database_client(self, _: str) -> _ValidationDatabase:
        return self.database


class ContainerValidationTests(unittest.IsolatedAsyncioTestCase):
    def _session(
        self,
        container: _ValidationContainer,
        *,
        ttl_seconds: int | None = None,
    ) -> CosmosDBSession:
        return CosmosDBSession(
            "session-1",
            client=_ValidationClient(container),  # type: ignore[arg-type]
            ttl_seconds=ttl_seconds,
        )

    async def test_valid_container_exercises_both_ordered_queries_without_writes(self) -> None:
        container = _ValidationContainer()

        result = await self._session(container, ttl_seconds=60).validate_container()

        self.assertTrue(result.is_valid)
        self.assertEqual(result.issue_codes, ())
        self.assertEqual(len(container.queries), 2)
        self.assertIn("ASC", container.queries[0][0])
        self.assertIn("DESC", container.queries[1][0])
        self.assertNotEqual(container.queries[0][1], "session-1")

    async def test_partition_key_and_ttl_mismatches_are_reported(self) -> None:
        container = _ValidationContainer()
        container.properties["partitionKey"] = {"paths": ["/tenantId"]}
        del container.properties["defaultTtl"]

        result = await self._session(container, ttl_seconds=60).validate_container()

        self.assertFalse(result.is_valid)
        self.assertEqual(
            result.issue_codes,
            ("invalid_partition_key", "ttl_not_enabled"),
        )

    async def test_query_failure_is_sanitized_and_can_fail_closed(self) -> None:
        container = _ValidationContainer()
        container.fail_queries = True

        result = await self._session(container).validate_container()

        self.assertEqual(result.issue_codes, ("query_incompatible",))
        with self.assertRaises(CosmosSessionConfigurationError) as raised:
            result.raise_for_invalid()
        error = raised.exception
        self.assertEqual(error.code, "container_validation_failed")
        self.assertNotIn("SENTINEL", repr(vars(error)))


if __name__ == "__main__":
    unittest.main()
