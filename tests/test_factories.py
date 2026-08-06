from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from openai_cosmosdb_session import CosmosDBSession
from openai_cosmosdb_session.exceptions import CosmosSessionConfigurationError


class _FakeContainer:
    def __init__(self) -> None:
        self.read_calls = 0
        self.query_calls = 0
        self.properties: dict[str, Any] = {
            "partitionKey": {"paths": ["/sessionId"]},
            "defaultTtl": -1,
            "indexingPolicy": {"indexingMode": "consistent", "includedPaths": [{"path": "/*"}]},
        }

    async def read(self, **_: Any) -> dict[str, Any]:
        self.read_calls += 1
        return self.properties

    def query_items(self, **_: Any) -> Any:
        self.query_calls += 1

        async def iterate() -> Any:
            if False:
                yield None

        return iterate()


class _FakeDatabase:
    def __init__(self) -> None:
        self.read_calls = 0
        self.container = _FakeContainer()

    async def read(self, **_: Any) -> dict[str, str]:
        self.read_calls += 1
        return {"id": "agents"}

    def get_container_client(self, _: str) -> _FakeContainer:
        return self.container


class _OwnedClient:
    def __init__(self) -> None:
        self.entered = False
        self.closed = False
        self.database = _FakeDatabase()

    async def __aenter__(self) -> _OwnedClient:
        self.entered = True
        return self

    def get_database_client(self, _: str) -> _FakeDatabase:
        return self.database

    async def close(self) -> None:
        self.closed = True


class FactoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_connection_string_factory_forwards_consistency_level(self) -> None:
        client = _OwnedClient()
        with patch(
            "openai_cosmosdb_session.session.CosmosClient.from_connection_string",
            return_value=client,
        ) as factory:
            session = await CosmosDBSession.from_connection_string(
                "session-1",
                "AccountEndpoint=https://example.invalid/;AccountKey=secret",
                consistency_level="Eventual",
            )

        factory.assert_called_once_with(
            "AccountEndpoint=https://example.invalid/;AccountKey=secret",
            credential=None,
            consistency_level="Eventual",
        )
        self.assertTrue(client.entered)
        self.assertEqual(client.database.read_calls, 1)
        self.assertEqual(client.database.container.read_calls, 1)
        self.assertEqual(client.database.container.query_calls, 2)
        await session.close()
        self.assertTrue(client.closed)

    async def test_token_credential_factory_is_explicit_and_owns_client(self) -> None:
        client = _OwnedClient()
        credential = object()
        with patch(
            "openai_cosmosdb_session.session.CosmosClient",
            return_value=client,
        ) as factory:
            session = await CosmosDBSession.from_token_credential(
                "session-1",
                "https://example.invalid/",
                credential,  # type: ignore[arg-type]
                validate_on_create=False,
            )

        factory.assert_called_once_with(
            "https://example.invalid/",
            credential,
            consistency_level="Session",
        )
        await session.close()
        self.assertTrue(client.closed)

    async def test_invalid_consistency_level_is_sanitized_and_builds_no_client(self) -> None:
        with patch(
            "openai_cosmosdb_session.session.CosmosClient.from_connection_string",
        ) as factory:
            with self.assertRaises(CosmosSessionConfigurationError) as raised:
                await CosmosDBSession.from_connection_string(
                    "session-1",
                    "SENTINEL-CONNECTION-STRING",
                    consistency_level="SENTINEL-INVALID-LEVEL",
                )

        factory.assert_not_called()
        error = raised.exception
        self.assertEqual(error.code, "invalid_consistency_level")
        self.assertEqual(error.invalid_fields, ("consistency_level",))
        rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
        self.assertNotIn("SENTINEL", rendered)

    async def test_factory_closes_client_when_session_construction_fails(self) -> None:
        client = _OwnedClient()
        with patch(
            "openai_cosmosdb_session.session.CosmosClient.from_connection_string",
            return_value=client,
        ):
            with self.assertRaises(ValueError):
                await CosmosDBSession.from_connection_string(
                    "",
                    "AccountEndpoint=https://example.invalid/;AccountKey=secret",
                )

        self.assertTrue(client.closed)

    async def test_factory_closes_client_when_container_validation_fails(self) -> None:
        client = _OwnedClient()
        client.database.container.properties["partitionKey"] = {"paths": ["/tenantId"]}

        with patch(
            "openai_cosmosdb_session.session.CosmosClient.from_connection_string",
            return_value=client,
        ):
            with self.assertRaises(CosmosSessionConfigurationError):
                await CosmosDBSession.from_connection_string(
                    "session-1",
                    "AccountEndpoint=https://example.invalid/;AccountKey=secret",
                )

        self.assertTrue(client.closed)

    async def test_factory_validation_can_be_explicitly_disabled(self) -> None:
        client = _OwnedClient()
        client.database.container.properties["partitionKey"] = {"paths": ["/tenantId"]}

        with patch(
            "openai_cosmosdb_session.session.CosmosClient.from_connection_string",
            return_value=client,
        ):
            session = await CosmosDBSession.from_connection_string(
                "session-1",
                "AccountEndpoint=https://example.invalid/;AccountKey=secret",
                validate_on_create=False,
            )

        self.assertEqual(client.database.read_calls, 0)
        await session.close()


if __name__ == "__main__":
    unittest.main()
