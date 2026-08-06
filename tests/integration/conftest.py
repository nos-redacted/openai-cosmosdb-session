from __future__ import annotations

import os
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from azure.cosmos.aio import ContainerProxy, CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

from openai_cosmosdb_session import CosmosDBSession


@dataclass
class LiveCosmos:
    client: CosmosClient
    database_name: str
    container_name: str
    container: ContainerProxy
    container_properties: dict[str, Any]
    auth_mode: str
    client_factory: Callable[[], CosmosClient]
    session_ids: set[str] = field(default_factory=set)
    additional_clients: list[CosmosClient] = field(default_factory=list)

    def new_session_id(self, label: str) -> str:
        session_id = f"openai-cosmosdb-live-{label}-{uuid4().hex}"
        self.session_ids.add(session_id)
        return session_id

    def session(
        self,
        session_id: str,
        *,
        ttl_seconds: int | None = None,
        max_write_attempts: int = 3,
        retry_backoff_seconds: float = 0.1,
    ) -> CosmosDBSession:
        self.session_ids.add(session_id)
        return CosmosDBSession(
            session_id,
            client=self.client,
            database=self.database_name,
            container=self.container_name,
            ttl_seconds=ttl_seconds,
            max_write_attempts=max_write_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )

    async def independent_session(
        self,
        session_id: str,
        *,
        ttl_seconds: int | None = None,
    ) -> CosmosDBSession:
        self.session_ids.add(session_id)
        client = self.client_factory()
        await client.__aenter__()
        self.additional_clients.append(client)
        return CosmosDBSession(
            session_id,
            client=client,
            database=self.database_name,
            container=self.container_name,
            ttl_seconds=ttl_seconds,
        )


@pytest_asyncio.fixture
async def live_cosmos() -> Any:
    if os.getenv("COSMOS_LIVE_TESTS") != "1":
        pytest.skip("set COSMOS_LIVE_TESTS=1 to run tests against Azure Cosmos DB")

    auth_mode = os.getenv("COSMOS_AUTH_MODE")
    valid_auth_modes = {"connection-string", "developer", "azure-cli", "managed-identity"}
    if auth_mode not in valid_auth_modes:
        pytest.fail(
            "COSMOS_AUTH_MODE must be one of connection-string, developer, "
            "azure-cli, or managed-identity when live tests are enabled"
        )

    connection_string = os.getenv("COSMOS_CONNECTION_STRING")
    endpoint = os.getenv("COSMOS_ENDPOINT")
    credential: Any = None
    if auth_mode == "connection-string":
        if not connection_string or endpoint:
            pytest.fail(
                "connection-string auth requires COSMOS_CONNECTION_STRING "
                "and forbids COSMOS_ENDPOINT"
            )

        def client_factory() -> CosmosClient:
            assert connection_string is not None
            return CosmosClient.from_connection_string(
                connection_string,
                consistency_level="Session",
            )

    else:
        if not endpoint or connection_string:
            pytest.fail(
                f"{auth_mode} auth requires COSMOS_ENDPOINT and forbids COSMOS_CONNECTION_STRING"
            )
        try:
            if auth_mode == "developer":
                from azure.identity.aio import DefaultAzureCredential

                credential = DefaultAzureCredential(
                    exclude_managed_identity_credential=True,
                )
            elif auth_mode == "azure-cli":
                from azure.identity.aio import AzureCliCredential

                credential = AzureCliCredential()
            else:
                from azure.identity.aio import ManagedIdentityCredential

                credential = ManagedIdentityCredential(
                    client_id=os.getenv("AZURE_CLIENT_ID"),
                )
        except ImportError:
            pytest.fail("install the identity extra for token-based live authentication")

        def client_factory() -> CosmosClient:
            assert endpoint is not None
            return CosmosClient(
                endpoint,
                credential,
                consistency_level="Session",
            )

    client = client_factory()

    database_name = os.getenv("COSMOS_DATABASE") or "agents"
    container_name = os.getenv("COSMOS_CONTAINER") or "agent_sessions"
    await client.__aenter__()
    live: LiveCosmos | None = None
    try:
        database = client.get_database_client(database_name)
        await database.read()
        container = database.get_container_client(container_name)
        properties = dict(await container.read())
        partition_key = properties.get("partitionKey")
        paths = partition_key.get("paths") if isinstance(partition_key, Mapping) else None
        if paths != ["/sessionId"]:
            pytest.fail(
                f"{database_name}/{container_name} must use partition key /sessionId; "
                f"found {paths!r}"
            )

        live = LiveCosmos(
            client=client,
            database_name=database_name,
            container_name=container_name,
            container=container,
            container_properties=properties,
            auth_mode=auth_mode,
            client_factory=client_factory,
        )
        yield live
    finally:
        if live is not None:
            for session_id in live.session_ids:
                try:
                    await live.session(session_id).delete_session()
                except CosmosHttpResponseError as error:
                    if error.status_code != 404:
                        warnings.warn(
                            f"Could not clean live test session {session_id}: {error}",
                            stacklevel=1,
                        )
            for additional_client in live.additional_clients:
                await additional_client.close()
        await client.close()
        if credential is not None:
            await credential.close()
