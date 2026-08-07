# Quickstart: OpenAI Agents with Azure Cosmos DB

This guide creates a two-turn OpenAI Agents SDK conversation, closes the Cosmos DB client,
reopens the same session, and verifies that the second turn can use the persisted history. It
then explains how to choose an Azure Cosmos DB for NoSQL indexing policy for several session
workloads.

The local path uses an account connection string because it has the fewest setup steps. Use
Microsoft Entra authentication and managed identity for deployed applications.

## Prerequisites

- Python 3.10 or later. Use Python 3.13 for the current production validation baseline.
- An Azure Cosmos DB account using the API for NoSQL.
- Permission to create a database and container, or an existing compatible container.
- An OpenAI API key.
- A checkout of this repository to run the bundled example.

The package does not provision resources when a session is constructed. Create the database and
container once through deployment infrastructure, the Azure portal, or a separate provisioning
script.

## 1. Install the package

From this repository checkout:

```powershell
python -m pip install .
```

Before the package is available from PyPI, a consuming project can pin a reviewed Git commit.
Replace `<reviewed-commit-sha>` with the full commit SHA that your application has tested:

```powershell
python -m pip install "openai-cosmosdb-session @ git+https://github.com/nos-redacted/openai-cosmosdb-session.git@<reviewed-commit-sha>"
```

After version 0.2.0 is published, pin the release instead:

```powershell
python -m pip install "openai-cosmosdb-session==0.2.0"
```

Add the `identity` extra when using Microsoft Entra credentials:

```powershell
python -m pip install "openai-cosmosdb-session[identity]==0.2.0"
```

## 2. Create the Cosmos DB container

The container contract is:

- Partition key: exactly `/sessionId`
- Indexing mode: `consistent`
- Required indexed paths: `/seq/?`, `/type/?`, and `/incarnation/?`
- Container TTL: `-1` when sessions may set `ttl_seconds`

`default_ttl=-1` enables per-item TTL without expiring every item by default. A session only writes
item-level `ttl` values when its `ttl_seconds` argument is set.

The following one-time script creates the default `agents` database and `agent_sessions`
container. It does not modify an existing container with the same name, so validate an existing
container separately.

```python
import asyncio
import os

from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient


async def main() -> None:
    connection_string = os.environ["COSMOS_CONNECTION_STRING"]
    async with CosmosClient.from_connection_string(connection_string) as client:
        database = await client.create_database_if_not_exists(id="agents")
        await database.create_container_if_not_exists(
            id="agent_sessions",
            partition_key=PartitionKey(path="/sessionId"),
            default_ttl=-1,
            indexing_policy={
                "automatic": True,
                "indexingMode": "consistent",
                "includedPaths": [
                  {"path": "/seq/?"},
                    {"path": "/type/?"},
                    {"path": "/incarnation/?"},
                ],
                "excludedPaths": [{"path": "/*"}],
            },
        )


asyncio.run(main())
```

The partition key cannot be changed in place. If an existing container does not use exactly
`/sessionId`, create a new container and migrate or discard the old data.

## 3. Set local environment variables

PowerShell:

```powershell
$env:OPENAI_API_KEY = "<openai-api-key>"
$env:COSMOS_CONNECTION_STRING = "<cosmos-connection-string>"
$env:COSMOS_DATABASE = "agents"
$env:COSMOS_CONTAINER = "agent_sessions"
```

Bash:

```bash
export OPENAI_API_KEY="<openai-api-key>"
export COSMOS_CONNECTION_STRING="<cosmos-connection-string>"
export COSMOS_DATABASE="agents"
export COSMOS_CONTAINER="agent_sessions"
```

Keep both secrets in a local secret store or deployment secret provider. Never commit them. You
can set `COSMOS_SESSION_ID` to resume a known test conversation; otherwise the example generates
a unique ID and prints it.

## 4. Run two persisted turns

Run the bundled script from the repository root:

```powershell
python examples/agents_quickstart.py
```

The example:

1. Opens a `CosmosDBSession` and asks which city contains the Golden Gate Bridge.
2. Closes the factory-owned Cosmos client.
3. Reopens the same session ID and confirms that stored items exist.
4. Asks a context-dependent second question: "What state is it in?"
5. Reads and reports the final persisted history count.

The exact response text and item count can vary by model and Agents SDK version. A run should show
a nonzero restored history before the second turn. The session remains in Cosmos DB for inspection
by default. Delete it at the end of the same run with:

```powershell
python examples/agents_quickstart.py --cleanup
```

To use a 30-day idle TTL for the quickstart documents:

```powershell
python examples/agents_quickstart.py --ttl-seconds 2592000
```

The container must have TTL enabled for that command. The factory validates this before the first
`Runner.run()` or model call begins.

## What the Agents SDK persists

For each `Runner.run(..., session=session)` call, the Agents SDK reads session history before the
model call and stores the new turn afterward. The package can store user input, assistant output,
tool calls, and other Agents SDK input items, so one turn does not necessarily equal two Cosmos
documents.

Internal transport retries are safe: one `add_items()` invocation serializes once, reserves one
sequence block, builds deterministic documents, and retries only those writes. This does not make
separate caller invocations idempotent. Repeating `Runner.run()` or calling `add_items()` again
creates a new append. Use the advanced `add_items_once(..., operation_id=...)` API only when the
caller has a stable operation ID and explicitly needs deduplication. Within the same session,
reusing that operation ID with the same serialized payload remains deduplicated across close and
reopen while its operation ledger is retained. Reusing the ID with different content fails.

Do not combine an Agents SDK session with `conversation_id`, `previous_response_id`, or
`auto_previous_response_id` in the same run. Those are separate continuation mechanisms.

## Production authentication

Connection strings are suitable for a private local evaluation. For local passwordless
development, use `DefaultAzureCredential`. For Azure-hosted workloads, use an explicit
`ManagedIdentityCredential` and grant that identity the **Cosmos DB Built-in Data Contributor**
data-plane role at the narrowest useful scope.

Set the endpoint for either token-credential flow. Set `AZURE_CLIENT_ID` when the deployed workload
uses a user-assigned managed identity:

```powershell
$env:COSMOS_ENDPOINT = "https://<account>.documents.azure.com:443/"
$env:AZURE_CLIENT_ID = "<user-assigned-managed-identity-client-id>"
```

```python
import os

from agents import Agent, Runner
from azure.identity.aio import ManagedIdentityCredential
from openai_cosmosdb_session import CosmosDBSession


credential = ManagedIdentityCredential(client_id=os.environ["AZURE_CLIENT_ID"])
session = await CosmosDBSession.from_token_credential(
    "conversation-123",
    os.environ["COSMOS_ENDPOINT"],
    credential,
    database=os.environ.get("COSMOS_DATABASE", "agents"),
    container=os.environ.get("COSMOS_CONTAINER", "agent_sessions"),
)

try:
    agent = Agent(name="Assistant", instructions="Reply concisely.")
    result = await Runner.run(agent, "Remember that my region is west.", session=session)
    print(result.final_output)
finally:
    await session.close()
    await credential.close()
```

For local developer sign-in, replace the managed-identity construction with:

```python
from azure.identity.aio import DefaultAzureCredential

credential = DefaultAzureCredential()
```

The endpoint, session construction, `finally` block, and credential ownership stay the same.

The session factory owns and closes the Cosmos client it creates. The application owns and closes
the credential. In a long-running service, create one async `CosmosClient` per process and inject
it into short-lived `CosmosDBSession` objects instead of creating a client per request.

## Indexing guidance

The policies below apply to Azure Cosmos DB for NoSQL. Other Cosmos DB APIs have different index
models.

### Microsoft guidance and the package-specific exception

Microsoft documents these general rules:

- A new container indexes every property by default using range indexes for string and numeric
  values. This is a good starting point while a schema and its queries are evolving.
- Microsoft generally recommends an **opt-out** policy: include `/*`, then exclude only paths that
  should not be indexed. New properties become queryable without another policy change.
- An **opt-in** policy excludes `/*` and includes selected paths. It can reduce index storage,
  write latency, and write RU charge, but every future query path must be added deliberately.
- Use `consistent` indexing for queried containers. `none` is for pure key-value or controlled
  bulk-load cases. TTL cannot be enabled when indexing mode is `none`.
- Scalar paths end in `/?`; `/*` covers descendants. In consistent mode, `id` and `_ts` are always
  indexed. `_etag` is excluded unless explicitly included.
- A single-property `ORDER BY` needs a range index on that property. An `ORDER BY` over two or more
  properties needs a matching composite index. Composite path order and direction must match the
  query, or be the exact inverse direction for every path.
- Index metrics identify utilized and potential paths, but potential paths are recommendations,
  not proof of a performance improvement. Enable metrics only while troubleshooting and confirm
  changes with request charge and latency measurements.

This package deliberately uses an opt-in policy as an exception to the general recommendation.
Its internal schema and query set are fixed, all history queries are routed to one `/sessionId`
logical partition, and large serialized `message_data` values are never queried by the package.

### Built-in query contract

The package executes these history query shapes with `partition_key=session_id` on the request.
Full-history and latest-N reads use:

```sql
SELECT *
FROM c
WHERE c.type = 'message' AND c.incarnation = @incarnation
ORDER BY c.seq ASC
```

```sql
SELECT *
FROM c
WHERE c.type = 'message' AND c.incarnation = @incarnation
ORDER BY c.seq DESC
OFFSET 0 LIMIT @limit
```

`pop_item()` selects the newest current-incarnation message:

```sql
SELECT TOP 1 *
FROM c
WHERE c.type = 'message' AND c.incarnation = @incarnation
ORDER BY c.seq DESC
```

Old-incarnation cleanup after a clear selects message IDs and incarnations:

```sql
SELECT c.id, c.incarnation
FROM c
WHERE c.type = 'message'
```

Metadata, operation ledgers, and individual messages are otherwise accessed by point read or
write using both item ID and partition key.

The baseline policy is therefore:

```json
{
  "automatic": true,
  "indexingMode": "consistent",
  "includedPaths": [
    { "path": "/seq/?" },
    { "path": "/type/?" },
    { "path": "/incarnation/?" }
  ],
  "excludedPaths": [
    { "path": "/*" }
  ]
}
```

The three included paths serve document filtering, clear-incarnation fencing, and chronological
ordering. The package does not query `/message_data/?`, `/created_at/?`, or `/ttl/?`, so the
baseline excludes them. Cosmos DB TTL processing does not require `/ttl/?` to be indexed.

No composite index is required for the current package queries. They order on one property, and
the package's SQL does not use a multi-property `ORDER BY` that would match a composite index. Do
not add a speculative composite index without confirming utilization and lower RU charge on
production-like data.

Microsoft notes that a partition key path is not automatically a secondary query index. The
package routes every request with the partition key value and does not use a SQL predicate on
`c.sessionId`, so `/sessionId/?` is not required by its built-in operations. Explicitly index that
path if application-owned SQL queries filter on `c.sessionId` instead of relying only on partition
routing.

## Scenario policies

Use this table as a starting point, then measure the real workload.

| Scenario | Recommended starting point |
| --- | --- |
| Dedicated, stable session container | Narrow package baseline |
| High aggregate write volume | Narrow package baseline plus throughput and partition monitoring |
| Evolving or shared document schema | Microsoft-style opt-out policy |
| Ephemeral sessions | Narrow baseline plus container and item TTL |
| Tenant reporting and administration | Separate session-catalog container with query-specific indexes |
| Full-text, vector, or analytical retrieval | Separate projection/search store |
| One-time bulk import | Offline staging container with indexing temporarily disabled |

### Scenario 1: Dedicated session container

Use the baseline opt-in policy unchanged when the container is dedicated to this package and the
application only uses its public session operations. This is the recommended default.

```json
{
  "automatic": true,
  "indexingMode": "consistent",
  "includedPaths": [
    { "path": "/seq/?" },
    { "path": "/type/?" },
    { "path": "/incarnation/?" }
  ],
  "excludedPaths": [{ "path": "/*" }]
}
```

Benefits: the large message payload and bookkeeping fields do not add ordinary range-index work.
Tradeoff: application-defined queries over new properties are unsupported until their paths are
added and the index transformation completes.

### Scenario 2: High write volume or many concurrent sessions

Keep the same narrow policy. Additional indexes increase write work and do not improve the fixed
session queries unless query metrics prove otherwise.

Scale aggregate throughput by using high-cardinality session IDs and enough provisioned or
autoscale throughput. Indexing cannot remove a hot logical partition: all writes for one session
must use the same `/sessionId` value to preserve ordering and fencing. Monitor throttling, request
charge, logical partition size, and latency separately for broad traffic and unusually active
single sessions. Use the repository load harness on a dedicated TTL-enabled container before
setting capacity or regression thresholds.

### Scenario 3: Evolving or genuinely shared schema

If other application document types share the container and their future query fields are not yet
known, prefer Microsoft's opt-out model. Keep the root indexed and exclude only known large,
unqueried fields:

```json
{
  "automatic": true,
  "indexingMode": "consistent",
  "includedPaths": [{ "path": "/*" }],
  "excludedPaths": [{ "path": "/message_data/?" }]
}
```

This remains compatible with the package because `/type/?`, `/incarnation/?`, and `/seq/?` are
covered by the root and are not excluded. It normally costs more write RU and index storage than
the dedicated policy, but newly introduced scalar fields are indexed proactively.

Do not share merely to reduce the number of containers. A shared policy, throughput budget, TTL
configuration, and operational blast radius now apply to every document type. Prefer a dedicated
session container when schemas, retention rules, or ownership differ.

### Scenario 4: Ephemeral sessions and TTL

Use the narrow baseline policy and enable TTL on the container:

```python
await database.create_container_if_not_exists(
    id="agent_sessions",
    partition_key=PartitionKey(path="/sessionId"),
    default_ttl=-1,
    indexing_policy=baseline_indexing_policy,
)
```

Then set a positive item TTL on each session:

```python
session = await CosmosDBSession.from_connection_string(
    "conversation-123",
    connection_string,
    ttl_seconds=30 * 24 * 60 * 60,
)
```

Do not switch the container to indexing mode `none`; Cosmos DB does not allow TTL with that mode.
TTL expiration is eventual and item-by-item. Older messages can expire before newer messages, and
TTL does not atomically delete a whole logical session. On provisioned-throughput accounts,
background deletion uses RUs left over from foreground work and can be delayed when none are
available. Serverless accounts charge expired-item deletion like ordinary delete operations.

### Scenario 5: Multi-tenant administration and reporting

Keep conversation history in the dedicated package container. Use globally unique, opaque session
IDs because `/sessionId` is both the partition boundary and the package's isolation key. Cosmos DB
RBAC does not create a separate tenant authorization boundary for each logical partition, so
enforce tenant authorization before constructing or accepting a session ID.

For queries such as "list a tenant's active sessions by most recent update," maintain a separate
application-owned catalog container. A suitable catalog document might contain `tenantId`,
`sessionId`, `status`, and `updatedAt`, with partition key `/tenantId`. For this query:

```sql
SELECT c.sessionId, c.updatedAt
FROM c
WHERE c.status = @status
ORDER BY c.status ASC, c.updatedAt DESC
```

start with this catalog policy:

```json
{
  "automatic": true,
  "indexingMode": "consistent",
  "includedPaths": [
    { "path": "/status/?" },
    { "path": "/updatedAt/?" },
    { "path": "/sessionId/?" }
  ],
  "excludedPaths": [{ "path": "/*" }],
  "compositeIndexes": [
    [
      { "path": "/status", "order": "ascending" },
      { "path": "/updatedAt", "order": "descending" }
    ]
  ]
}
```

Pass the tenant ID as the request partition key. The equality field appears first in the
`ORDER BY` so the query can use the matching composite index. Add other catalog indexes only for
documented queries, and verify each with index metrics and request charge. Do not add catalog
fields to the package's internal message, metadata, or operation documents.

### Scenario 6: Full-text, vector, semantic search, or analytics

Do not query the package's `message_data` field for search. It is an opaque serialized Agents SDK
item, not a stable search schema. Project approved, normalized text and metadata into a separate
container or search service. This also lets the application apply independent retention, access
control, redaction, and encryption policies.

A separate search container might start with this indexing-policy fragment:

```json
{
  "automatic": true,
  "indexingMode": "consistent",
  "includedPaths": [
    { "path": "/tenantId/?" },
    { "path": "/sessionId/?" },
    { "path": "/createdAt/?" },
    { "path": "/text/?" }
  ],
  "excludedPaths": [{ "path": "/*" }],
  "fullTextIndexes": [{ "path": "/text" }],
  "vectorIndexes": [{ "path": "/embedding", "type": "diskANN" }]
}
```

This fragment is not sufficient by itself. Full-text and vector search must be enabled for the
account, and the container needs matching full-text and vector embedding policies. Vector policy
configuration has creation-time and replacement constraints, so design and test the projection
container independently. Use Azure AI Search, Microsoft Fabric, Synapse Link, or another analytical
store when the access pattern is cross-session reporting rather than transactional retrieval.

### Scenario 7: One-time bulk import or backfill

For a new, offline staging container with no TTL requirement, indexing can be temporarily disabled
to reduce bulk-ingest work:

```json
{
  "indexingMode": "none"
}
```

After loading, apply the final `consistent` policy, wait for index transformation to reach 100%,
run `validate_container()`, and only then direct session traffic to the container. Never disable
indexing on an active session container. Do not construct a `CosmosDBSession` against the staging
container or bypass its readiness validation during the unindexed phase. Do not use this approach
when TTL is enabled.

## Change an indexing policy safely

Cosmos DB transforms ordinary range and composite index changes online and in place. Reads and
writes remain available, but the transformation consumes RUs at a lower priority than foreground
operations. The duration depends on throughput, item count, and item size.

Use this sequence:

1. Inventory every production query, including the package's fixed queries and application-owned
   queries.
2. Record request charge and latency on representative data before changing the policy.
3. Add new paths or composite indexes in one policy update.
4. Wait for index transformation to complete. Queries do not use a newly added index until then.
5. Run `validate_container()`, representative queries, and load checks.
6. Confirm actual utilization and lower cost with temporary index metrics.
7. Remove obsolete paths in a later, grouped update only after replacements are active.

Removing an indexed path takes effect immediately and can make a query scan or fail. When replacing
an index, always add the replacement, wait for completion, and remove the old index afterward.

The Azure portal exposes policy editing under **Data Explorer > Container > Settings > Indexing
Policy**. SDKs can also update the policy and report the
`x-ms-documentdb-collection-index-transformation-progress` response header. Treat policy JSON as
deployment-controlled infrastructure and review it like application code.

## Validate compatibility

Owned-client factories validate the container by default. You can also validate explicitly:

```python
result = await session.validate_container()
result.raise_for_invalid()
```

Validation checks database and container access, exact `/sessionId`, all three required index
paths, TTL support when `ttl_seconds` is configured, and both ordered history queries against an
unused logical partition. It does not write application data.

Common issue codes are:

| Issue code | Action |
| --- | --- |
| `database_unreachable` | Check account endpoint, credentials, firewall, and data-plane role. |
| `container_unreachable` | Check database/container names and container-level access. |
| `invalid_partition_key` | Create a new container with exactly `/sessionId`. |
| `invalid_indexing_policy` | Restore all required paths and consistent indexing. |
| `ttl_not_enabled` | Set a container TTL before using session `ttl_seconds`. |
| `query_incompatible` | Check policy syntax and wait for index transformation completion. |

Compatibility is not the same as optimal cost. Use the package observer or Cosmos diagnostics to
measure request charge and latency. Microsoft supports query index metrics in the Python SDK used
by this package, but recommends enabling them only for performance troubleshooting because they
add overhead and usually do not change while the query and policy remain unchanged.

## Lifecycle and cleanup

- Always close a factory-created `CosmosDBSession`; it owns its Cosmos client.
- Always close a credential supplied to `from_token_credential()`; the caller owns it.
- `clear_session()` fences existing history and begins a new incarnation. It is not a physical
  administrative purge.
- `delete_session()` physically removes a quiescent session partition and should not race with
  application traffic.
- Use unique session IDs and a dedicated container for tests and load runs.

## References

- [OpenAI Agents SDK sessions](https://openai.github.io/openai-agents-python/sessions/)
- [Azure Cosmos DB indexing overview](https://learn.microsoft.com/azure/cosmos-db/index-overview)
- [Azure Cosmos DB indexing policies](https://learn.microsoft.com/azure/cosmos-db/index-policy)
- [Manage indexing policies](https://learn.microsoft.com/azure/cosmos-db/how-to-manage-indexing-policy)
- [Azure Cosmos DB indexing metrics](https://learn.microsoft.com/azure/cosmos-db/index-metrics)
- [Azure Cosmos DB time to live](https://learn.microsoft.com/azure/cosmos-db/time-to-live)
- [Azure Cosmos DB full-text search](https://learn.microsoft.com/azure/cosmos-db/gen-ai/full-text-search)
- [Azure Cosmos DB vector search](https://learn.microsoft.com/azure/cosmos-db/vector-search)