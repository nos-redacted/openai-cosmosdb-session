# OpenAI Cosmos DB Session

An async Azure Cosmos DB session backend for the OpenAI Agents SDK.

The package implements the structural `Session` protocol from `openai-agents` and stores
each conversation item as a separate document in one logical partition. It targets Python
3.10+, `openai-agents` 0.19.1+, and `azure-cosmos` 4.16.3+.

For production validation, use Python 3.13 until the installed `azure-cosmos` release declares
support for newer Python versions. Development on newer Python versions may work but is not a
substitute for testing on a dependency-supported runtime.

Version 0.2 uses persistence schema v2. Delete pre-production 0.1 session data before upgrading;
there is no legacy migration path.

## Cosmos DB setup

Create a container with partition key `/sessionId`. Enable TTL with `default_ttl=-1` if
sessions will use `ttl_seconds`. The recommended indexing policy indexes only the fields used
by session queries:

```python
from azure.cosmos import PartitionKey

await database.create_container_if_not_exists(
    id="agent_sessions",
    partition_key=PartitionKey(path="/sessionId"),
    default_ttl=-1,
    indexing_policy={
        "indexingMode": "consistent",
        "includedPaths": [
            {"path": "/seq/?"},
            {"path": "/type/?"},
            {"path": "/incarnation/?"},
        ],
        "excludedPaths": [{"path": "/*"}],
    },
)
```

Container provisioning is intentionally separate from session construction.

The partition key is an immutable container property. If an existing container does not use
exactly `/sessionId`, create a new container rather than reusing it. The indexing policy above is
recommended for cost and query efficiency; the live test suite exercises the required ordered
queries against the deployed policy.

For a one-time private test, the same resources can be provisioned with the async SDK and an
account connection string:

```python
from azure.cosmos import PartitionKey
from azure.cosmos.aio import CosmosClient

async with CosmosClient.from_connection_string(connection_string) as client:
    database = await client.create_database_if_not_exists(id="agents")
    await database.create_container_if_not_exists(
        id="agent_sessions",
        partition_key=PartitionKey(path="/sessionId"),
        default_ttl=-1,
        indexing_policy={
            "indexingMode": "consistent",
            "includedPaths": [
                {"path": "/seq/?"},
                {"path": "/type/?"},
                {"path": "/incarnation/?"},
            ],
            "excludedPaths": [{"path": "/*"}],
        },
    )
```

Provision production resources through deployment infrastructure rather than at application
startup.

### Deployment template

[`infra/live-test/main.bicep`](infra/live-test/main.bicep) creates the release-validation
environment: a Session-consistent Cosmos DB account, separate live and load containers with the
exact `/sessionId` and indexing contract, continuous backup, a user-assigned identity, an Azure
Container Registry, a Log Analytics workspace, and a manual Azure Container Apps Job. Local key
authentication is disabled by default.

The user-assigned identity is attached to the job and has only the ACR pull and Cosmos data-plane
roles needed by the proof. The protected workflow uses GitHub OIDC only to orchestrate Azure; the
job itself obtains a Cosmos token through `ManagedIdentityCredential`. See
[`infra/live-test/README.md`](infra/live-test/README.md) for deployment parameters, identity
boundaries, protected variables, and validation commands.

## Authentication

Connection strings are supported for local smoke tests. Keep them in environment or secret
storage and never commit them:

```python
session = await CosmosDBSession.from_connection_string(
    "conversation-123",
    connection_string,
)
```

For Azure-hosted applications, use `ManagedIdentityCredential`. Use `DefaultAzureCredential`
for local developer flows. Install the optional identity dependency and grant the identity the
Cosmos DB Built-in Data Contributor data-plane role on the account:

```powershell
python -m pip install ".[identity]"
```

```python
from azure.identity.aio import ManagedIdentityCredential
from openai_cosmosdb_session import CosmosDBSession

credential = ManagedIdentityCredential()
session = await CosmosDBSession.from_token_credential(
    "conversation-123",
    "https://<account>.documents.azure.com:443/",
    credential,
)

try:
    validation = await session.validate_container()
    validation.raise_for_invalid()
finally:
    await session.close()
    await credential.close()
```

`CosmosDBSession` closes a Cosmos client created by its factories. The caller still owns and must
close an injected Azure credential. Owned-client factories validate the container by default;
pass `validate_on_create=False` only for controlled provisioning or diagnostics.

## Usage

Create a session from a connection string:

```python
from openai_cosmosdb_session import CosmosDBSession

session = await CosmosDBSession.from_connection_string(
    "conversation-123",
    connection_string,
    database="agents",
    container="agent_sessions",
    ttl_seconds=30 * 24 * 60 * 60,
)

try:
    # Pass session to Runner.run(..., session=session).
    ...
finally:
    await session.close()
```

Or inject an existing async client. Injected clients remain owned by the caller and are not
closed by `session.close()`:

```python
from azure.cosmos.aio import CosmosClient
from openai_cosmosdb_session import CosmosDBSession

async with CosmosClient(endpoint, credential) as client:
    session = CosmosDBSession("conversation-123", client=client)
    ...
```

Create one long-lived async `CosmosClient` per process and inject it into sessions. Do not create a
new client per request. Factory-created clients are convenient for bounded lifetimes and are closed
by `session.close()`.

## Ordering and retries

Each non-empty protocol `add_items()` call performs this sequence exactly once:

1. Serialize the supplied items.
2. Atomically reserve one contiguous sequence-number block.
3. Build documents with deterministic IDs from the current incarnation and reserved block.
4. Retry only creation of those already-built documents.

Native Cosmos write replay is disabled for these operations. If a retried create receives
`409 Conflict`, the existing item is point-read and accepted only when `sessionId`, `incarnation`,
`seq`, and `message_data` exactly match the reserved document. Any mismatch raises
`CosmosSessionConflictError`.

This is internal retry safety, not caller-level idempotency. Calling `add_items()` again after
an uncertain result is a new append and reserves a new sequence block. A failed write after a
successful reservation may leave a sequence gap; gaps are valid and ordering remains monotonic.
Do not automatically retry the entire `add_items()` call unless duplicate logical content is
acceptable.

For explicit caller deduplication, supply an operation ID to the Cosmos-specific extension:

```python
receipt = await session.add_items_once(
    [{"role": "user", "content": "hello"}],
    operation_id=request_id,
)
```

The operation ID is hashed before persistence. Reusing it with the same serialized payload resumes
one atomic reservation and deterministic document set; reusing it with different content raises a
sanitized `CosmosSessionOperationError`. Operation ledgers have the same TTL as the session. This
does not change protocol `add_items()` behavior.

## Concurrency and clear semantics

Metadata stores a random incarnation and the next sequence position. Message IDs have the form
`<incarnation>:<12-digit-sequence>`. `clear_session()` atomically rotates the incarnation and resets
the sequence before cleaning old documents, so writes reserved before the clear remain invisible
even if they land afterward. Cleanup is not the correctness boundary.

Reads show only the current incarnation. `pop_item()` deletes the current newest item with its ETag
and retries on concurrent change. These operations provide fenced safe overlap, not strict global
linearizability. `delete_session()` physically purges a quiescent logical partition and is intended
for administrative cleanup, not concurrent application traffic.

## Cross-client read-your-writes

Factory-created clients default to Session consistency; another supported Cosmos consistency level
can be supplied with `consistency_level`. Under Session consistency, use an operation-local receipt
when a different `CosmosClient` must immediately observe a write:

```python
receipt = await writer.add_items_with_receipt(items)
history = await reader.get_items_with_receipt(receipt)
```

`add_items_once()` also returns a receipt. Receipt-aware `pop_item_with_receipt()` and
`clear_session_with_receipt()` are available for destructive workflows. Session tokens are opaque,
excluded from receipt `repr`, and captured from each operation's response hook rather than shared
client-global headers. Without a receipt, independent clients receive the account's normal
consistency guarantee.

## Corruption and errors

Reads validate message type, schema version, ID/incarnation/sequence consistency, payload type, JSON,
and decoded object shape. The default is strict and non-destructive: malformed data raises
`CosmosSessionCorruptDocumentError` and is not deleted. Public package errors retain stable codes and
field names only; they do not retain expected/existing documents, message data, operation IDs, or
credentials.

## Readiness and liveness

`ping()` is a liveness check only. `validate_container()` performs read-only readiness checks for
database/container access, exact `/sessionId`, required index paths, TTL compatibility, and both
ordered query shapes against an unused logical partition:

```python
result = await session.validate_container()
result.raise_for_invalid()
```

Validation results and configuration exceptions expose stable issue codes without Cosmos response
details.

## Retry timing

The SDK's native write replay remains disabled with `retry_write=0`. Safe deterministic document
replays honor `x-ms-retry-after-ms`, then standard `Retry-After`, and add bounded full jitter to the
client exponential component. A server hint is never shortened. Configure the base and client cap
with `retry_backoff_seconds` and `max_retry_delay_seconds`.

## Observability

Pass a `CosmosSessionObserver` to receive immutable `CosmosOperationEvent` values. Events contain
bounded operation/outcome facts, duration, aggregate request charge, status class, retry reason and
delay, throttling, and stable error codes. They never contain raw session IDs, message data,
operation IDs, credentials, endpoint strings, activity IDs, session tokens, or exceptions. Observer
failures are swallowed and cannot fail a storage operation. The core package ships no telemetry
exporter; adapt events to the logging or OpenTelemetry backend selected by the consuming service.

For example, an adapter can project only the documented bounded fields into an application logger:

```python
class LoggingObserver:
    def on_event(self, event):
        logger.info(
            "cosmos_session",
            extra={
                "operation": event.operation,
                "outcome": event.outcome,
                "duration_seconds": event.duration_seconds,
                "request_charge": event.request_charge,
                "status_class": event.status_class,
                "code": event.code,
                "throttled": event.throttled,
            },
        )
```

Use the same projection for OpenTelemetry counters and histograms. Keep trace IDs and sampling in
the consuming application; do not add session or operation IDs as metric attributes.

Production monitoring should cover 429 rate, retry exhaustion, 5xx and transport failures, p95/p99
latency, RU per operation, conflict/corruption/configuration counts, cleanup failure, logical
partition size, and liveness/readiness failure. Thresholds are deployment-specific and should be
set from a measured baseline rather than package defaults.

## TTL semantics

When `ttl_seconds` is set, each message expires that many seconds after its own last write, so
older messages expire first. The metadata document is touched by every append and expires after
the session has been idle for the same interval. Operation ledgers use the same TTL. TTL does not
atomically expire an entire session.

## Code organization

- `session.py` contains the public `CosmosDBSession` facade, factories, lifecycle, and the
    visible append orchestration.
- `_documents.py` owns deterministic serialization, timestamps, IDs, TTL fields, and document
    equality rules.
- `_history.py` owns chronological reads, latest-item removal, corrupt-item handling, and clear.
- `_write_pipeline.py` owns sequence reservation, metadata bootstrap, bounded write retries, and
    validated conflict recovery.
- `_validation.py`, `_retry.py`, `_tokens.py`, and `_telemetry.py` own readiness, retry timing,
    operation-local response capture, and safe event emission.
- `exceptions.py` contains the public package error hierarchy.

Modules prefixed with `_` are implementation details. Applications should import the public API
from `openai_cosmosdb_session`.

## Development

```powershell
uv sync --locked --all-extras --group dev
uv run pytest
uv run ruff check .
uv run mypy src
```

The checked-in lock currently records Microsoft package-feed proxy URLs because this development
host receives a TLS handshake failure from `files.pythonhosted.org`. Public release is blocked
until `uv.lock` is regenerated from `https://pypi.org/simple` on a host with working canonical
PyPI TLS. Do not disable certificate verification or replace registry URLs by hand.

## Live Cosmos validation

Live tests are disabled unless `COSMOS_LIVE_TESTS=1` is explicitly set. They use unique session
IDs and attempt cleanup, but should still target a dedicated test container rather than production
data. The suite verifies:

- connectivity, add/get/latest-N/pop/clear, and cross-session isolation;
- concurrent sequence allocation from multiple session objects;
- a real `409 Conflict` replay after a synthetic post-create transport failure;
- per-item TTL fields when TTL is enabled on the container;
- read-only container validation;
- cross-client receipt handoff between independent clients;
- concurrent duplicate operation IDs producing one message.

Connection-string authentication:

```powershell
$env:COSMOS_LIVE_TESTS = "1"
$env:COSMOS_AUTH_MODE = "connection-string"
$env:COSMOS_CONNECTION_STRING = "<secret>"
$env:COSMOS_DATABASE = "agents"
$env:COSMOS_CONTAINER = "agent_sessions"
python -m pytest -m live_cosmos tests/integration -v
```

Explicit developer authentication:

```powershell
python -m pip install -e ".[identity]"
$env:COSMOS_LIVE_TESTS = "1"
$env:COSMOS_AUTH_MODE = "developer"
$env:COSMOS_ENDPOINT = "https://<account>.documents.azure.com:443/"
$env:COSMOS_DATABASE = "agents"
$env:COSMOS_CONTAINER = "agent_sessions"
python -m pytest -m live_cosmos tests/integration -v
```

Use `COSMOS_AUTH_MODE=azure-cli` to require `AzureCliCredential`, or
`COSMOS_AUTH_MODE=managed-identity` on an Azure host to require `ManagedIdentityCredential`.
Conflicting or missing authentication variables fail instead of silently falling back. The fixture
fails before writing if the target container's partition key is not exactly `/sessionId`. The TTL
test skips when container TTL is disabled.

## Load validation

`benchmarks/load_cosmos.py` is a fixed-rate cloud harness with spread-session append, hot-partition
append, multi-item append, latest-N read, clear/pop overlap, concurrent operation-ID deduplication,
and mixed scenarios. Always target a dedicated TTL-enabled load container. The harness creates
unique logical partitions and attempts physical cleanup, but a process or host failure can leave
data until TTL expiry.

The initial non-blocking baseline is 32 workers at 100 offered operations per second for five
minutes, after a 30-second warmup:

```powershell
$env:COSMOS_AUTH_MODE = "connection-string"
$env:COSMOS_CONNECTION_STRING = "<secret>"
$env:COSMOS_DATABASE = "agents"
$env:COSMOS_CONTAINER = "agent_sessions_load"

uv run python benchmarks/load_cosmos.py `
    --scenario mixed `
    --concurrency 32 `
    --rate 100 `
    --duration 300 `
    --warmup 30 `
    --output artifacts/load-mixed.json
```

Use `--scenario all` to run every scenario sequentially. Output schema v1 contains requested and
achieved rate, p50/p95/p99 latency, aggregate and per-success RU, retries, throttling, bounded error
codes, integrity checks, and cleanup status. It contains no endpoint, credential, session ID,
operation ID, message body, token, activity ID, or raw exception. Exit code 1 means a measured
operation, integrity check, or cleanup failed; exit code 2 means setup or execution failed. A release
baseline requires zero operation and integrity errors and successful cleanup. Latency and RU
regression thresholds remain unset until a stable dedicated environment has produced a baseline.

For the final installation proof, build and install the wheel into a fresh Python 3.13 environment
before running the same live suite:

```powershell
py -3.13 -m venv .venv-live
.\.venv-live\Scripts\python -m pip install --upgrade pip
.\.venv-live\Scripts\python -m pip wheel --no-deps --wheel-dir dist .
.\.venv-live\Scripts\python -m pip install (Get-ChildItem dist\*.whl | Select-Object -First 1).FullName
.\.venv-live\Scripts\python -m pip install pytest pytest-asyncio
.\.venv-live\Scripts\python -m pytest -m live_cosmos tests/integration -v
```

## Protected workflows and release

The OIDC workflow uses `AzureCliCredential` after `azure/login`; this proves GitHub workload
federation, not Azure managed identity. Configure these protected GitHub environments:

| Environment | Required variables | Purpose |
| --- | --- | --- |
| `cosmos-integration` | `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_CONTAINER` | OIDC live suite |
| `cosmos-load` | `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_LOAD_CONTAINER` | Dedicated load baseline |
| `managed-identity-live` | `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_CONTAINER_REGISTRY`, `AZURE_CONTAINER_APP_JOB`, `AZURE_LOG_ANALYTICS_WORKSPACE_ID`, `AZURE_MANAGED_IDENTITY_CLIENT_ID`, `COSMOS_ENDPOINT`, `COSMOS_DATABASE`, `COSMOS_CONTAINER` | GitHub OIDC orchestration of the Container Apps UAMI proof |
| `pypi` | `MANAGED_IDENTITY_RELEASE_ENABLED` | Trusted Publishing approval gate |

Set repository variable `COSMOS_OIDC_ENABLED=true` only after scheduled OIDC validation is ready.
Do not configure `COSMOS_CONNECTION_STRING` as either a variable or secret in
`managed-identity-live`; the workflow rejects it. Set `MANAGED_IDENTITY_RELEASE_ENABLED=true` only
after the protected Container Apps proof is operational.

Protect each environment with required reviewers, prevent self-review and administrator bypass,
and restrict deployment branches or tags. Configure the `pypi` environment as the PyPI Trusted
Publisher for this repository and workflow.

For a release candidate, dispatch `Live Cosmos OIDC` from the exact commit that will be tagged,
with `run_load=true`, `load_scenario=mixed`, concurrency 32, rate 100, and duration 300. Both cloud
artifacts can come from the same successful workflow run ID. Complete a separate Azure-hosted
`Managed Identity Cosmos` run for that commit. It builds the commit in ACR, resolves its immutable
digest, starts the preprovisioned Container Apps Job with that digest, and recovers nonce-bound,
checksum-protected JUnit from Log Analytics. The recovered JUnit must bind the managed-identity auth
mode, repository, run ID and attempt, source commit, and image digest.

Then date the matching changelog section, create the `vX.Y.Z` tag, and dispatch `Release` with the
live, load, and managed-identity run IDs. The release workflow independently verifies repository,
workflow, commit, JUnit provenance, load integrity, branch coverage, locked dependency audit,
full-history secret scan, package metadata, checksums, and the exact wheel before publishing. The
successful dependency-audit and secret-scan JSON reports are attached to the GitHub release with
the package and cloud evidence.
