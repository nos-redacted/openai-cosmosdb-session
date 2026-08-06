# Contributing

## Development Setup

Use Python 3.13 and `uv` for the locked development environment:

```powershell
uv sync --locked --all-extras --group dev
```

Run the local gates before proposing a change:

```powershell
uv lock --check
uv run ruff check .
uv run mypy src benchmarks/load_cosmos.py scripts/verify_release.py scripts/verify_workflow_run.py
uv run pytest -m "not live_cosmos" --cov=openai_cosmosdb_session --cov-branch --cov-fail-under=84
uv build
bicep build infra/live-test/main.bicep
```

Audit the locked runtime and identity dependencies, and run the source-tree secret scan when
Gitleaks 8.30.1 is installed locally:

```powershell
$runtimeRequirements = Join-Path $env:TEMP "openai-cosmosdb-runtime-requirements.txt"
uv export --locked --no-dev --all-extras --no-emit-project --output-file $runtimeRequirements
uv run pip-audit --requirement $runtimeRequirements --disable-pip --strict
gitleaks dir --no-banner --redact .
```

CI downloads Gitleaks over TLS, verifies the published archive SHA-256, and scans full Git history.

Before a public release, regenerate the lock from canonical PyPI on a host with a working TLS path:

```powershell
uv --system-certs lock --refresh --default-index https://pypi.org/simple
uv lock --check
```

The current development host cannot complete TLS negotiation with `files.pythonhosted.org`, so the
checked-in lock still records Microsoft package-feed proxy URLs. Never disable TLS verification or
rewrite lock URLs manually to conceal that limitation.

## Persistence Changes

Changes to sequence reservation, deterministic IDs, operation ledgers, incarnation fencing, or
conflict validation require focused fault-injection tests. Preserve the invariant that ordinary
`add_items()` serializes once, reserves once, builds once, and retries only deterministic document
creates. Never enable native SDK write replay for ambiguous mutations.

## Security and Tests

Never commit or paste Cosmos credentials. Live tests must target a dedicated account/container,
use explicit opt-in configuration, and clean their unique logical partitions. Keep exceptions,
logs, and test output free of message bodies, operation IDs, session IDs, endpoints, and tokens.

## Release Administration

Repository administrators must configure required checks, branch and tag protection, CODEOWNERS,
the protected `cosmos-integration`, `cosmos-load`, `managed-identity-live`, and `pypi`
environments, Azure workload federation, and the PyPI Trusted Publisher. OIDC live/load evidence
must come from the exact release commit. It does not replace the Azure-hosted
`ManagedIdentityCredential` proof.

Keep `MANAGED_IDENTITY_RELEASE_ENABLED` false until a selected Azure runtime has passed that proof.
The release workflow fails closed while the variable is absent or false. It also regenerates and
validates empty dependency-audit and secret-scan JSON reports from the tagged commit.