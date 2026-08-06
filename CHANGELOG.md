# Changelog

All notable changes to this project are documented in this file.

## 0.2.0 - Unreleased

### Added

- Schema-v2 incarnation fencing for safe overlap between append, clear, and pop.
- Explicit `add_items_once(..., operation_id=...)` caller deduplication.
- Operation-local Session consistency receipts and receipt-aware reads, pops, and clears.
- Strict, non-destructive document corruption handling and sanitized public errors.
- Server-aware bounded-jitter retries that preserve deterministic write replay.
- Read-only Cosmos container readiness validation and explicit token-credential construction.
- Backend-neutral sanitized operation telemetry with request-charge and retry observations.
- A versioned fixed-rate Cosmos load harness with seven scenarios and integrity checks.
- Locked `uv` development commands and least-privilege, commit-pinned CI jobs.
- Bicep for dedicated live/load resources, ACR, a UAMI-enabled Container Apps Job, least-privilege
	RBAC, network controls, and durable Log Analytics evidence, with warning-free compilation in CI.
- Protected Azure-hosted `ManagedIdentityCredential` validation that executes an immutable image
	digest and binds recovered JUnit to the exact repository, commit, workflow run, and image.
- Evidence-gated Trusted Publishing automation with exact cloud-workflow provenance checks.
- An exactly pinned Hatchling build backend in the locked development environment.
- Branch-coverage enforcement, locked dependency auditing, and checksum-verified full-history
	secret scanning in CI and release evidence.

### Changed

- Message IDs now include the session incarnation.
- `clear_session()` is a logical fence followed by best-effort old-incarnation cleanup.
- Version 0.1 test data is not compatible with schema v2 and must be deleted before upgrade.

## 0.1.0 - 2026-01-01

- Initial pre-production implementation of deterministic internal write retries.