from __future__ import annotations

import argparse
import base64
import hashlib
import re
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from xml.etree import ElementTree

EVIDENCE_PREFIX = "COSMOS_MI_JUNIT"
_NONCE_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_PROVENANCE_PROPERTIES = (
    "azure_auth_mode",
    "github_repository",
    "github_run_attempt",
    "github_run_id",
    "image_digest",
    "source_commit",
)


class ManagedIdentityEvidenceError(ValueError):
    pass


def encode_junit(junit: bytes, nonce: str, *, chunk_size: int = 2048) -> list[str]:
    if not _NONCE_PATTERN.fullmatch(nonce):
        raise ManagedIdentityEvidenceError("evidence nonce has an invalid format")
    if chunk_size < 1:
        raise ManagedIdentityEvidenceError("chunk size must be positive")

    encoded = base64.b64encode(junit).decode("ascii")
    chunks = [encoded[index : index + chunk_size] for index in range(0, len(encoded), chunk_size)]
    if not chunks:
        chunks = [""]

    digest = hashlib.sha256(junit).hexdigest()
    total = len(chunks)
    return [
        f"{EVIDENCE_PREFIX}|{nonce}|{index}|{total}|{digest}|{chunk}"
        for index, chunk in enumerate(chunks)
    ]


def decode_junit(log_lines: Iterable[str], nonce: str) -> bytes:
    if not _NONCE_PATTERN.fullmatch(nonce):
        raise ManagedIdentityEvidenceError("evidence nonce has an invalid format")

    prefix = f"{EVIDENCE_PREFIX}|{nonce}|"
    chunks: dict[int, str] = {}
    expected_total: int | None = None
    expected_digest: str | None = None

    for line in log_lines:
        if not line.startswith(prefix):
            continue
        parts = line.split("|", 5)
        if len(parts) != 6:
            raise ManagedIdentityEvidenceError("evidence chunk has an invalid format")
        try:
            index = int(parts[2])
            total = int(parts[3])
        except ValueError as error:
            raise ManagedIdentityEvidenceError("evidence chunk index is invalid") from error
        digest = parts[4]
        if total < 1 or index < 0 or index >= total:
            raise ManagedIdentityEvidenceError("evidence chunk bounds are invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ManagedIdentityEvidenceError("evidence digest is invalid")
        if expected_total is not None and total != expected_total:
            raise ManagedIdentityEvidenceError("evidence chunks disagree on total count")
        if expected_digest is not None and digest != expected_digest:
            raise ManagedIdentityEvidenceError("evidence chunks disagree on digest")
        if index in chunks and chunks[index] != parts[5]:
            raise ManagedIdentityEvidenceError("conflicting duplicate evidence chunk")
        expected_total = total
        expected_digest = digest
        chunks[index] = parts[5]

    if expected_total is None or expected_digest is None:
        raise ManagedIdentityEvidenceError("managed-identity JUnit evidence was not found")
    if set(chunks) != set(range(expected_total)):
        raise ManagedIdentityEvidenceError("managed-identity JUnit evidence is incomplete")

    try:
        encoded = "".join(chunks[index] for index in range(expected_total))
        junit = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise ManagedIdentityEvidenceError(
            "managed-identity JUnit evidence is not valid base64"
        ) from error
    if hashlib.sha256(junit).hexdigest() != expected_digest:
        raise ManagedIdentityEvidenceError(
            "managed-identity JUnit evidence failed checksum validation"
        )
    return junit


def validate_junit_provenance(
    junit: bytes,
    *,
    repository: str,
    run_attempt: str,
    run_id: str,
    source_commit: str,
    image_digest: str,
) -> None:
    if not repository or "/" not in repository:
        raise ManagedIdentityEvidenceError("expected repository has an invalid format")
    if not run_id.isdigit() or int(run_id) < 1:
        raise ManagedIdentityEvidenceError("expected workflow run ID has an invalid format")
    if not run_attempt.isdigit() or int(run_attempt) < 1:
        raise ManagedIdentityEvidenceError("expected workflow run attempt has an invalid format")
    if not _COMMIT_PATTERN.fullmatch(source_commit):
        raise ManagedIdentityEvidenceError("expected source commit has an invalid format")
    if not _DIGEST_PATTERN.fullmatch(image_digest):
        raise ManagedIdentityEvidenceError("expected image digest has an invalid format")

    expected = {
        "azure_auth_mode": "managed-identity",
        "github_repository": repository,
        "github_run_attempt": run_attempt,
        "github_run_id": run_id,
        "image_digest": image_digest,
        "source_commit": source_commit,
    }
    try:
        root = ElementTree.fromstring(junit)
    except ElementTree.ParseError as error:
        raise ManagedIdentityEvidenceError("managed-identity JUnit is not valid XML") from error

    suites = [element for element in root.iter() if _local_name(element.tag) == "testsuite"]
    if not suites:
        raise ManagedIdentityEvidenceError("managed-identity JUnit contains no test suites")
    for suite in suites:
        properties_nodes = [
            child for child in suite if _local_name(child.tag) == "properties"
        ]
        if len(properties_nodes) != 1:
            raise ManagedIdentityEvidenceError(
                "managed-identity JUnit suite must contain exactly one properties element"
            )

        actual: dict[str, str] = {}
        for property_node in properties_nodes[0]:
            if _local_name(property_node.tag) != "property":
                continue
            name = property_node.get("name")
            if name not in _PROVENANCE_PROPERTIES:
                continue
            if name in actual:
                raise ManagedIdentityEvidenceError(
                    "managed-identity JUnit contains duplicate provenance properties"
                )
            value = property_node.get("value")
            if value is None:
                raise ManagedIdentityEvidenceError(
                    "managed-identity JUnit provenance property has no value"
                )
            actual[name] = value

        if set(actual) != set(_PROVENANCE_PROPERTIES):
            raise ManagedIdentityEvidenceError(
                "managed-identity JUnit provenance properties are incomplete"
            )
        for name, value in expected.items():
            if actual[name] != value:
                raise ManagedIdentityEvidenceError(
                    f"managed-identity JUnit {name} does not match expected provenance"
                )


def _local_name(tag: object) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _expected_provenance(arguments: argparse.Namespace) -> Mapping[str, str] | None:
    names = (
        "expected_repository",
        "expected_run_attempt",
        "expected_run_id",
        "expected_source_commit",
        "expected_image_digest",
    )
    values = {name: getattr(arguments, name) for name in names}
    if not any(values.values()):
        return None
    if not all(values.values()):
        raise ManagedIdentityEvidenceError(
            "all expected provenance arguments must be provided together"
        )
    return values


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recover checksum-verified managed-identity JUnit from Container Apps logs."
    )
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-repository")
    parser.add_argument("--expected-run-attempt")
    parser.add_argument("--expected-run-id")
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--expected-image-digest")
    arguments = parser.parse_args()

    try:
        log_lines = arguments.log_file.read_text(encoding="utf-8").splitlines()
        junit = decode_junit(log_lines, arguments.nonce)
        expected = _expected_provenance(arguments)
        if expected is not None:
            validate_junit_provenance(
                junit,
                repository=expected["expected_repository"],
                run_attempt=expected["expected_run_attempt"],
                run_id=expected["expected_run_id"],
                source_commit=expected["expected_source_commit"],
                image_digest=expected["expected_image_digest"],
            )
        arguments.output.write_bytes(junit)
    except (ManagedIdentityEvidenceError, OSError) as error:
        print(f"managed-identity evidence recovery failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
