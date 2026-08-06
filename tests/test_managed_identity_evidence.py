from __future__ import annotations

import random
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

from scripts.managed_identity_evidence import (
    ManagedIdentityEvidenceError,
    decode_junit,
    encode_junit,
    validate_junit_provenance,
)
from scripts.run_managed_identity_tests import (
    ManagedIdentityTestConfigurationError,
    annotate_junit,
    validate_environment,
)


class ManagedIdentityEvidenceTests(unittest.TestCase):
    def test_junit_round_trip_is_order_independent_and_ignores_other_logs(self) -> None:
        junit = b'<?xml version="1.0"?><testsuites tests="1"><testsuite /></testsuites>'
        lines = encode_junit(junit, "run-123-attempt-2", chunk_size=11)
        random.Random(42).shuffle(lines)

        self.assertEqual(
            decode_junit(["unrelated output", *lines], "run-123-attempt-2"),
            junit,
        )

    def test_missing_chunk_is_rejected(self) -> None:
        lines = encode_junit(b"long enough to need chunks", "run-123", chunk_size=4)

        with self.assertRaisesRegex(ManagedIdentityEvidenceError, "incomplete"):
            decode_junit(lines[1:], "run-123")

    def test_checksum_mismatch_is_rejected(self) -> None:
        lines = encode_junit(b"junit", "run-123")
        lines[0] = f"{lines[0][:-1]}A"

        with self.assertRaisesRegex(ManagedIdentityEvidenceError, "checksum"):
            decode_junit(lines, "run-123")

    def test_recovery_cli_writes_exact_junit(self) -> None:
        junit = b'<testsuites tests="1"><testsuite /></testsuites>'
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            log_file = root / "logs.txt"
            output = root / "recovered.xml"
            log_file.write_text("\n".join(encode_junit(junit, "run-123")), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/managed_identity_evidence.py",
                    "--log-file",
                    str(log_file),
                    "--nonce",
                    "run-123",
                    "--output",
                    str(output),
                ],
                check=False,
            )

            self.assertEqual(completed.returncode, 0)
            self.assertEqual(output.read_bytes(), junit)

    def test_matching_junit_provenance_is_accepted(self) -> None:
        validate_junit_provenance(
            _provenance_junit(),
            repository="nos-redacted/openai-cosmosdb-session",
            run_attempt="2",
            run_id="123",
            source_commit="a" * 40,
            image_digest=f"sha256:{'b' * 64}",
        )

    def test_mismatched_junit_provenance_is_rejected(self) -> None:
        with self.assertRaisesRegex(ManagedIdentityEvidenceError, "image_digest"):
            validate_junit_provenance(
                _provenance_junit(),
                repository="nos-redacted/openai-cosmosdb-session",
                run_attempt="2",
                run_id="123",
                source_commit="a" * 40,
                image_digest=f"sha256:{'c' * 64}",
            )


class ManagedIdentityTestRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.environment = {
            "AZURE_CLIENT_ID": "client-id",
            "COSMOS_AUTH_MODE": "managed-identity",
            "COSMOS_CONTAINER": "agent_sessions",
            "COSMOS_DATABASE": "agents",
            "COSMOS_ENDPOINT": "https://example.documents.azure.com:443/",
            "COSMOS_LIVE_TESTS": "1",
            "GITHUB_REPOSITORY": "nos-redacted/openai-cosmosdb-session",
            "GITHUB_RUN_ATTEMPT": "2",
            "GITHUB_RUN_ID": "123",
            "GITHUB_SHA": "a" * 40,
            "MANAGED_IDENTITY_EVIDENCE_NONCE": "run-123-attempt-2",
            "OPENAI_COSMOSDB_IMAGE_DIGEST": f"sha256:{'b' * 64}",
            "OPENAI_COSMOSDB_SOURCE_COMMIT": "a" * 40,
            "OPENAI_COSMOSDB_SOURCE_REPOSITORY": (
                "nos-redacted/openai-cosmosdb-session"
            ),
        }

    def test_valid_environment_is_accepted(self) -> None:
        validate_environment(self.environment)

    def test_commit_mismatch_is_rejected(self) -> None:
        self.environment["GITHUB_SHA"] = "c" * 40

        with self.assertRaisesRegex(ManagedIdentityTestConfigurationError, "commit"):
            validate_environment(self.environment)

    def test_connection_string_is_rejected(self) -> None:
        self.environment["COSMOS_CONNECTION_STRING"] = "secret"

        with self.assertRaisesRegex(ManagedIdentityTestConfigurationError, "must not"):
            validate_environment(self.environment)

    def test_junit_is_annotated_with_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "junit.xml"
            path.write_text(
                '<testsuites><testsuite name="live" tests="1" /></testsuites>',
                encoding="utf-8",
            )

            annotate_junit(path, self.environment)

            properties = {
                node.attrib["name"]: node.attrib["value"]
                for node in ElementTree.parse(path).findall("testsuite/properties/property")
            }
            self.assertEqual(properties["source_commit"], "a" * 40)
            self.assertEqual(properties["azure_auth_mode"], "managed-identity")
            self.assertEqual(properties["github_run_id"], "123")

def _provenance_junit() -> bytes:
    properties = {
        "azure_auth_mode": "managed-identity",
        "github_repository": "nos-redacted/openai-cosmosdb-session",
        "github_run_attempt": "2",
        "github_run_id": "123",
        "image_digest": f"sha256:{'b' * 64}",
        "source_commit": "a" * 40,
    }
    property_xml = "".join(
        f'<property name="{name}" value="{value}" />'
        for name, value in properties.items()
    )
    return (
        f'<testsuites><testsuite tests="1"><properties>{property_xml}</properties>'
        "</testsuite></testsuites>"
    ).encode()


if __name__ == "__main__":
    unittest.main()
