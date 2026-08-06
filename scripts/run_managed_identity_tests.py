from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from xml.etree import ElementTree

from .managed_identity_evidence import encode_junit

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_REQUIRED_ENVIRONMENT = (
    "AZURE_CLIENT_ID",
    "COSMOS_ENDPOINT",
    "COSMOS_DATABASE",
    "COSMOS_CONTAINER",
    "GITHUB_REPOSITORY",
    "GITHUB_RUN_ATTEMPT",
    "GITHUB_RUN_ID",
    "GITHUB_SHA",
    "MANAGED_IDENTITY_EVIDENCE_NONCE",
    "OPENAI_COSMOSDB_IMAGE_DIGEST",
    "OPENAI_COSMOSDB_SOURCE_COMMIT",
    "OPENAI_COSMOSDB_SOURCE_REPOSITORY",
)


class ManagedIdentityTestConfigurationError(ValueError):
    pass


def validate_environment(environment: Mapping[str, str]) -> None:
    missing = [name for name in _REQUIRED_ENVIRONMENT if not environment.get(name)]
    if missing:
        raise ManagedIdentityTestConfigurationError(
            f"required environment variables are missing: {', '.join(missing)}"
        )
    if environment.get("COSMOS_LIVE_TESTS") != "1":
        raise ManagedIdentityTestConfigurationError("COSMOS_LIVE_TESTS must be 1")
    if environment.get("COSMOS_AUTH_MODE") != "managed-identity":
        raise ManagedIdentityTestConfigurationError(
            "COSMOS_AUTH_MODE must be managed-identity"
        )
    if environment.get("COSMOS_CONNECTION_STRING"):
        raise ManagedIdentityTestConfigurationError(
            "COSMOS_CONNECTION_STRING must not be configured"
        )

    source_commit = environment["OPENAI_COSMOSDB_SOURCE_COMMIT"]
    if not _COMMIT_PATTERN.fullmatch(source_commit):
        raise ManagedIdentityTestConfigurationError("image source commit has an invalid format")
    if environment["GITHUB_SHA"] != source_commit:
        raise ManagedIdentityTestConfigurationError(
            "workflow commit does not match the image source commit"
        )
    if environment["GITHUB_REPOSITORY"] != environment["OPENAI_COSMOSDB_SOURCE_REPOSITORY"]:
        raise ManagedIdentityTestConfigurationError(
            "workflow repository does not match the image source repository"
        )
    if not _DIGEST_PATTERN.fullmatch(environment["OPENAI_COSMOSDB_IMAGE_DIGEST"]):
        raise ManagedIdentityTestConfigurationError("container image digest has an invalid format")


def annotate_junit(path: Path, environment: Mapping[str, str]) -> None:
    tree = ElementTree.parse(path)
    root = tree.getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites:
        raise ManagedIdentityTestConfigurationError("pytest produced no JUnit test suites")

    provenance = {
        "azure_auth_mode": environment["COSMOS_AUTH_MODE"],
        "github_repository": environment["GITHUB_REPOSITORY"],
        "github_run_attempt": environment["GITHUB_RUN_ATTEMPT"],
        "github_run_id": environment["GITHUB_RUN_ID"],
        "image_digest": environment["OPENAI_COSMOSDB_IMAGE_DIGEST"],
        "source_commit": environment["GITHUB_SHA"],
    }
    for suite in suites:
        properties = suite.find("properties")
        if properties is None:
            properties = ElementTree.Element("properties")
            suite.insert(0, properties)
        for name, value in provenance.items():
            ElementTree.SubElement(properties, "property", name=name, value=value)
    tree.write(path, encoding="utf-8", xml_declaration=True)


def main() -> int:
    try:
        validate_environment(os.environ)
    except ManagedIdentityTestConfigurationError as error:
        print(f"managed-identity preflight failed: {error}", file=sys.stderr, flush=True)
        return 2

    with TemporaryDirectory() as temporary_directory:
        junit_path = Path(temporary_directory) / "managed-identity-test-results.xml"
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-m",
                "live_cosmos",
                "tests/integration",
                "-v",
                f"--junit-xml={junit_path}",
            ],
            check=False,
        )
        if not junit_path.is_file():
            print("pytest did not produce managed-identity JUnit", file=sys.stderr, flush=True)
            return completed.returncode or 2

        try:
            annotate_junit(junit_path, os.environ)
        except (ElementTree.ParseError, ManagedIdentityTestConfigurationError) as error:
            print(f"managed-identity JUnit is invalid: {error}", file=sys.stderr, flush=True)
            return completed.returncode or 2

        for line in encode_junit(
            junit_path.read_bytes(), os.environ["MANAGED_IDENTITY_EVIDENCE_NONCE"]
        ):
            print(line, flush=True)
        return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
