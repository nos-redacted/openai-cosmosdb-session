"""Verify local distributions and protected cloud evidence before publication."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
import xml.etree.ElementTree as ElementTree
import zipfile
from collections.abc import Mapping, Sequence
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any

from .managed_identity_evidence import (
    ManagedIdentityEvidenceError,
    validate_junit_provenance,
)

PROJECT_NAME = "openai-cosmosdb-session"
MINIMUM_LOAD_CONCURRENCY = 32
MINIMUM_LOAD_RATE = 100.0
MINIMUM_LOAD_DURATION_SECONDS = 300.0


class ReleaseVerificationError(ValueError):
    """A sanitized release-evidence validation failure."""


def read_project_version(project_file: Path) -> str:
    """Read the static version from the project table on Python 3.10+."""
    current_table = ""
    for raw_line in project_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("[") and line.endswith("]"):
            current_table = line[1:-1].strip()
            continue
        if current_table != "project":
            continue
        match = re.fullmatch(r'version\s*=\s*"([^"]+)"\s*(?:#.*)?', line)
        if match is not None:
            return match.group(1)
    raise ReleaseVerificationError("project version is missing or is not a static string")


def verify_tag(tag: str, version: str) -> None:
    expected = f"v{version}"
    if tag != expected:
        raise ReleaseVerificationError("release tag does not match the project version")


def extract_release_notes(
    changelog_file: Path,
    version: str,
    *,
    allow_unreleased: bool = False,
) -> str:
    text = changelog_file.read_text(encoding="utf-8")
    heading = re.search(
        rf"^## {re.escape(version)} - (?P<release_date>[^\r\n]+)$",
        text,
        flags=re.MULTILINE,
    )
    if heading is None:
        raise ReleaseVerificationError("the changelog has no section for the project version")

    release_date = heading.group("release_date").strip()
    if release_date == "Unreleased":
        if not allow_unreleased:
            raise ReleaseVerificationError("the changelog release is still marked Unreleased")
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", release_date) is None:
        raise ReleaseVerificationError("the changelog release date is not ISO formatted")

    body_start = heading.end()
    next_heading = re.search(r"^## ", text[body_start:], flags=re.MULTILINE)
    body_end = body_start + next_heading.start() if next_heading is not None else len(text)
    notes = text[body_start:body_end].strip()
    if not notes:
        raise ReleaseVerificationError("the changelog release section is empty")
    return notes + "\n"


def verify_distributions(dist_dir: Path, *, version: str) -> None:
    wheels = sorted(dist_dir.glob("*.whl"))
    source_archives = sorted(dist_dir.glob("*.tar.gz"))
    unexpected = sorted(
        path.name
        for path in dist_dir.iterdir()
        if path.is_file() and path not in {*wheels, *source_archives}
    )
    if len(wheels) != 1 or len(source_archives) != 1 or unexpected:
        raise ReleaseVerificationError(
            "the distribution directory must contain exactly one wheel and one source archive"
        )

    _verify_wheel(wheels[0], version=version)
    _verify_source_archive(source_archives[0], version=version)


def verify_live_junit(junit_file: Path) -> None:
    try:
        root = ElementTree.parse(junit_file).getroot()
    except (ElementTree.ParseError, OSError) as error:
        raise ReleaseVerificationError("live-test JUnit is unreadable") from error

    suites = [
        element
        for element in root.iter()
        if _local_name(element.tag) == "testsuite"
        and not any(_local_name(child.tag) == "testsuite" for child in element)
    ]
    if not suites:
        raise ReleaseVerificationError("live-test JUnit contains no test suites")

    tests = sum(_integer_attribute(suite, "tests") for suite in suites)
    failures = sum(_integer_attribute(suite, "failures") for suite in suites)
    errors = sum(_integer_attribute(suite, "errors") for suite in suites)
    skipped = sum(_integer_attribute(suite, "skipped") for suite in suites)
    if tests < 1 or failures or errors or skipped:
        raise ReleaseVerificationError("live tests did not complete with zero failures and skips")


def verify_managed_identity_junit(
    junit_file: Path,
    *,
    repository: str,
    commit: str,
    run_id: int,
    run_attempt: int,
    image_digest: str,
) -> None:
    verify_live_junit(junit_file)
    try:
        validate_junit_provenance(
            junit_file.read_bytes(),
            repository=repository,
            run_attempt=str(run_attempt),
            run_id=str(run_id),
            source_commit=commit,
            image_digest=image_digest,
        )
    except (ManagedIdentityEvidenceError, OSError) as error:
        raise ReleaseVerificationError(
            f"managed-identity JUnit provenance is invalid: {error}"
        ) from error


def verify_load_report(report_file: Path) -> None:
    try:
        document: Any = json.loads(report_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise ReleaseVerificationError("the load report is unreadable") from error
    report = _mapping(document, "load report")
    if report.get("schema_version") != 1:
        raise ReleaseVerificationError("the load report schema version is unsupported")

    scenarios_value = report.get("scenarios")
    if not isinstance(scenarios_value, list) or not scenarios_value:
        raise ReleaseVerificationError("the load report contains no scenarios")

    scenario_names: set[str] = set()
    for value in scenarios_value:
        scenario = _mapping(value, "load scenario")
        name = scenario.get("scenario")
        if not isinstance(name, str) or not name:
            raise ReleaseVerificationError("a load scenario has no bounded name")
        scenario_names.add(name)
        if _integer(scenario, "failed_operations") != 0:
            raise ReleaseVerificationError("a load scenario contains failed operations")
        integrity_errors = scenario.get("integrity_errors")
        if not isinstance(integrity_errors, list) or integrity_errors:
            raise ReleaseVerificationError("a load scenario contains integrity errors")
        if scenario.get("cleanup_succeeded") is not True:
            raise ReleaseVerificationError("a load scenario did not clean up successfully")
        if _integer(scenario, "concurrency") < MINIMUM_LOAD_CONCURRENCY:
            raise ReleaseVerificationError("load concurrency is below the release baseline")
        if _number(scenario, "requested_rate_per_second") < MINIMUM_LOAD_RATE:
            raise ReleaseVerificationError("load rate is below the release baseline")
        if _number(scenario, "requested_duration_seconds") < MINIMUM_LOAD_DURATION_SECONDS:
            raise ReleaseVerificationError("load duration is below the release baseline")

    if "mixed" not in scenario_names:
        raise ReleaseVerificationError("the load report does not contain the mixed baseline")


def verify_dependency_audit(report_file: Path) -> None:
    try:
        document: Any = json.loads(report_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise ReleaseVerificationError("the dependency audit is unreadable") from error
    report = _mapping(document, "dependency audit")
    dependencies = report.get("dependencies")
    if not isinstance(dependencies, list) or not dependencies:
        raise ReleaseVerificationError("the dependency audit contains no dependencies")
    for value in dependencies:
        dependency = _mapping(value, "dependency audit entry")
        if not isinstance(dependency.get("name"), str) or not dependency.get("name"):
            raise ReleaseVerificationError("the dependency audit contains an invalid package")
        vulnerabilities = dependency.get("vulns")
        if not isinstance(vulnerabilities, list):
            raise ReleaseVerificationError("the dependency audit contains an invalid finding list")
        if vulnerabilities:
            raise ReleaseVerificationError("the dependency audit contains known vulnerabilities")


def verify_secret_scan(report_file: Path) -> None:
    try:
        findings: Any = json.loads(report_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise ReleaseVerificationError("the secret scan is unreadable") from error
    if not isinstance(findings, list):
        raise ReleaseVerificationError("the secret scan has an unsupported format")
    if findings:
        raise ReleaseVerificationError("the secret scan contains findings")


def verify_release(
    *,
    project_file: Path,
    changelog_file: Path,
    dist_dir: Path,
    tag: str,
    live_junit: Path,
    managed_identity_junit: Path,
    managed_identity_repository: str,
    managed_identity_commit: str,
    managed_identity_run_id: int,
    managed_identity_run_attempt: int,
    managed_identity_image_digest: str,
    load_report: Path,
    dependency_audit: Path,
    secret_scan: Path,
    allow_unreleased: bool = False,
) -> str:
    version = read_project_version(project_file)
    verify_tag(tag, version)
    notes = extract_release_notes(
        changelog_file,
        version,
        allow_unreleased=allow_unreleased,
    )
    verify_distributions(dist_dir, version=version)
    verify_live_junit(live_junit)
    verify_managed_identity_junit(
        managed_identity_junit,
        repository=managed_identity_repository,
        commit=managed_identity_commit,
        run_id=managed_identity_run_id,
        run_attempt=managed_identity_run_attempt,
        image_digest=managed_identity_image_digest,
    )
    verify_load_report(load_report)
    verify_dependency_audit(dependency_audit)
    verify_secret_scan(secret_scan)
    return notes


def _verify_wheel(wheel_file: Path, *, version: str) -> None:
    try:
        with zipfile.ZipFile(wheel_file) as archive:
            members = archive.namelist()
            metadata_members = [name for name in members if name.endswith(".dist-info/METADATA")]
            if len(metadata_members) != 1:
                raise ReleaseVerificationError("the wheel has invalid metadata layout")
            metadata = BytesParser(policy=policy.default).parsebytes(
                archive.read(metadata_members[0])
            )
    except (OSError, zipfile.BadZipFile, KeyError) as error:
        raise ReleaseVerificationError("the wheel is unreadable") from error

    _verify_package_metadata(metadata, version=version)
    if "openai_cosmosdb_session/py.typed" not in members:
        raise ReleaseVerificationError("the wheel does not contain py.typed")
    if not any(name.endswith(".dist-info/licenses/LICENSE") for name in members):
        raise ReleaseVerificationError("the wheel does not contain the license file")


def _verify_source_archive(source_archive: Path, *, version: str) -> None:
    try:
        with tarfile.open(source_archive, mode="r:gz") as archive:
            members = archive.getnames()
            metadata_members = [name for name in members if name.endswith("/PKG-INFO")]
            if len(metadata_members) != 1:
                raise ReleaseVerificationError("the source archive has invalid metadata layout")
            extracted = archive.extractfile(metadata_members[0])
            if extracted is None:
                raise ReleaseVerificationError("the source archive metadata is unreadable")
            metadata = BytesParser(policy=policy.default).parse(extracted)
    except (OSError, tarfile.TarError, KeyError) as error:
        raise ReleaseVerificationError("the source archive is unreadable") from error

    _verify_package_metadata(metadata, version=version)
    if not any(name.endswith("/LICENSE") for name in members):
        raise ReleaseVerificationError("the source archive does not contain the license file")
    if not any(name.endswith("/src/openai_cosmosdb_session/py.typed") for name in members):
        raise ReleaseVerificationError("the source archive does not contain py.typed")


def _verify_package_metadata(metadata: Any, *, version: str) -> None:
    if metadata.get("Name") != PROJECT_NAME or metadata.get("Version") != version:
        raise ReleaseVerificationError("distribution name or version metadata is invalid")
    if metadata.get("License-Expression") != "MIT":
        raise ReleaseVerificationError("distribution license expression is not MIT")
    if "LICENSE" not in metadata.get_all("License-File", []):
        raise ReleaseVerificationError("distribution license-file metadata is missing")


def _local_name(tag: object) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _integer_attribute(element: ElementTree.Element, name: str) -> int:
    value = element.get(name, "0")
    try:
        parsed = int(value)
    except ValueError as error:
        raise ReleaseVerificationError("live-test JUnit contains an invalid count") from error
    if parsed < 0:
        raise ReleaseVerificationError("live-test JUnit contains a negative count")
    return parsed


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReleaseVerificationError(f"the {label} is not a JSON object")
    return value


def _integer(document: Mapping[str, Any], name: str) -> int:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReleaseVerificationError("a load scenario contains an invalid integer field")
    return value


def _number(document: Mapping[str, Any], name: str) -> float:
    value = document.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReleaseVerificationError("a load scenario contains an invalid numeric field")
    return float(value)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--live-junit", type=Path, required=True)
    parser.add_argument("--managed-identity-junit", type=Path, required=True)
    parser.add_argument("--managed-identity-repository", required=True)
    parser.add_argument("--managed-identity-commit", required=True)
    parser.add_argument("--managed-identity-run-id", type=int, required=True)
    parser.add_argument("--managed-identity-run-attempt", type=int, required=True)
    parser.add_argument("--managed-identity-image-digest-file", type=Path, required=True)
    parser.add_argument("--load-report", type=Path, required=True)
    parser.add_argument("--dependency-audit", type=Path, required=True)
    parser.add_argument("--secret-scan", type=Path, required=True)
    parser.add_argument("--notes-output", type=Path, required=True)
    parser.add_argument("--allow-unreleased", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_args(argv)
    try:
        notes = verify_release(
            project_file=arguments.project,
            changelog_file=arguments.changelog,
            dist_dir=arguments.dist,
            tag=arguments.tag,
            live_junit=arguments.live_junit,
            managed_identity_junit=arguments.managed_identity_junit,
            managed_identity_repository=arguments.managed_identity_repository,
            managed_identity_commit=arguments.managed_identity_commit,
            managed_identity_run_id=arguments.managed_identity_run_id,
            managed_identity_run_attempt=arguments.managed_identity_run_attempt,
            managed_identity_image_digest=(
                arguments.managed_identity_image_digest_file.read_text(
                    encoding="utf-8"
                ).strip()
            ),
            load_report=arguments.load_report,
            dependency_audit=arguments.dependency_audit,
            secret_scan=arguments.secret_scan,
            allow_unreleased=arguments.allow_unreleased,
        )
    except (OSError, ReleaseVerificationError) as error:
        print(f"release verification failed: {error}")
        return 2

    arguments.notes_output.parent.mkdir(parents=True, exist_ok=True)
    arguments.notes_output.write_text(notes, encoding="utf-8")
    print("release verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
