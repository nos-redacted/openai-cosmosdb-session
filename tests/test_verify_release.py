from __future__ import annotations

import io
import json
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.verify_release import (
    ReleaseVerificationError,
    extract_release_notes,
    verify_release,
    verify_tag,
)

MANAGED_IDENTITY_REPOSITORY = "nos-redacted/openai-cosmosdb-session"
MANAGED_IDENTITY_COMMIT = "a" * 40
MANAGED_IDENTITY_RUN_ID = 123
MANAGED_IDENTITY_RUN_ATTEMPT = 2
MANAGED_IDENTITY_IMAGE_DIGEST = f"sha256:{'b' * 64}"


class ReleaseVerificationTests(unittest.TestCase):
    def test_valid_release_evidence_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = self._valid_evidence(root)

            notes = self._verify_release(
                project_file=evidence[0],
                changelog_file=evidence[1],
                dist_dir=evidence[2],
                tag="v0.2.0",
                live_junit=evidence[3],
                managed_identity_junit=evidence[4],
                load_report=evidence[5],
                dependency_audit=evidence[6],
                secret_scan=evidence[7],
            )

            self.assertIn("### Added", notes)

    def test_tag_must_exactly_match_static_project_version(self) -> None:
        with self.assertRaisesRegex(ReleaseVerificationError, "tag"):
            verify_tag("v0.2.1", "0.2.0")

    def test_unreleased_changelog_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            changelog = Path(directory, "CHANGELOG.md")
            changelog.write_text(
                "# Changelog\n\n## 0.2.0 - Unreleased\n\n- Pending.\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ReleaseVerificationError, "Unreleased"):
                extract_release_notes(changelog, "0.2.0")

    def test_live_test_failures_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = self._valid_evidence(root)
            junit = evidence[3]
            junit.write_text(
                '<testsuites><testsuite tests="7" failures="1" errors="0" '
                'skipped="0" /></testsuites>',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ReleaseVerificationError, "live tests"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=junit,
                    managed_identity_junit=evidence[4],
                    load_report=evidence[5],
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def test_managed_identity_test_failures_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = self._valid_evidence(root)
            managed_identity_junit = evidence[4]
            managed_identity_junit.write_text(
                '<testsuites><testsuite tests="7" failures="0" errors="0" '
                'skipped="1" /></testsuites>',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ReleaseVerificationError, "live tests"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=evidence[3],
                    managed_identity_junit=managed_identity_junit,
                    load_report=evidence[5],
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def test_sub_baseline_load_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = self._valid_evidence(root)
            load = evidence[5]
            document = json.loads(load.read_text(encoding="utf-8"))
            document["scenarios"][0]["requested_duration_seconds"] = 299
            load.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(ReleaseVerificationError, "duration"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=evidence[3],
                    managed_identity_junit=evidence[4],
                    load_report=load,
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def test_dependency_vulnerability_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._valid_evidence(Path(directory))
            evidence[6].write_text(
                json.dumps(
                    {
                        "dependencies": [
                            {"name": "dependency", "version": "1", "vulns": [{"id": "VULN"}]}
                        ],
                        "fixes": [],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ReleaseVerificationError, "vulnerabilities"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=evidence[3],
                    managed_identity_junit=evidence[4],
                    load_report=evidence[5],
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def test_secret_scan_finding_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._valid_evidence(Path(directory))
            evidence[7].write_text('[{"RuleID": "example"}]', encoding="utf-8")

            with self.assertRaisesRegex(ReleaseVerificationError, "findings"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=evidence[3],
                    managed_identity_junit=evidence[4],
                    load_report=evidence[5],
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def test_managed_identity_provenance_mismatches_are_rejected(self) -> None:
        mismatches = {
            "managed_identity_repository": "other/repository",
            "managed_identity_commit": "c" * 40,
            "managed_identity_run_id": 456,
            "managed_identity_run_attempt": 3,
            "managed_identity_image_digest": f"sha256:{'d' * 64}",
        }
        for argument, value in mismatches.items():
            with self.subTest(argument=argument), tempfile.TemporaryDirectory() as directory:
                evidence = self._valid_evidence(Path(directory))

                with self.assertRaisesRegex(ReleaseVerificationError, "provenance"):
                    self._verify_release(
                        project_file=evidence[0],
                        changelog_file=evidence[1],
                        dist_dir=evidence[2],
                        tag="v0.2.0",
                        live_junit=evidence[3],
                        managed_identity_junit=evidence[4],
                        load_report=evidence[5],
                        dependency_audit=evidence[6],
                        secret_scan=evidence[7],
                        **{argument: value},
                    )

    def test_managed_identity_auth_mode_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            evidence = self._valid_evidence(Path(directory))
            managed_identity_junit = evidence[4]
            managed_identity_junit.write_bytes(
                _managed_identity_junit().replace(b"managed-identity", b"azure-cli")
            )

            with self.assertRaisesRegex(ReleaseVerificationError, "azure_auth_mode"):
                self._verify_release(
                    project_file=evidence[0],
                    changelog_file=evidence[1],
                    dist_dir=evidence[2],
                    tag="v0.2.0",
                    live_junit=evidence[3],
                    managed_identity_junit=managed_identity_junit,
                    load_report=evidence[5],
                    dependency_audit=evidence[6],
                    secret_scan=evidence[7],
                )

    def _verify_release(self, **arguments: object) -> str:
        provenance: dict[str, object] = {
            "managed_identity_repository": MANAGED_IDENTITY_REPOSITORY,
            "managed_identity_commit": MANAGED_IDENTITY_COMMIT,
            "managed_identity_run_id": MANAGED_IDENTITY_RUN_ID,
            "managed_identity_run_attempt": MANAGED_IDENTITY_RUN_ATTEMPT,
            "managed_identity_image_digest": MANAGED_IDENTITY_IMAGE_DIGEST,
        }
        provenance.update(arguments)
        return verify_release(**provenance)  # type: ignore[arg-type]

    def _valid_evidence(
        self,
        root: Path,
    ) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path]:
        project = root / "pyproject.toml"
        project.write_text(
            '[project]\nname = "openai-cosmosdb-session"\nversion = "0.2.0"\n',
            encoding="utf-8",
        )
        changelog = root / "CHANGELOG.md"
        changelog.write_text(
            "# Changelog\n\n## 0.2.0 - 2026-04-01\n\n### Added\n\n- Safe release.\n",
            encoding="utf-8",
        )
        dist = root / "dist"
        dist.mkdir()
        self._write_wheel(dist / "openai_cosmosdb_session-0.2.0-py3-none-any.whl")
        self._write_sdist(dist / "openai_cosmosdb_session-0.2.0.tar.gz")
        junit = root / "live.xml"
        junit.write_text(
            '<testsuites><testsuite tests="7" failures="0" errors="0" '
            'skipped="0" /></testsuites>',
            encoding="utf-8",
        )
        managed_identity_junit = root / "managed-identity.xml"
        managed_identity_junit.write_bytes(_managed_identity_junit())
        load = root / "load.json"
        load.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "scenarios": [
                        {
                            "scenario": "mixed",
                            "failed_operations": 0,
                            "integrity_errors": [],
                            "cleanup_succeeded": True,
                            "concurrency": 32,
                            "requested_rate_per_second": 100,
                            "requested_duration_seconds": 300,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        dependency_audit = root / "dependency-audit.json"
        dependency_audit.write_text(
            json.dumps(
                {
                    "dependencies": [
                        {"name": "dependency", "version": "1", "vulns": []}
                    ],
                    "fixes": [],
                }
            ),
            encoding="utf-8",
        )
        secret_scan = root / "secret-scan.json"
        secret_scan.write_text("[]", encoding="utf-8")
        return (
            project,
            changelog,
            dist,
            junit,
            managed_identity_junit,
            load,
            dependency_audit,
            secret_scan,
        )

    def _write_wheel(self, path: Path) -> None:
        with zipfile.ZipFile(path, mode="w") as archive:
            archive.writestr(
                "openai_cosmosdb_session-0.2.0.dist-info/METADATA",
                _metadata(),
            )
            archive.writestr("openai_cosmosdb_session/py.typed", "")
            archive.writestr(
                "openai_cosmosdb_session-0.2.0.dist-info/licenses/LICENSE",
                "MIT",
            )

    def _write_sdist(self, path: Path) -> None:
        members = {
            "openai_cosmosdb_session-0.2.0/PKG-INFO": _metadata().encode(),
            "openai_cosmosdb_session-0.2.0/LICENSE": b"MIT",
            "openai_cosmosdb_session-0.2.0/src/openai_cosmosdb_session/py.typed": b"",
        }
        with tarfile.open(path, mode="w:gz") as archive:
            for name, content in members.items():
                info = tarfile.TarInfo(name)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))


def _metadata() -> str:
    return (
        "Metadata-Version: 2.4\n"
        "Name: openai-cosmosdb-session\n"
        "Version: 0.2.0\n"
        "License-Expression: MIT\n"
        "License-File: LICENSE\n"
        "\n"
    )


def _managed_identity_junit() -> bytes:
    properties = {
        "azure_auth_mode": "managed-identity",
        "github_repository": MANAGED_IDENTITY_REPOSITORY,
        "github_run_attempt": str(MANAGED_IDENTITY_RUN_ATTEMPT),
        "github_run_id": str(MANAGED_IDENTITY_RUN_ID),
        "image_digest": MANAGED_IDENTITY_IMAGE_DIGEST,
        "source_commit": MANAGED_IDENTITY_COMMIT,
    }
    property_xml = "".join(
        f'<property name="{name}" value="{value}" />'
        for name, value in properties.items()
    )
    return (
        '<testsuites><testsuite tests="7" failures="0" errors="0" skipped="0">'
        f"<properties>{property_xml}</properties></testsuite></testsuites>"
    ).encode()


if __name__ == "__main__":
    unittest.main()
