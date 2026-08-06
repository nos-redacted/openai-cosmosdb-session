from __future__ import annotations

import unittest
from typing import Any

from scripts.verify_workflow_run import (
    WorkflowRunVerificationError,
    verify_workflow_run,
)


class WorkflowRunVerificationTests(unittest.TestCase):
    def test_matching_successful_manual_run_is_accepted(self) -> None:
        verify_workflow_run(
            _run_metadata(),
            run_id=123,
            repository="owner/repository",
            commit="a" * 40,
            workflow_path=".github/workflows/live.yml",
        )

    def test_different_commit_is_rejected(self) -> None:
        metadata = _run_metadata()
        metadata["head_sha"] = "b" * 40

        with self.assertRaisesRegex(WorkflowRunVerificationError, "commit"):
            verify_workflow_run(
                metadata,
                run_id=123,
                repository="owner/repository",
                commit="a" * 40,
                workflow_path=".github/workflows/live.yml",
            )

    def test_unsuccessful_run_is_rejected(self) -> None:
        metadata = _run_metadata()
        metadata["conclusion"] = "failure"

        with self.assertRaisesRegex(WorkflowRunVerificationError, "successfully"):
            verify_workflow_run(
                metadata,
                run_id=123,
                repository="owner/repository",
                commit="a" * 40,
                workflow_path=".github/workflows/live.yml",
            )

    def test_wrong_repository_or_workflow_is_rejected(self) -> None:
        for field, value, message in (
            ("repository", {"full_name": "owner/other"}, "repository"),
            ("path", ".github/workflows/ci.yml", "workflow"),
        ):
            with self.subTest(field=field):
                metadata = _run_metadata()
                metadata[field] = value
                with self.assertRaisesRegex(WorkflowRunVerificationError, message):
                    verify_workflow_run(
                        metadata,
                        run_id=123,
                        repository="owner/repository",
                        commit="a" * 40,
                        workflow_path=".github/workflows/live.yml",
                    )

    def test_scheduled_run_is_rejected(self) -> None:
        metadata = _run_metadata()
        metadata["event"] = "schedule"

        with self.assertRaisesRegex(WorkflowRunVerificationError, "manually"):
            verify_workflow_run(
                metadata,
                run_id=123,
                repository="owner/repository",
                commit="a" * 40,
                workflow_path=".github/workflows/live.yml",
            )


def _run_metadata() -> dict[str, Any]:
    return {
        "id": 123,
        "status": "completed",
        "conclusion": "success",
        "event": "workflow_dispatch",
        "head_sha": "a" * 40,
        "repository": {"full_name": "owner/repository"},
        "path": ".github/workflows/live.yml@refs/heads/main",
    }


if __name__ == "__main__":
    unittest.main()
