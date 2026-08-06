"""Verify that cloud evidence came from the intended GitHub workflow revision."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class WorkflowRunVerificationError(ValueError):
    """A sanitized workflow-run provenance failure."""


def verify_workflow_run(
    metadata: Mapping[str, Any],
    *,
    run_id: int,
    repository: str,
    commit: str,
    workflow_path: str,
) -> None:
    if metadata.get("id") != run_id:
        raise WorkflowRunVerificationError("workflow run ID does not match")
    if metadata.get("status") != "completed" or metadata.get("conclusion") != "success":
        raise WorkflowRunVerificationError("workflow run did not complete successfully")
    if metadata.get("event") != "workflow_dispatch":
        raise WorkflowRunVerificationError("workflow run was not manually dispatched")
    if metadata.get("head_sha") != commit:
        raise WorkflowRunVerificationError("workflow run does not match the release commit")

    repository_metadata = metadata.get("repository")
    if (
        not isinstance(repository_metadata, Mapping)
        or repository_metadata.get("full_name") != repository
    ):
        raise WorkflowRunVerificationError("workflow run does not match the repository")

    actual_path = metadata.get("path")
    if not isinstance(actual_path, str) or actual_path.split("@", 1)[0] != workflow_path:
        raise WorkflowRunVerificationError("workflow run does not match the required workflow")


def load_workflow_run(path: Path) -> Mapping[str, Any]:
    try:
        value: Any = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        raise WorkflowRunVerificationError("workflow run metadata is unreadable") from error
    if not isinstance(value, Mapping):
        raise WorkflowRunVerificationError("workflow run metadata is not a JSON object")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--workflow-path", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parse_args(argv)
    try:
        verify_workflow_run(
            load_workflow_run(arguments.metadata),
            run_id=arguments.run_id,
            repository=arguments.repository,
            commit=arguments.commit,
            workflow_path=arguments.workflow_path,
        )
    except WorkflowRunVerificationError as error:
        print(f"workflow run verification failed: {error}")
        return 2

    print("workflow run verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
