from __future__ import annotations

import argparse
import asyncio
import os
import runpy
import subprocess
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
QUICKSTART = REPOSITORY_ROOT / "examples" / "agents_quickstart.py"


class AgentsQuickstartTests(unittest.TestCase):
    def _run(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.pop("COSMOS_CONNECTION_STRING", None)
        environment.pop("OPENAI_API_KEY", None)
        return subprocess.run(
            [sys.executable, str(QUICKSTART), *arguments],
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_help_is_available_without_credentials_or_runtime_dependencies(self) -> None:
        completed = self._run("--help")

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("closing and reopening", completed.stdout)
        self.assertIn("--cleanup", completed.stdout)

    def test_import_has_no_runtime_side_effects(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                f"import runpy; runpy.run_path({str(QUICKSTART)!r}, run_name='quickstart')",
            ],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_missing_connection_string_fails_before_runtime_imports(self) -> None:
        completed = self._run()

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("Set COSMOS_CONNECTION_STRING", completed.stderr)
        self.assertNotIn("ModuleNotFoundError", completed.stderr)

    def test_non_positive_ttl_is_rejected_by_argument_parser(self) -> None:
        completed = self._run("--ttl-seconds", "0")

        self.assertEqual(completed.returncode, 2)
        self.assertIn("must be a positive integer", completed.stderr)

    def test_two_turn_flow_closes_reopens_restores_and_cleans_up(self) -> None:
        namespace = runpy.run_path(str(QUICKSTART), run_name="quickstart_test")
        run_quickstart = namespace["run_quickstart"]
        events: list[str] = []
        persisted_items: list[dict[str, str]] = []

        class FakeAgent:
            def __init__(self, **_: str) -> None:
                pass

        class FakeRunner:
            @staticmethod
            async def run(_: FakeAgent, prompt: str, *, session: FakeSession) -> SimpleNamespace:
                events.append(f"run:{prompt}")
                persisted_items.append({"role": "user", "content": prompt})
                return SimpleNamespace(final_output="test response")

        class FakeSession:
            def __init__(self, session_id: str) -> None:
                self.session_id = session_id

            @classmethod
            async def from_connection_string(
                cls,
                session_id: str,
                _: str,
                **__: object,
            ) -> FakeSession:
                events.append(f"open:{session_id}")
                return cls(session_id)

            async def get_items(self) -> list[dict[str, str]]:
                events.append(f"read:{self.session_id}")
                return list(persisted_items)

            async def delete_session(self) -> None:
                events.append(f"delete:{self.session_id}")
                persisted_items.clear()

            async def close(self) -> None:
                events.append(f"close:{self.session_id}")

        agents_module = ModuleType("agents")
        agents_module.Agent = FakeAgent  # type: ignore[attr-defined]
        agents_module.Runner = FakeRunner  # type: ignore[attr-defined]
        cosmos_module = ModuleType("openai_cosmosdb_session")
        cosmos_module.CosmosDBSession = FakeSession  # type: ignore[attr-defined]
        arguments = argparse.Namespace(
            session_id="quickstart-test",
            database="agents",
            container="agent_sessions",
            ttl_seconds=None,
            cleanup=True,
        )

        with (
            patch.dict(
                sys.modules,
                {
                    "agents": agents_module,
                    "openai_cosmosdb_session": cosmos_module,
                },
            ),
            patch.dict(
                os.environ,
                {
                    "COSMOS_CONNECTION_STRING": "test-connection-string",
                    "OPENAI_API_KEY": "test-openai-key",
                },
                clear=False,
            ),
            redirect_stdout(StringIO()),
        ):
            asyncio.run(run_quickstart(arguments))

        self.assertEqual(
            events,
            [
                "open:quickstart-test",
                "run:What city is the Golden Gate Bridge in?",
                "close:quickstart-test",
                "open:quickstart-test",
                "read:quickstart-test",
                "run:What state is it in?",
                "read:quickstart-test",
                "delete:quickstart-test",
                "close:quickstart-test",
            ],
        )
        self.assertEqual(persisted_items, [])


if __name__ == "__main__":
    unittest.main()
