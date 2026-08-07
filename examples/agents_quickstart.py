"""Run a two-turn OpenAI Agents SDK conversation backed by Cosmos DB."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from uuid import uuid4


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run two agent turns, closing and reopening the Cosmos DB session "
            "between them to prove persistence."
        ),
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help=(
            "Cosmos session ID. Defaults to COSMOS_SESSION_ID or a generated "
            "quickstart ID."
        ),
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("COSMOS_DATABASE", "agents"),
        help="Cosmos database name (default: COSMOS_DATABASE or agents).",
    )
    parser.add_argument(
        "--container",
        default=os.environ.get("COSMOS_CONTAINER", "agent_sessions"),
        help="Cosmos container name (default: COSMOS_CONTAINER or agent_sessions).",
    )
    parser.add_argument(
        "--ttl-seconds",
        type=_positive_int,
        default=None,
        help="Optional positive per-session TTL in seconds.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the quickstart session after the persistence check.",
    )
    return parser


def _required_environment_variable(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value
    raise SystemExit(f"Set {name} before running the quickstart.")


async def run_quickstart(args: argparse.Namespace) -> None:
    connection_string = _required_environment_variable("COSMOS_CONNECTION_STRING")
    _required_environment_variable("OPENAI_API_KEY")

    from agents import Agent, Runner

    from openai_cosmosdb_session import CosmosDBSession

    session_id = (
        args.session_id
        or os.environ.get("COSMOS_SESSION_ID")
        or f"agents-quickstart-{uuid4().hex}"
    )

    agent = Agent(
        name="Travel assistant",
        instructions="Answer in one concise sentence and remember prior context.",
    )

    print(f"Session ID: {session_id}")
    first_session = await CosmosDBSession.from_connection_string(
        session_id,
        connection_string,
        database=args.database,
        container=args.container,
        ttl_seconds=args.ttl_seconds,
    )
    try:
        first_result = await Runner.run(
            agent,
            "What city is the Golden Gate Bridge in?",
            session=first_session,
        )
        print(f"First turn: {first_result.final_output}")
    finally:
        await first_session.close()

    print("Closed the first client; reopening the same session.")
    resumed_session = await CosmosDBSession.from_connection_string(
        session_id,
        connection_string,
        database=args.database,
        container=args.container,
        ttl_seconds=args.ttl_seconds,
    )
    try:
        restored_history = await resumed_session.get_items()
        if not restored_history:
            raise RuntimeError("No persisted history was found after reopening the session.")
        print(f"Restored {len(restored_history)} item(s) before the second turn.")

        second_result = await Runner.run(
            agent,
            "What state is it in?",
            session=resumed_session,
        )
        print(f"Second turn: {second_result.final_output}")

        final_history = await resumed_session.get_items()
        print(f"Persisted history now contains {len(final_history)} item(s).")

        if args.cleanup:
            await resumed_session.delete_session()
            print("Deleted the quickstart session.")
        else:
            print("Session retained for inspection. Re-run with --cleanup to delete it.")
    finally:
        await resumed_session.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    asyncio.run(run_quickstart(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
