"""Fixed-rate Azure Cosmos DB load validation for the session backend."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import uuid4

from azure.cosmos.aio import CosmosClient

from openai_cosmosdb_session import CosmosDBSession, CosmosOperationEvent

RESULT_SCHEMA_VERSION = 1
SCENARIOS = (
    "spread-append",
    "hot-append",
    "multi-item-append",
    "latest-n-read",
    "clear-pop-overlap",
    "deduplicated-retry",
    "mixed",
)


@dataclass(frozen=True)
class LatencySummary:
    """Latency distribution in milliseconds."""

    sample_count: int
    minimum_ms: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    maximum_ms: float | None

    @classmethod
    def from_seconds(cls, samples: list[float]) -> LatencySummary:
        if any(not math.isfinite(sample) or sample < 0 for sample in samples):
            raise ValueError("latency samples must be finite and non-negative")
        if not samples:
            return cls(0, None, None, None, None, None)

        ordered_ms = sorted(sample * 1000 for sample in samples)
        return cls(
            sample_count=len(ordered_ms),
            minimum_ms=ordered_ms[0],
            p50_ms=_percentile(ordered_ms, 0.50),
            p95_ms=_percentile(ordered_ms, 0.95),
            p99_ms=_percentile(ordered_ms, 0.99),
            maximum_ms=ordered_ms[-1],
        )


@dataclass(frozen=True)
class ErrorCount:
    """A bounded error code and its occurrence count."""

    code: str
    count: int


@dataclass(frozen=True)
class ScenarioResult:
    """Sanitized measurements for one load scenario."""

    schema_version: int
    scenario: str
    started_at: str
    warmup_seconds: float
    requested_duration_seconds: float
    measured_duration_seconds: float
    concurrency: int
    requested_rate_per_second: float
    offered_rate_per_second: float
    achieved_rate_per_second: float
    scheduled_operations: int
    completed_operations: int
    successful_operations: int
    failed_operations: int
    latency: LatencySummary
    request_charge_total: float
    request_charge_per_success: float | None
    retry_events: int
    throttled_events: int
    errors: tuple[ErrorCount, ...]
    integrity_errors: tuple[str, ...]
    cleanup_succeeded: bool


@dataclass(frozen=True)
class LoadReport:
    """Serializable output from one invocation of the load runner."""

    schema_version: int
    scenarios: tuple[ScenarioResult, ...]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True) + "\n"


@dataclass(frozen=True)
class LoadConfig:
    """Validated command-line configuration with no credentials or endpoint."""

    auth_mode: str
    database: str
    container: str
    scenarios: tuple[str, ...]
    concurrency: int = 32
    requested_rate_per_second: float = 100
    duration_seconds: float = 300
    warmup_seconds: float = 30
    session_count: int = 128
    multi_item_count: int = 3
    latest_n: int = 10
    ttl_seconds: int | None = 3600
    output: Path = Path("load-results.json")

    def __post_init__(self) -> None:
        if self.auth_mode not in {
            "connection-string",
            "developer",
            "azure-cli",
            "managed-identity",
        }:
            raise ValueError("unsupported authentication mode")
        if not self.database or not self.container:
            raise ValueError("database and container must not be empty")
        if not self.scenarios or any(scenario not in SCENARIOS for scenario in self.scenarios):
            raise ValueError("at least one supported scenario is required")
        if self.concurrency < 1 or self.session_count < 1:
            raise ValueError("concurrency and session count must be positive")
        if self.requested_rate_per_second <= 0 or self.duration_seconds <= 0:
            raise ValueError("rate and duration must be positive")
        if self.requested_rate_per_second * self.duration_seconds < 1:
            raise ValueError("rate and duration must schedule at least one operation")
        if self.warmup_seconds < 0:
            raise ValueError("warmup must not be negative")
        if self.multi_item_count < 1 or self.latest_n < 1:
            raise ValueError("item counts must be positive")
        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            raise ValueError("TTL must be positive when enabled")


class _MetricsObserver:
    def __init__(self) -> None:
        self.request_charge_total = 0.0
        self.retry_events = 0
        self.throttled_events = 0
        self.error_counts: Counter[str] = Counter()

    def on_event(self, event: CosmosOperationEvent) -> None:
        if event.request_charge is not None:
            self.request_charge_total += event.request_charge
        if event.outcome == "retry":
            self.retry_events += 1
        if event.throttled:
            self.throttled_events += 1
        if event.outcome == "failure":
            self.error_counts[event.code or "unclassified_failure"] += 1

    def record_harness_integrity_failure(self) -> None:
        self.error_counts["harness_integrity_error"] += 1


@dataclass
class _Execution:
    scheduled_operations: int
    measured_duration_seconds: float
    successful_indices: list[int]
    failed_operations: int
    latencies_seconds: list[float]


@dataclass(frozen=True)
class _PreparedWorkload:
    operation: Callable[[int], Awaitable[None]]
    session_keys: tuple[str, ...]
    active_session_count: int


class _HarnessIntegrityError(Exception):
    pass


class _ScenarioContext:
    def __init__(
        self,
        *,
        client: CosmosClient,
        config: LoadConfig,
        observer: _MetricsObserver,
    ) -> None:
        self._client = client
        self._config = config
        self._observer = observer
        self._namespace = uuid4().hex
        self._sessions: dict[tuple[str, bool], CosmosDBSession] = {}
        self._session_ids: set[str] = set()

    def session(self, key: str, *, observed: bool = True) -> CosmosDBSession:
        cache_key = (key, observed)
        existing = self._sessions.get(cache_key)
        if existing is not None:
            return existing

        session_id = f"openai-cosmos-load-{self._namespace}-{key}"
        self._session_ids.add(session_id)
        session = CosmosDBSession(
            session_id,
            client=self._client,
            database=self._config.database,
            container=self._config.container,
            ttl_seconds=self._config.ttl_seconds,
            max_write_attempts=5,
            observer=self._observer if observed else None,
        )
        self._sessions[cache_key] = session
        return session

    def operation_id(self, index: int) -> str:
        return f"{self._namespace}:{index}"

    async def cleanup(self) -> bool:
        semaphore = asyncio.Semaphore(self._config.concurrency)

        async def delete(session_id: str) -> bool:
            async with semaphore:
                session = CosmosDBSession(
                    session_id,
                    client=self._client,
                    database=self._config.database,
                    container=self._config.container,
                )
                try:
                    await session.delete_session()
                except Exception:
                    return False
                return True

        results = await asyncio.gather(*(delete(session_id) for session_id in self._session_ids))
        return all(results)


def build_scenario_result(
    *,
    scenario: str,
    started_at: str,
    warmup_seconds: float,
    requested_duration_seconds: float,
    measured_duration_seconds: float,
    concurrency: int,
    requested_rate_per_second: float,
    scheduled_operations: int,
    successful_operations: int,
    failed_operations: int,
    latencies_seconds: list[float],
    request_charge_total: float,
    retry_events: int,
    throttled_events: int,
    error_counts: dict[str, int],
    integrity_errors: list[str],
    cleanup_succeeded: bool,
) -> ScenarioResult:
    completed_operations = successful_operations + failed_operations
    numeric_counts = (
        scheduled_operations,
        successful_operations,
        failed_operations,
        retry_events,
        throttled_events,
    )
    if not scenario:
        raise ValueError("scenario must not be empty")
    if any(count < 0 for count in numeric_counts):
        raise ValueError("operation and event counts must not be negative")
    if completed_operations > scheduled_operations:
        raise ValueError("completed operations must not exceed scheduled operations")
    if len(latencies_seconds) != completed_operations:
        raise ValueError("one latency sample is required for each completed operation")
    if concurrency < 1:
        raise ValueError("concurrency must be at least one")
    if requested_duration_seconds <= 0 or measured_duration_seconds <= 0:
        raise ValueError("duration must be positive")
    if warmup_seconds < 0 or requested_rate_per_second <= 0:
        raise ValueError("warmup must be non-negative and rate must be positive")
    if not math.isfinite(request_charge_total) or request_charge_total < 0:
        raise ValueError("request charge must be finite and non-negative")
    if any(not code or count < 1 for code, count in error_counts.items()):
        raise ValueError("error counts require non-empty codes and positive counts")

    offered_rate = scheduled_operations / requested_duration_seconds
    achieved_rate = completed_operations / measured_duration_seconds
    request_charge_per_success = (
        request_charge_total / successful_operations if successful_operations else None
    )
    return ScenarioResult(
        schema_version=RESULT_SCHEMA_VERSION,
        scenario=scenario,
        started_at=started_at,
        warmup_seconds=warmup_seconds,
        requested_duration_seconds=requested_duration_seconds,
        measured_duration_seconds=measured_duration_seconds,
        concurrency=concurrency,
        requested_rate_per_second=requested_rate_per_second,
        offered_rate_per_second=offered_rate,
        achieved_rate_per_second=achieved_rate,
        scheduled_operations=scheduled_operations,
        completed_operations=completed_operations,
        successful_operations=successful_operations,
        failed_operations=failed_operations,
        latency=LatencySummary.from_seconds(latencies_seconds),
        request_charge_total=request_charge_total,
        request_charge_per_success=request_charge_per_success,
        retry_events=retry_events,
        throttled_events=throttled_events,
        errors=tuple(
            ErrorCount(code=code, count=count)
            for code, count in sorted(error_counts.items())
        ),
        integrity_errors=tuple(integrity_errors),
        cleanup_succeeded=cleanup_succeeded,
    )


def _percentile(ordered_values: list[float], quantile: float) -> float:
    if not ordered_values:
        raise ValueError("ordered_values must not be empty")
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between zero and one")

    position = (len(ordered_values) - 1) * quantile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return ordered_values[lower_index]
    fraction = position - lower_index
    return (
        ordered_values[lower_index]
        + (ordered_values[upper_index] - ordered_values[lower_index]) * fraction
    )


async def run_load(config: LoadConfig) -> LoadReport:
    """Run configured scenarios against one validated shared Cosmos client."""
    results: list[ScenarioResult] = []
    async with _open_client(config) as client:
        validation_session = CosmosDBSession(
            f"openai-cosmos-load-validation-{uuid4().hex}",
            client=client,
            database=config.database,
            container=config.container,
            ttl_seconds=config.ttl_seconds,
        )
        validation = await validation_session.validate_container()
        validation.raise_for_invalid()

        for scenario in config.scenarios:
            if config.warmup_seconds > 0:
                warmup = await _run_scenario_phase(
                    client=client,
                    config=config,
                    scenario=scenario,
                    duration_seconds=config.warmup_seconds,
                    collect_integrity=False,
                )
                if warmup.failed_operations or not warmup.cleanup_succeeded:
                    raise RuntimeError("load warmup failed")

            measured = await _run_scenario_phase(
                client=client,
                config=config,
                scenario=scenario,
                duration_seconds=config.duration_seconds,
                collect_integrity=True,
            )
            results.append(
                build_scenario_result(
                    scenario=scenario,
                    started_at=measured.started_at,
                    warmup_seconds=config.warmup_seconds,
                    requested_duration_seconds=config.duration_seconds,
                    measured_duration_seconds=measured.execution.measured_duration_seconds,
                    concurrency=config.concurrency,
                    requested_rate_per_second=config.requested_rate_per_second,
                    scheduled_operations=measured.execution.scheduled_operations,
                    successful_operations=len(measured.execution.successful_indices),
                    failed_operations=measured.execution.failed_operations,
                    latencies_seconds=measured.execution.latencies_seconds,
                    request_charge_total=measured.observer.request_charge_total,
                    retry_events=measured.observer.retry_events,
                    throttled_events=measured.observer.throttled_events,
                    error_counts=dict(measured.observer.error_counts),
                    integrity_errors=measured.integrity_errors,
                    cleanup_succeeded=measured.cleanup_succeeded,
                )
            )
    return LoadReport(schema_version=RESULT_SCHEMA_VERSION, scenarios=tuple(results))


@dataclass(frozen=True)
class _PhaseResult:
    started_at: str
    execution: _Execution
    observer: _MetricsObserver
    integrity_errors: list[str]
    cleanup_succeeded: bool

    @property
    def failed_operations(self) -> int:
        return self.execution.failed_operations


async def _run_scenario_phase(
    *,
    client: CosmosClient,
    config: LoadConfig,
    scenario: str,
    duration_seconds: float,
    collect_integrity: bool,
) -> _PhaseResult:
    observer = _MetricsObserver()
    context = _ScenarioContext(client=client, config=config, observer=observer)
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        prepared = await _prepare_workload(
            scenario=scenario,
            context=context,
            config=config,
            duration_seconds=duration_seconds,
        )
        execution = await _execute_fixed_rate(
            operation=prepared.operation,
            observer=observer,
            concurrency=config.concurrency,
            requested_rate_per_second=config.requested_rate_per_second,
            duration_seconds=duration_seconds,
        )
        integrity_errors = (
            await _validate_integrity(
                scenario=scenario,
                context=context,
                config=config,
                prepared=prepared,
                execution=execution,
            )
            if collect_integrity
            else []
        )
    finally:
        cleanup_succeeded = await context.cleanup()
    return _PhaseResult(
        started_at=started_at,
        execution=execution,
        observer=observer,
        integrity_errors=integrity_errors,
        cleanup_succeeded=cleanup_succeeded,
    )


async def _execute_fixed_rate(
    *,
    operation: Callable[[int], Awaitable[None]],
    observer: _MetricsObserver,
    concurrency: int,
    requested_rate_per_second: float,
    duration_seconds: float,
) -> _Execution:
    scheduled_operations = math.floor(requested_rate_per_second * duration_seconds)
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    successful_indices: list[int] = []
    latencies: list[float] = []
    failed_operations = 0

    async def worker() -> None:
        nonlocal failed_operations
        while True:
            index = await queue.get()
            if index is None:
                return
            started = monotonic()
            try:
                await operation(index)
            except _HarnessIntegrityError:
                observer.record_harness_integrity_failure()
                failed_operations += 1
            except Exception:
                failed_operations += 1
            else:
                successful_indices.append(index)
            finally:
                latencies.append(monotonic() - started)

    started = monotonic()
    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    for index in range(scheduled_operations):
        target = started + index / requested_rate_per_second
        delay = target - monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        await queue.put(index)
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)
    elapsed = max(monotonic() - started, sys.float_info.epsilon)
    return _Execution(
        scheduled_operations=scheduled_operations,
        measured_duration_seconds=elapsed,
        successful_indices=successful_indices,
        failed_operations=failed_operations,
        latencies_seconds=latencies,
    )


async def _prepare_workload(
    *,
    scenario: str,
    context: _ScenarioContext,
    config: LoadConfig,
    duration_seconds: float,
) -> _PreparedWorkload:
    scheduled_operations = math.floor(config.requested_rate_per_second * duration_seconds)
    active_session_count = min(config.session_count, max(1, scheduled_operations))

    def spread_key(index: int) -> str:
        return f"spread-{index % active_session_count}"

    def dedupe_key(index: int) -> str:
        return f"dedupe-{index % active_session_count}"

    async def append_one(index: int, *, key: str) -> None:
        await context.session(key).add_items(
            [{"role": "user", "content": f"load-item-{index}"}]
        )

    if scenario == "spread-append":

        async def operation(index: int) -> None:
            await append_one(index, key=spread_key(index))

        keys = tuple(f"spread-{index}" for index in range(active_session_count))
    elif scenario == "hot-append":

        async def operation(index: int) -> None:
            await append_one(index, key="hot")

        keys = ("hot",)
    elif scenario == "multi-item-append":

        async def operation(index: int) -> None:
            await context.session(spread_key(index)).add_items(
                [
                    {"role": "user", "content": f"load-item-{index}-{item_index}"}
                    for item_index in range(config.multi_item_count)
                ]
            )

        keys = tuple(f"spread-{index}" for index in range(active_session_count))
    elif scenario == "latest-n-read":
        keys = tuple(f"read-{index}" for index in range(active_session_count))

        async def seed(key: str) -> None:
            await context.session(key, observed=False).add_items(
                [
                    {"role": "user", "content": f"seed-{item_index}"}
                    for item_index in range(config.latest_n)
                ]
            )

        await _run_bounded(
            (_bind_string(seed, key) for key in keys),
            concurrency=config.concurrency,
        )

        async def operation(index: int) -> None:
            items = await context.session(keys[index % len(keys)]).get_items(
                limit=config.latest_n
            )
            if len(items) != config.latest_n:
                raise _HarnessIntegrityError

    elif scenario == "clear-pop-overlap":
        overlap_count = min(4, active_session_count)
        keys = tuple(f"overlap-{index}" for index in range(overlap_count))

        async def operation(index: int) -> None:
            session = context.session(keys[(index // 4) % len(keys)])
            branch = index % 4
            if branch in {0, 3}:
                await session.add_items(
                    [{"role": "user", "content": f"overlap-{index}"}]
                )
            elif branch == 1:
                await session.pop_item()
            else:
                await session.clear_session()

    elif scenario == "deduplicated-retry":

        async def operation(index: int) -> None:
            session = context.session(dedupe_key(index))
            await asyncio.gather(
                session.add_items_once(
                    [{"role": "user", "content": f"deduplicated-{index}"}],
                    operation_id=context.operation_id(index),
                ),
                session.add_items_once(
                    [{"role": "user", "content": f"deduplicated-{index}"}],
                    operation_id=context.operation_id(index),
                ),
            )

        keys = tuple(f"dedupe-{index}" for index in range(active_session_count))
    elif scenario == "mixed":
        keys = tuple(f"mixed-{index}" for index in range(active_session_count))

        async def operation(index: int) -> None:
            session = context.session(keys[index % len(keys)])
            branch = index % 10
            if branch <= 3:
                await session.add_items(
                    [{"role": "user", "content": f"mixed-{index}"}]
                )
            elif branch <= 6:
                await session.get_items(limit=config.latest_n)
            elif branch == 7:
                await session.pop_item()
            elif branch == 8:
                await session.clear_session()
            else:
                await session.add_items_once(
                    [{"role": "user", "content": f"mixed-once-{index}"}],
                    operation_id=context.operation_id(index),
                )

    else:
        raise ValueError("unsupported scenario")

    return _PreparedWorkload(
        operation=operation,
        session_keys=keys,
        active_session_count=active_session_count,
    )


async def _run_bounded(
    operations: Iterable[Callable[[], Awaitable[None]]],
    *,
    concurrency: int,
) -> None:
    semaphore = asyncio.Semaphore(concurrency)

    async def run(operation: Callable[[], Awaitable[None]]) -> None:
        async with semaphore:
            await operation()

    await asyncio.gather(*(run(operation) for operation in operations))


def _bind_string(
    operation: Callable[[str], Awaitable[None]],
    value: str,
) -> Callable[[], Awaitable[None]]:
    async def bound() -> None:
        await operation(value)

    return bound


async def _validate_integrity(
    *,
    scenario: str,
    context: _ScenarioContext,
    config: LoadConfig,
    prepared: _PreparedWorkload,
    execution: _Execution,
) -> list[str]:
    documents_per_key: dict[str, int] = {}
    read_failed = False

    async def inspect(key: str) -> None:
        nonlocal read_failed
        try:
            items = await context.session(key, observed=False).get_items()
        except Exception:
            read_failed = True
            return
        documents_per_key[key] = len(items)

    await _run_bounded(
        (_bind_string(inspect, key) for key in prepared.session_keys),
        concurrency=config.concurrency,
    )
    errors: set[str] = set()
    if read_failed:
        errors.add("integrity_read_failed")

    if execution.failed_operations == 0:
        expected: Counter[str] = Counter()
        if scenario == "spread-append":
            expected.update(
                f"spread-{index % prepared.active_session_count}"
                for index in execution.successful_indices
            )
        elif scenario == "hot-append":
            expected["hot"] = len(execution.successful_indices)
        elif scenario == "multi-item-append":
            for index in execution.successful_indices:
                expected[f"spread-{index % prepared.active_session_count}"] += (
                    config.multi_item_count
                )
        elif scenario == "latest-n-read":
            expected.update(dict.fromkeys(prepared.session_keys, config.latest_n))
        elif scenario == "deduplicated-retry":
            expected.update(
                f"dedupe-{index % prepared.active_session_count}"
                for index in execution.successful_indices
            )

        if expected and any(
            documents_per_key.get(key) != count for key, count in expected.items()
        ):
            errors.add("message_count_mismatch")
    return sorted(errors)


@asynccontextmanager
async def _open_client(config: LoadConfig) -> AsyncIterator[CosmosClient]:
    connection_string = os.getenv("COSMOS_CONNECTION_STRING")
    endpoint = os.getenv("COSMOS_ENDPOINT")
    credential: Any = None
    if config.auth_mode == "connection-string":
        if not connection_string or endpoint:
            raise RuntimeError(
                "connection-string auth requires COSMOS_CONNECTION_STRING "
                "and forbids COSMOS_ENDPOINT"
            )
        client = CosmosClient.from_connection_string(
            connection_string,
            consistency_level="Session",
        )
    else:
        if not endpoint or connection_string:
            raise RuntimeError(
                "token auth requires COSMOS_ENDPOINT and forbids COSMOS_CONNECTION_STRING"
            )
        try:
            if config.auth_mode == "developer":
                from azure.identity.aio import DefaultAzureCredential

                credential = DefaultAzureCredential(
                    exclude_managed_identity_credential=True,
                )
            elif config.auth_mode == "azure-cli":
                from azure.identity.aio import AzureCliCredential

                credential = AzureCliCredential()
            else:
                from azure.identity.aio import ManagedIdentityCredential

                credential = ManagedIdentityCredential(
                    client_id=os.getenv("AZURE_CLIENT_ID"),
                )
        except ImportError as error:
            raise RuntimeError("token auth requires the identity extra") from error
        client = CosmosClient(endpoint, credential, consistency_level="Session")

    await client.__aenter__()
    try:
        yield client
    finally:
        await client.close()
        if credential is not None:
            await credential.close()


def parse_args(argv: Sequence[str] | None = None) -> LoadConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--auth-mode",
        choices=("connection-string", "developer", "azure-cli", "managed-identity"),
        default=os.getenv("COSMOS_AUTH_MODE"),
        required=os.getenv("COSMOS_AUTH_MODE") is None,
    )
    parser.add_argument("--database", default=os.getenv("COSMOS_DATABASE", "agents"))
    parser.add_argument(
        "--container",
        default=os.getenv("COSMOS_CONTAINER", "agent_sessions_load"),
    )
    parser.add_argument("--scenario", action="append", choices=(*SCENARIOS, "all"))
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--session-count", type=int, default=128)
    parser.add_argument("--multi-item-count", type=int, default=3)
    parser.add_argument("--latest-n", type=int, default=10)
    parser.add_argument("--ttl-seconds", type=int, default=3600)
    parser.add_argument("--output", type=Path, default=Path("load-results.json"))
    arguments = parser.parse_args(argv)
    selected = tuple(arguments.scenario or ("mixed",))
    scenarios = SCENARIOS if "all" in selected else selected
    return LoadConfig(
        auth_mode=arguments.auth_mode,
        database=arguments.database,
        container=arguments.container,
        scenarios=scenarios,
        concurrency=arguments.concurrency,
        requested_rate_per_second=arguments.rate,
        duration_seconds=arguments.duration,
        warmup_seconds=arguments.warmup,
        session_count=arguments.session_count,
        multi_item_count=arguments.multi_item_count,
        latest_n=arguments.latest_n,
        ttl_seconds=arguments.ttl_seconds or None,
        output=arguments.output,
    )


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_args(argv)
    try:
        report = asyncio.run(run_load(config))
    except Exception:
        print("Load run failed; inspect protected service diagnostics.", file=sys.stderr)
        return 2

    output = report.to_json()
    config.output.parent.mkdir(parents=True, exist_ok=True)
    config.output.write_text(output, encoding="utf-8")
    print(output, end="")
    acceptable = all(
        scenario.failed_operations == 0
        and not scenario.integrity_errors
        and scenario.cleanup_succeeded
        for scenario in report.scenarios
    )
    return 0 if acceptable else 1


if __name__ == "__main__":
    raise SystemExit(main())
