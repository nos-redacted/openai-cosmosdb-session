from __future__ import annotations

import json
import unittest

from benchmarks.load_cosmos import (
    RESULT_SCHEMA_VERSION,
    LatencySummary,
    LoadConfig,
    LoadReport,
    _execute_fixed_rate,
    _MetricsObserver,
    _prepare_workload,
    build_scenario_result,
)


class LoadResultTests(unittest.TestCase):
    def test_latency_summary_uses_interpolated_percentiles(self) -> None:
        summary = LatencySummary.from_seconds([0.004, 0.001, 0.003, 0.002])

        self.assertEqual(summary.sample_count, 4)
        self.assertEqual(summary.minimum_ms, 1.0)
        self.assertEqual(summary.p50_ms, 2.5)
        self.assertAlmostEqual(summary.p95_ms or 0, 3.85)
        self.assertAlmostEqual(summary.p99_ms or 0, 3.97)
        self.assertEqual(summary.maximum_ms, 4.0)

    def test_empty_latency_summary_is_explicit(self) -> None:
        self.assertEqual(
            LatencySummary.from_seconds([]),
            LatencySummary(0, None, None, None, None, None),
        )

    def test_result_derives_rates_charge_and_stable_error_order(self) -> None:
        result = build_scenario_result(
            scenario="mixed",
            started_at="2026-04-01T00:00:00+00:00",
            warmup_seconds=5,
            requested_duration_seconds=10,
            measured_duration_seconds=12,
            concurrency=4,
            requested_rate_per_second=2,
            scheduled_operations=20,
            successful_operations=18,
            failed_operations=2,
            latencies_seconds=[0.01] * 20,
            request_charge_total=45,
            retry_events=3,
            throttled_events=2,
            error_counts={"z_error": 1, "a_error": 1},
            integrity_errors=[],
            cleanup_succeeded=True,
        )

        self.assertEqual(result.offered_rate_per_second, 2)
        self.assertAlmostEqual(result.achieved_rate_per_second, 20 / 12)
        self.assertEqual(result.request_charge_per_success, 2.5)
        self.assertEqual([error.code for error in result.errors], ["a_error", "z_error"])

        report = LoadReport(RESULT_SCHEMA_VERSION, (result,))
        document = json.loads(report.to_json())
        self.assertEqual(document["schema_version"], RESULT_SCHEMA_VERSION)
        self.assertEqual(document["scenarios"][0]["scenario"], "mixed")

    def test_result_rejects_inconsistent_completed_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "latency"):
            build_scenario_result(
                scenario="append",
                started_at="2026-04-01T00:00:00+00:00",
                warmup_seconds=0,
                requested_duration_seconds=1,
                measured_duration_seconds=1,
                concurrency=1,
                requested_rate_per_second=1,
                scheduled_operations=1,
                successful_operations=1,
                failed_operations=0,
                latencies_seconds=[],
                request_charge_total=0,
                retry_events=0,
                throttled_events=0,
                error_counts={},
                integrity_errors=[],
                cleanup_succeeded=True,
            )


class _OperationSession:
    def __init__(self, key: str, calls: list[tuple[str, str]]) -> None:
        self._key = key
        self._calls = calls

    async def add_items(self, _: object) -> None:
        self._calls.append((self._key, "add"))

    async def pop_item(self) -> None:
        self._calls.append((self._key, "pop"))

    async def clear_session(self) -> None:
        self._calls.append((self._key, "clear"))


class _OperationContext:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def session(self, key: str, *, observed: bool = True) -> _OperationSession:
        del observed
        return _OperationSession(key, self.calls)


class LoadRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_fixed_rate_runner_accounts_for_each_operation(self) -> None:
        async def operation(index: int) -> None:
            if index == 1:
                raise RuntimeError("not retained")

        execution = await _execute_fixed_rate(
            operation=operation,
            observer=_MetricsObserver(),
            concurrency=2,
            requested_rate_per_second=1000,
            duration_seconds=0.003,
        )

        self.assertEqual(execution.scheduled_operations, 3)
        self.assertEqual(sorted(execution.successful_indices), [0, 2])
        self.assertEqual(execution.failed_operations, 1)
        self.assertEqual(len(execution.latencies_seconds), 3)

    async def test_overlap_scenario_applies_all_operations_to_each_partition(self) -> None:
        context = _OperationContext()
        config = LoadConfig(
            auth_mode="connection-string",
            database="agents",
            container="load",
            scenarios=("clear-pop-overlap",),
            concurrency=2,
            requested_rate_per_second=8,
            duration_seconds=1,
            warmup_seconds=0,
            session_count=2,
        )
        prepared = await _prepare_workload(
            scenario="clear-pop-overlap",
            context=context,  # type: ignore[arg-type]
            config=config,
            duration_seconds=1,
        )

        for index in range(8):
            await prepared.operation(index)

        self.assertEqual(
            context.calls,
            [
                ("overlap-0", "add"),
                ("overlap-0", "pop"),
                ("overlap-0", "clear"),
                ("overlap-0", "add"),
                ("overlap-1", "add"),
                ("overlap-1", "pop"),
                ("overlap-1", "clear"),
                ("overlap-1", "add"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
