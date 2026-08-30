import tempfile
import unittest
from pathlib import Path

from scripts.benchmarks2.api import (
    ActionResult,
    Endpoint,
    Limits,
    Measurement,
    RunResult,
    ScenarioResult,
    Workload,
)
from scripts.benchmarks2.core import aggregate, compare, measure, percentile, run_suite, summarize


class FakeRuntime:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.endpoint: Endpoint | None = None

    def start(self) -> Endpoint:
        self.started = True
        self.endpoint = Endpoint("fake", 1)
        return self.endpoint

    def restart(self) -> int:
        return 7

    def stop(self) -> None:
        self.stopped = True

    def sample(self) -> dict[str, int]:
        return {"memory_bytes": 12}


class FakeBackend:
    name = "fake"

    def __init__(self) -> None:
        self.runtime_instance = FakeRuntime()

    def runtime(self, limits: Limits, nodes: int) -> FakeRuntime:
        return self.runtime_instance

    def client_factory(self, runtime: FakeRuntime):
        return lambda: object()

    def scenarios(self):
        return {"messages": lambda runtime, clients, workload, payload: ActionResult((10, 20))}


def result(latencies: tuple[int, ...]) -> RunResult:
    workload = Workload(2, (100,))
    scenario = ScenarioResult("messages", 100, Measurement(100, ActionResult(latencies)))
    return RunResult("test", "fake", workload, (scenario,))


class Benchmark2Tests(unittest.TestCase):
    def test_workload_rejects_unbounded_values(self) -> None:
        with self.assertRaises(ValueError):
            Workload(0)
        with self.assertRaises(ValueError):
            Workload(1, (0,))

    def test_percentile_interpolates_and_rejects_invalid_percentage(self) -> None:
        self.assertEqual(percentile((10, 20, 30), 50), 20)
        self.assertEqual(percentile((), 99), 0)
        with self.assertRaises(ValueError):
            percentile((1,), 101)

    def test_measure_rejects_empty_or_negative_action(self) -> None:
        with self.assertRaises(ValueError):
            measure(lambda: ActionResult(()))
        with self.assertRaises(ValueError):
            measure(lambda: ActionResult((-1,)))

    def test_suite_owns_runtime_lifecycle_and_serializes_resources(self) -> None:
        backend = FakeBackend()
        run = run_suite(backend, Workload(2, (100,)), selected=("messages",))
        self.assertTrue(backend.runtime_instance.started)
        self.assertTrue(backend.runtime_instance.stopped)
        self.assertEqual(summarize(run.scenarios[0])["resource_samples"], {"memory_bytes": 12})

    def test_suite_rejects_unknown_scenarios(self) -> None:
        with self.assertRaises(ValueError):
            run_suite(FakeBackend(), Workload(1), selected=("missing",))
        with self.assertRaises(ValueError):
            run_suite(FakeBackend(), Workload(1), selected=())

    def test_aggregate_keeps_observations_and_compare_reports_delta(self) -> None:
        baseline, current = result((10, 10)), result((20, 20))
        aggregated = aggregate((baseline, current))
        self.assertEqual(aggregated.metadata["aggregate"]["messages:100"]["count"], 2)
        self.assertEqual(
            compare(current, baseline)["messages:100"]["throughput_messages_per_second"]["delta_percent"],
            0,
        )

    def test_result_output_parent_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "result.json"
            run_suite(FakeBackend(), Workload(1, (100,)), output=output)
            self.assertTrue(output.exists())


if __name__ == "__main__":
    unittest.main()
