import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from pr_local import (  # noqa: E402
    BenchmarkOptions,
    BenchmarkTarget,
    LocalBenchmarkError,
    assess_stability,
    benchmark_command,
    benchmark_interpretation,
    reportable_result,
    require_stable,
    stamp_resource_limits,
)


class PrLocalTests(unittest.TestCase):
    def target(self) -> BenchmarkTarget:
        return BenchmarkTarget(
            name="pull-request",
            root=Path("/tmp/runnel"),
            target_dir=Path("/tmp/runnel-target"),
            output_dir=Path("/tmp/runnel-results"),
            log_dir=Path("/tmp/runnel-logs"),
        )

    def options(self, *, include_recovery: bool = False) -> BenchmarkOptions:
        return BenchmarkOptions(
            base_ref="origin/main",
            repetitions=None,
            min_repetitions=3,
            max_repetitions=7,
            max_throughput_range_percent=10.0,
            max_p99_range_percent=20.0,
            cpu_limit="2",
            memory_limit="2g",
            messages=1000,
            warmup=200,
            nodes=3,
            concurrency=2,
            payload_sizes="100,1024",
            include_recovery=include_recovery,
        )

    def test_command_is_a_three_node_comparison_workload(self) -> None:
        command = benchmark_command(
            self.target(),
            self.options(),
            Path("/tmp/current.json"),
            Path("/tmp/logs"),
            build=True,
        )

        self.assertIn("--build", command)
        self.assertIn("--nodes", command)
        self.assertIn("3", command)
        self.assertIn("--payload-sizes", command)
        self.assertIn("100,1024", command)
        self.assertIn("--skip-recovery", command)
        self.assertNotIn("--include-recovery", command)

    def test_recovery_is_explicitly_opt_in(self) -> None:
        command = benchmark_command(
            self.target(),
            self.options(include_recovery=True),
            Path("/tmp/current.json"),
            Path("/tmp/logs"),
            build=False,
        )

        self.assertNotIn("--build", command)
        self.assertNotIn("--skip-recovery", command)

    def test_report_removes_machine_specific_binary_paths(self) -> None:
        result = {
            "backends": {
                "runnel-cluster": {"image": "/home/user/project/target/runnel"}
            }
        }

        report_result = reportable_result(result)

        self.assertEqual(
            report_result["backends"]["runnel-cluster"]["image"],
            "local release binary",
        )
        self.assertEqual(result["backends"]["runnel-cluster"]["image"], "/home/user/project/target/runnel")

    def test_stability_requires_both_throughput_and_p99_ranges(self) -> None:
        result = {
            "backends": {
                "runnel-cluster": {
                    "scenarios": [
                        {
                            "repetition_summary": {
                                "throughput_messages_per_second": {"relative_range_percent": 5.0},
                                "latency_p99": {"relative_range_percent": 15.0},
                            }
                        }
                    ]
                }
            }
        }

        stability = assess_stability(result, result, self.options(), 3)

        self.assertEqual(stability["status"], "stable")

    def test_stability_reports_inconclusive_after_maximum(self) -> None:
        result = {
            "backends": {
                "runnel-cluster": {
                    "scenarios": [
                        {
                            "repetition_summary": {
                                "throughput_messages_per_second": {"relative_range_percent": 50.0},
                                "latency_p99": {"relative_range_percent": 50.0},
                            }
                        }
                    ]
                }
            }
        }

        stability = assess_stability(result, result, self.options(), 7)

        self.assertEqual(stability["status"], "inconclusive")
        self.assertIn("throughput range exceeded its limit", stability["inconclusive_reasons"])
        self.assertIn("p99 range exceeded its limit", stability["inconclusive_reasons"])

    def test_interpretation_reports_direction_and_outlier_sensitivity(self) -> None:
        def result(throughput: list[float], p99: list[float]) -> dict[str, object]:
            return {
                "backends": {
                    "runnel-cluster": {
                        "scenarios": [
                            {
                                "operation": "publish",
                                "messages": 1000,
                                "message_size_bytes": 100,
                                "repetition_summary": {
                                    "throughput_messages_per_second": {"samples": throughput},
                                    "latency_p99": {"samples": p99},
                                },
                            }
                        ]
                    }
                }
            }

        interpretation = benchmark_interpretation(
            result([110.0, 110.0, 110.0, 110.0, 1000.0], [90.0, 90.0, 90.0, 90.0, 90.0]),
            result([100.0] * 5, [100.0] * 5),
        )

        throughput = interpretation["throughput"]
        self.assertEqual(throughput["raw"]["direction"], "generally improved")
        self.assertEqual(throughput["raw"]["improved_scenarios"], 1)
        self.assertEqual(throughput["candidate_outlier_pairs"], 1)
        self.assertEqual(throughput["filtered"]["median_delta_percent"], 10.0)
        self.assertEqual(interpretation["p99"]["raw"]["direction"], "generally improved")

    def test_authoritative_run_rejects_inconclusive_result(self) -> None:
        with self.assertRaisesRegex(LocalBenchmarkError, "stable repeated measurements are required"):
            require_stable({"status": "inconclusive"}, allow_inconclusive=False)

    def test_diagnostic_override_allows_non_stable_result(self) -> None:
        require_stable({"status": "inconclusive"}, allow_inconclusive=True)

    def test_stamps_resource_limits_for_an_older_base_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            path.write_text(json.dumps({"resource_limits": {"processes": "host-scheduled"}}), encoding="utf-8")

            stamp_resource_limits(path, self.options())

            result = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(result["resource_limits"]["cpu"], "2")
        self.assertEqual(result["resource_limits"]["memory"], "2G")


if __name__ == "__main__":
    unittest.main()
