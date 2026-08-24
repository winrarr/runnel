import copy
import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import pr_report  # noqa: E402


def container_result() -> dict:
    return {
        "schema_version": 1,
        "git_revision": "abc1234",
        "container": {
            "image": "runnel:pr",
            "cpu_limit": "2",
            "memory_limit": "1g",
            "resource_samples": {
                "cpu_percent_max": 42.0,
                "memory_bytes_max": 8 * 1024 * 1024,
            },
        },
        "workload": {
            "messages": 100,
            "warmup": 10,
            "concurrency": 2,
            "payload_sizes_bytes": [100],
        },
        "scenarios": [
            {
                "name": "durable_publish",
                "messages": 100,
                "message_size_bytes": 100,
                "throughput_messages_per_second": 800.0,
                "latency_microseconds": {"p50": 100.0, "p99": 250.0, "p999": 400.0},
                "resource_samples": {
                    "cpu_seconds": 0.5,
                    "memory_bytes_max": 4 * 1024 * 1024,
                },
            }
        ],
    }


class PrReportTests(unittest.TestCase):
    def test_formats_container_metrics_and_metadata(self) -> None:
        report = pr_report.render_report(container_result())

        self.assertIn("Revision: `abc1234`", report)
        self.assertIn("100 messages; payload 100 B; warmup 10; concurrency 2", report)
        self.assertIn("CPU `2`; memory `1g`", report)
        self.assertIn("durable_publish", report)
        self.assertIn("800 msg/s", report)
        self.assertIn("100 µs", report)
        self.assertIn("250 µs", report)
        self.assertIn("400 µs", report)
        self.assertIn("0.5 s", report)
        self.assertIn("4 MiB", report)
        self.assertNotIn("raw_tool_output", report)

    def test_missing_metrics_are_omitted_without_failing(self) -> None:
        result = container_result()
        result["scenarios"][0] = {
            "operation": "sparse",
            "messages": 1,
            "message_size_bytes": 1,
            "resource_samples": {"memory_bytes_avg": 1024},
        }

        report = pr_report.render_report(result)

        self.assertIn("| Operation | Messages | Size | Memory |", report)
        self.assertIn("sparse", report)
        self.assertIn("1 KiB", report)
        self.assertNotIn("Throughput", report)
        self.assertNotIn("| p50 |", report)
        self.assertNotIn("| CPU |", report)

    def test_baseline_delta_requires_matching_workload_identity(self) -> None:
        baseline = container_result()
        baseline["git_revision"] = "base123"
        baseline["scenarios"][0]["throughput_messages_per_second"] = 400.0

        report = pr_report.render_report(container_result(), baseline)

        self.assertIn("Baseline revision: `base123`", report)
        self.assertIn("Δ +100.0%", report)
        baseline_table, pull_request_table = report.split("### Pull request benchmark", maxsplit=1)
        self.assertIn("### Default branch benchmark", baseline_table)
        self.assertIn("400 msg/s", baseline_table)
        self.assertNotIn("Δ", baseline_table)
        self.assertIn("800 msg/s (Δ +100.0%)", pull_request_table)

        mismatched = copy.deepcopy(baseline)
        mismatched["workload"]["messages"] = 200
        mismatch_report = pr_report.render_report(container_result(), mismatched)
        self.assertIn("workload identity differs", mismatch_report)
        self.assertNotIn("Δ", mismatch_report)

    def test_baseline_delta_requires_matching_operation_and_message_size(self) -> None:
        baseline = container_result()
        baseline["scenarios"][0]["operation"] = "other_operation"
        baseline["scenarios"][0]["message_size_bytes"] = 1024

        report = pr_report.render_report(container_result(), baseline)

        self.assertIn("matching operation, message count, and message size", report)
        self.assertNotIn("Δ", report)

    def test_accepts_clustered_runnel_shape(self) -> None:
        result = {
            "git_revision": "cluster1",
            "resource_limits": {"processes": "host-scheduled"},
            "workload": {"messages": 20, "nodes": 3, "payload_sizes_bytes": [100]},
            "backends": {
                "runnel-cluster": {
                    "image": "target/release/runnel",
                    "resource_samples": {"memory_bytes_max": 16 * 1024 * 1024},
                    "scenarios": [
                        {
                            "operation": "cluster_publish",
                            "messages": 20,
                            "message_size_bytes": 100,
                            "throughput_messages_per_second": 200.0,
                            "latency_microseconds": {"p50": 50.0},
                            "resource_samples": {"cpu_seconds": 1.0},
                        }
                    ],
                }
            },
        }

        report = pr_report.render_report(result, heading="Clustered Runnel benchmark (primary)")

        self.assertIn("## Clustered Runnel benchmark (primary)", report)
        self.assertIn("nodes 3", report)
        self.assertIn("cluster_publish", report)
        self.assertIn("200 msg/s", report)
        self.assertIn("50 µs", report)
        self.assertIn("1 s", report)
        self.assertIn("16 MiB", report)

        baseline = copy.deepcopy(result)
        baseline["git_revision"] = "base123"
        baseline["backends"]["runnel-cluster"]["scenarios"][0][
            "throughput_messages_per_second"
        ] = 100.0
        report_with_baseline = pr_report.render_report(result, baseline)
        self.assertIn("Default branch benchmark", report_with_baseline)
        self.assertIn("200 msg/s (Δ +100.0%)", report_with_baseline)


if __name__ == "__main__":
    unittest.main()
