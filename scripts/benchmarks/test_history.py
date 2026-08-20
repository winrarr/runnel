import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from build_site import build_points, load_runs, render_html, site_data  # noqa: E402
from normalize import normalize_result  # noqa: E402


def comparison_result() -> dict:
    return {
        "schema_version": 1,
        "generated_at": "2026-08-20T08:57:56.024168+00:00",
        "comparison_mode": "test",
        "resource_limits": {"broker_cpu": "2", "broker_memory": "2g"},
        "workload": {"messages": 10, "payload_sizes_bytes": [100]},
        "backends": {
            "runnel": {
                "image": "runnel:test",
                "image_id": "sha256:test",
                "acknowledgement": "durable",
                "replication": "single node",
                "measurement_boundary": "test protocol",
                "measurement_client": "test client",
                "startup_seconds": 0.1,
                "resource_samples": {
                    "samples": 1,
                    "cpu_percent_max": 20.0,
                    "memory_bytes_max": 1024.0,
                },
                "scenarios": [
                    {
                        "operation": "publish",
                        "messages": 10,
                        "message_size_bytes": 100,
                        "throughput_messages_per_second": 1000.0,
                        "latency_microseconds": {
                            "p50": 10.0,
                            "p99": 20.0,
                            "p999": 30.0,
                        },
                    }
                ],
                "raw_tool_output": {"should": "not be retained"},
            }
        },
    }


class HistoryTests(unittest.TestCase):
    def test_normalization_removes_raw_output_and_keeps_measurements(self) -> None:
        normalized = normalize_result(comparison_result(), source_name="test.json")

        self.assertEqual(normalized["history_schema_version"], 1)
        self.assertNotIn("raw_tool_output", json.dumps(normalized))
        self.assertEqual(normalized["backends"]["runnel"]["scenarios"][0]["operation"], "publish")

    def test_site_points_include_latency_and_resources(self) -> None:
        normalized = normalize_result(comparison_result(), source_name="test.json")
        points = build_points([normalized])
        metrics = {point["metric"] for point in points}

        self.assertEqual(
            metrics,
            {
                "throughput_messages_per_second",
                "latency_p50",
                "latency_p99",
                "latency_p999",
                "cpu_percent_max",
                "memory_bytes_max",
            },
        )

    def test_loader_accepts_raw_comparison_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs_dir = Path(directory)
            (runs_dir / "run.json").write_text(json.dumps(comparison_result()), encoding="utf-8")
            runs = load_runs(runs_dir)
            rendered = render_html(site_data(runs))

        self.assertEqual(len(runs), 1)
        self.assertIn("Runnel benchmark history", rendered)
        self.assertIn("benchmark-data", rendered)


if __name__ == "__main__":
    unittest.main()
