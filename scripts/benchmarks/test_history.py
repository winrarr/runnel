import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from build_history import build_points, load_runs, site_data  # noqa: E402
from aggregate import aggregate_results  # noqa: E402
from normalize import normalize_result  # noqa: E402
from resources import parse_cpu_stat, summarize_stats  # noqa: E402


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
                        "resource_samples": {
                            "samples": 2,
                            "cpu_seconds": 0.25,
                            "cpu_percent_max": 30.0,
                            "memory_bytes_max": 2048.0,
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
        self.assertEqual(
            normalized["backends"]["runnel"]["scenarios"][0]["resource_samples"]["cpu_seconds"],
            0.25,
        )

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
                "cpu_efficiency_messages_per_cpu_second",
                "cpu_percent_max",
                "memory_bytes_max",
            },
        )

    def test_aggregation_keeps_medians_and_observed_ranges(self) -> None:
        first = normalize_result(comparison_result(), source_name="first.json")
        second = copy.deepcopy(first)
        second["generated_at"] = "2026-08-20T08:58:56.024168+00:00"
        second["source_result"] = "second.json"
        second["backends"]["runnel"]["scenarios"][0][
            "throughput_messages_per_second"
        ] = 2000.0

        aggregated = aggregate_results([first, second])
        scenario = aggregated["backends"]["runnel"]["scenarios"][0]
        self.assertEqual(aggregated["aggregate"]["repetitions"], 2)
        self.assertEqual(scenario["repetitions"], 2)
        self.assertEqual(scenario["throughput_messages_per_second"], 1500.0)
        self.assertEqual(
            scenario["repetition_summary"]["throughput_messages_per_second"]["min"],
            1000.0,
        )
        self.assertAlmostEqual(
            scenario["repetition_summary"]["throughput_messages_per_second"][
                "relative_range_percent"
            ],
            66.6666666667,
        )
        self.assertEqual(
            scenario["repetition_summary"]["throughput_messages_per_second"]["samples"],
            [1000.0, 2000.0],
        )
        self.assertEqual(
            scenario["repetition_summary"]["throughput_messages_per_second"][
                "standard_deviation"
            ],
            500.0,
        )
        self.assertEqual(
            aggregated["repetition_runs"],
            [
                {
                    "run_id": first["source"]["run_id"],
                    "generated_at": "2026-08-20T08:57:56.024168+00:00",
                    "source_result": "first.json",
                },
                {
                    "run_id": second["source"]["run_id"],
                    "generated_at": "2026-08-20T08:58:56.024168+00:00",
                    "source_result": "second.json",
                },
            ],
        )

        generated = site_data(
            [{**aggregated, "_path": "aggregate.json"}]
        )
        current = next(
            point
            for point in generated["points"]
            if point["run_file"] == "aggregate.json"
            and point["metric"] == "throughput_messages_per_second"
        )
        self.assertAlmostEqual(current["range"]["relative_range_percent"], 66.6666666667)
        self.assertEqual(current["evidence"]["aggregation"], "median")
        self.assertEqual(current["evidence"]["sample_count"], 2)
        self.assertEqual(current["evidence"]["repetition_count"], 2)
        self.assertEqual(current["evidence"]["sample_coverage_percent"], 100.0)
        self.assertEqual(current["evidence"]["sample_values"], [1000.0, 2000.0])

    def test_legacy_aggregate_without_samples_remains_readable(self) -> None:
        first = normalize_result(comparison_result(), source_name="first.json")
        second = copy.deepcopy(first)
        second["generated_at"] = "2026-08-20T08:58:56.024168+00:00"
        legacy = aggregate_results([first, second])
        for backend in legacy["backends"].values():
            for metric_summary in backend.get("repetition_summary", {}).values():
                metric_summary.pop("samples", None)
                metric_summary.pop("standard_deviation", None)
                metric_summary.pop("relative_standard_deviation_percent", None)
            for scenario in backend.get("scenarios", []):
                for metric_summary in scenario.get("repetition_summary", {}).values():
                    metric_summary.pop("samples", None)
                    metric_summary.pop("standard_deviation", None)
                    metric_summary.pop("relative_standard_deviation_percent", None)
        legacy.pop("repetition_runs", None)

        generated = site_data([{**legacy, "_path": "legacy-aggregate.json"}])
        current = next(
            point
            for point in generated["points"]
            if point["metric"] == "throughput_messages_per_second"
        )
        self.assertEqual(current["range"]["min"], 1000.0)
        self.assertNotIn("sample_values", current["evidence"])
        self.assertEqual(current["evidence"]["sample_count"], 2)

    def test_site_data_compares_only_compatible_runs(self) -> None:
        first = normalize_result(comparison_result(), source_name="first.json")
        second = copy.deepcopy(first)
        second["generated_at"] = "2026-08-20T08:58:56.024168+00:00"
        second["backends"]["runnel"]["scenarios"][0][
            "throughput_messages_per_second"
        ] = 2000.0
        second["backends"]["runnel"]["image_id"] = "sha256:rebuilt"

        generated = site_data(
            [
                {**first, "_path": "first.json"},
                {**second, "_path": "second.json"},
            ]
        )
        current = next(
            point
            for point in generated["points"]
            if point["run_file"] == "second.json"
            and point["metric"] == "throughput_messages_per_second"
        )
        self.assertEqual(current["previous_value"], 1000.0)
        self.assertEqual(current["delta_percent"], 100.0)
        self.assertTrue(current["improved"])

    def test_runnel_history_series_separates_measurement_suites(self) -> None:
        raw = comparison_result()
        raw["workload"]["single_node"] = True
        first = normalize_result(raw, source_name="first.json")
        second = copy.deepcopy(first)
        second["benchmark_suite"] = "runnel"
        second["generated_at"] = "2026-08-20T08:58:56.024168+00:00"
        second["backends"]["runnel"]["scenarios"][0][
            "throughput_messages_per_second"
        ] = 2000.0

        points = build_points(
            [
                {**first, "_path": "first.json"},
                {**second, "_path": "second.json"},
            ]
        )

        runnel_points = [point for point in points if point["backend"] == "runnel"]
        self.assertEqual(
            {point["benchmark_series"] for point in runnel_points},
            {"runnel-native-comparison", "runnel"},
        )

        generated = site_data(
            [
                {**first, "_path": "first.json"},
                {**second, "_path": "second.json"},
            ]
        )
        current = next(
            point
            for point in generated["points"]
            if point["run_file"] == "second.json"
            and point["metric"] == "throughput_messages_per_second"
        )
        self.assertNotIn("previous_value", current)
        self.assertNotIn("delta_percent", current)

    def test_resource_helpers_report_cpu_time_and_averages(self) -> None:
        self.assertEqual(parse_cpu_stat("usage_usec 250000\nuser_usec 1\n"), 0.25)
        self.assertEqual(parse_cpu_stat("cpuacct.usage 500000000\n"), 0.5)
        self.assertIsNone(parse_cpu_stat("user_usec 1\n"))
        summary = summarize_stats(
            [
                {"cpu_percent": 10.0, "memory_bytes": 100.0, "memory_percent": 1.0},
                {"cpu_percent": 30.0, "memory_bytes": 200.0, "memory_percent": 2.0},
            ],
            cpu_seconds=0.25,
            elapsed_seconds=1.0,
        )
        self.assertEqual(summary["cpu_percent_avg"], 20.0)
        self.assertEqual(summary["memory_bytes_max"], 200.0)
        self.assertEqual(summary["cpu_seconds"], 0.25)

    def test_loader_accepts_raw_comparison_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runs_dir = Path(directory)
            (runs_dir / "run.json").write_text(json.dumps(comparison_result()), encoding="utf-8")
            runs = load_runs(runs_dir)
            generated = site_data(runs)

        self.assertEqual(len(runs), 1)
        self.assertEqual(len(generated["points"]), 9)

        site_dir = SCRIPT_DIR.parents[1] / "docs" / "benchmarks"
        self.assertIn("Runnel benchmark history", (site_dir / "index.html").read_text(encoding="utf-8"))
        app = (site_dir / "app.js").read_text(encoding="utf-8")
        self.assertIn("CPU efficiency", app)
        self.assertIn("runnel-native-comparison", app)
        self.assertIn("rollingMedian", app)
        self.assertIn("logTickValues", app)
        self.assertIn("commit ${revision}", app)
        self.assertIn("point.operation !== operation", app)
        self.assertIn("seriesKey", app)
        self.assertIn("loadData", app)


if __name__ == "__main__":
    unittest.main()
