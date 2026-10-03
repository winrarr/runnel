import copy
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import product_fit


class ProductFitHarnessTests(unittest.TestCase):
    def test_default_manifest_declares_both_reference_workloads(self):
        manifest = product_fit.load_manifest(product_fit.DEFAULT_MANIFEST)

        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(
            set(manifest["workloads"]),
            {"background_work", "events_replay"},
        )
        for workload in manifest["workloads"].values():
            self.assertGreater(workload["messages"], 0)
            self.assertGreater(workload["payload_bytes"], 0)
            self.assertTrue(workload["budgets"])
        self.assertEqual(
            manifest["workloads"]["background_work"]["registered_observations"][
                "in_flight_deliveries_while_two_held"
            ]["expected_metric_value"],
            2,
        )

    def test_manifest_rejects_nonpositive_budget(self):
        manifest = product_fit.load_manifest(product_fit.DEFAULT_MANIFEST)
        manifest = copy.deepcopy(manifest)
        manifest["workloads"]["background_work"]["budgets"]["publish_p95_ms"] = 0

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(product_fit.ProductFitError):
                product_fit.load_manifest(path)

    def test_percentile_interpolates_between_samples(self):
        self.assertEqual(product_fit.percentile([], 95), 0.0)
        self.assertEqual(product_fit.percentile([1, 2, 3, 4], 0), 1)
        self.assertEqual(product_fit.percentile([1, 2, 3, 4], 100), 4)
        self.assertEqual(product_fit.percentile([1, 2, 3, 4], 50), 2.5)

    def test_metrics_report_does_not_hide_restart_counter_reset(self):
        initial = {"requests": 0.0}
        before_restart = {"requests": 4.0}
        after_restart = {"requests": 1.0}

        report = product_fit.metrics_report(initial, before_restart, after_restart)

        self.assertTrue(report["available"])
        self.assertEqual(
            [snapshot["phase"] for snapshot in report["snapshots"]],
            ["initial", "before_restart", "after_restart"],
        )
        self.assertEqual(report["same_process_delta"]["delta"]["requests"], 4.0)
        self.assertEqual(report["restart_counter_delta"]["delta"]["requests"], -3.0)

    def test_budget_checks_are_explicit(self):
        self.assertEqual(product_fit.check_upper(2, 3)["status"], "pass")
        self.assertEqual(product_fit.check_upper(4, 3)["status"], "fail")
        self.assertEqual(product_fit.check_lower(3, 2)["status"], "pass")
        self.assertEqual(product_fit.check_lower(1, 2)["status"], "fail")

    def test_exact_observation_check_fails_for_missing_or_unexpected_metric(self):
        self.assertEqual(product_fit.check_exact(2.0, 2)["status"], "pass")
        self.assertEqual(product_fit.check_exact(1.0, 2)["status"], "fail")
        self.assertEqual(product_fit.check_exact(None, 2)["status"], "fail")

    def test_in_flight_observation_requires_two_distinct_held_offsets(self):
        registration = {
            "metric": "runnel_in_flight_deliveries",
            "held_delivery_count": 2,
            "expected_metric_value": 2,
            "scope": "Two held deliveries in this reference scenario only.",
        }

        result = product_fit.in_flight_observation(
            {"runnel_in_flight_deliveries": 2.0}, registration, [4, 5], 3.0, 100
        )
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["observed_metric_value"], 2.0)

        duplicate = product_fit.in_flight_observation(
            {"runnel_in_flight_deliveries": 2.0}, registration, [4, 4], 3.0, 100
        )
        self.assertEqual(duplicate["status"], "fail")

        expired = product_fit.in_flight_observation(
            {"runnel_in_flight_deliveries": 2.0}, registration, [4, 5], 101.0, 100
        )
        self.assertEqual(expired["status"], "fail")

    def test_failed_registered_observation_fails_workload_status(self):
        result = product_fit.build_workload_result(
            name="background_work",
            workload={
                "messages": 1,
                "budgets": {
                    "publish_p95_ms": 10,
                    "publish_p99_ms": 10,
                    "throughput_min_messages_per_second": 1,
                    "rss_peak_bytes": 100,
                    "disk_growth_bytes": 100,
                    "recovery_seconds": 1,
                },
            },
            resources={"rss_peak_bytes": 1, "storage_growth_bytes": 1},
            metrics={},
            latencies={"publish": [1], "poll": [1], "ack": [1]},
            recovery_seconds=0,
            ledger=[],
            broker=SimpleNamespace(readiness=[], exit_codes=[]),
            started=time.perf_counter_ns(),
            extra={},
            registered_observations={"in_flight": {"status": "fail"}},
        )
        self.assertEqual(result["status"], "fail")

    def test_manifest_rejects_unmatched_in_flight_expectation(self):
        manifest = copy.deepcopy(product_fit.load_manifest(product_fit.DEFAULT_MANIFEST))
        manifest["workloads"]["background_work"]["registered_observations"][
            "in_flight_deliveries_while_two_held"
        ]["expected_metric_value"] = 1

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(product_fit.ProductFitError):
                product_fit.load_manifest(path)


if __name__ == "__main__":
    unittest.main()
