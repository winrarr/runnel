import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
