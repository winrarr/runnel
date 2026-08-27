import argparse
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import run  # noqa: E402


class RunBenchmarkArgumentTests(unittest.TestCase):
    def test_parse_scenarios_accepts_named_workload_subset(self) -> None:
        self.assertEqual(
            run.parse_scenarios("durable_publish, consume_ack"),
            ["durable_publish", "consume_ack"],
        )

    def test_parse_scenarios_rejects_unknown_or_duplicate_names(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            run.parse_scenarios("durable_publish,unknown")
        with self.assertRaises(argparse.ArgumentTypeError):
            run.parse_scenarios("consume_ack,consume_ack")

    def test_parse_args_defaults_to_the_complete_workload(self) -> None:
        with patch.object(sys, "argv", ["run.py"]):
            args = run.parse_args()

        self.assertEqual(args.scenarios, list(run.SCENARIO_NAMES))

    def test_parse_args_can_select_a_workload_subset(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["run.py", "--scenarios", "durable_publish,consume_ack"],
        ):
            args = run.parse_args()

        self.assertEqual(args.scenarios, ["durable_publish", "consume_ack"])


if __name__ == "__main__":
    unittest.main()
