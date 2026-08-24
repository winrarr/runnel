import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from pr_local import (  # noqa: E402
    BenchmarkOptions,
    BenchmarkTarget,
    benchmark_command,
    reportable_result,
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
            repetitions=3,
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


if __name__ == "__main__":
    unittest.main()
