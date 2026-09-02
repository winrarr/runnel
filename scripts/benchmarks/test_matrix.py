import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import matrix  # noqa: E402


class MatrixBenchmarkTests(unittest.TestCase):
    def parse(self, *arguments: str):
        with patch.object(sys, "argv", ["matrix.py", *arguments]):
            return matrix.parse_args()

    def test_matrix_expands_only_relevant_dimensions_per_scenario(self) -> None:
        args = self.parse(
            "--scenarios",
            "slow_consumer,parallel_grouped_consume_ack",
            "--payload-sizes",
            "100,1024",
            "--concurrency-values",
            "1,4",
            "--slow-consumer-delays-ms",
            "0,10",
            "--retained-message-values",
            "1025,2048",
            "--repetitions",
            "2",
            "--max-cases",
            "20",
        )

        cases = matrix.matrix_cases(args)

        self.assertEqual(len(cases), 16)
        self.assertEqual(
            len({matrix.case_id(index, case) for index, case in enumerate(cases, 1)}),
            len(cases),
        )
        slow_cases = [case for case in cases if case["scenario"] == "slow_consumer"]
        grouped_cases = [
            case for case in cases if case["scenario"] == "parallel_grouped_consume_ack"
        ]
        self.assertEqual({case["concurrency"] for case in slow_cases}, {1})
        self.assertEqual({case["slow_consumer_delay_ms"] for case in slow_cases}, {0, 10})
        self.assertEqual({case["concurrency"] for case in grouped_cases}, {1, 4})
        self.assertEqual({case["slow_consumer_delay_ms"] for case in grouped_cases}, {0})
        self.assertEqual({case["retained_messages"] for case in cases}, {1025})

    def test_case_command_records_explicit_workload_and_output_controls(self) -> None:
        args = self.parse(
            "--binary",
            "/tmp/runnel",
            "--image",
            "runnel:test",
            "--messages",
            "40",
            "--warmup",
            "4",
            "--nodes",
            "3",
            "--ack-timeout-ms",
            "1000",
            "--scenarios",
            "slow_consumer",
            "--slow-consumer-delays-ms",
            "25",
            "--payload-sizes",
            "100",
        )
        case = matrix.matrix_cases(args)[0]
        command = matrix.case_command(
            args,
            case,
            Path("/tmp/matrix/result.json"),
            Path("/tmp/matrix/logs"),
            build=True,
        )

        self.assertIn("--build", command)
        self.assertIn("--runtime", command)
        self.assertIn("process", command)
        self.assertIn("--messages", command)
        self.assertIn("40", command)
        self.assertIn("--slow-consumer-delay-ms", command)
        self.assertIn("25", command)
        self.assertIn("--output", command)
        self.assertIn("/tmp/matrix/result.json", command)
        self.assertIn("--log-dir", command)

    def test_run_matrix_keeps_each_case_and_builds_a_machine_readable_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "matrix.json"
            args = self.parse(
                "--build",
                "--scenarios",
                "durable_publish",
                "--payload-sizes",
                "100",
                "--repetitions",
                "2",
                "--output",
                str(output),
            )
            builds: list[bool] = []

            def executor(
                _args: object,
                case: dict[str, object],
                index: int,
                _artifacts_dir: Path,
                *,
                build: bool,
            ) -> dict[str, object]:
                builds.append(build)
                return {
                    **case,
                    "case_id": matrix.case_id(index, case),
                    "status": "complete",
                }

            result = matrix.run_matrix(args, executor=executor)
            envelope = json.loads(output.read_text(encoding="utf-8"))
            self.assertTrue(Path(envelope["matrix"]["artifacts_dir"]).is_dir())

        self.assertEqual(result, 0)
        self.assertEqual(builds, [True, False])
        self.assertEqual(envelope["status"], "complete")
        self.assertEqual(envelope["matrix"]["planned_cases"], 2)
        self.assertEqual(envelope["matrix"]["attempted_cases"], 2)
        self.assertEqual(envelope["matrix"]["completed_cases"], 2)
        self.assertEqual(len(envelope["cases"]), 2)

    def test_case_timeout_is_bounded_and_native_scope_is_explicit(self) -> None:
        args = self.parse(
            "--case-timeout-seconds",
            "12.5",
            "--native-resource-scope",
        )

        self.assertEqual(args.case_timeout_seconds, 12.5)
        self.assertTrue(args.native_resource_scope)

        with self.assertRaises(SystemExit):
            self.parse("--case-timeout-seconds", str(matrix.MAX_CASE_TIMEOUT_SECONDS + 1))
        with self.assertRaises(SystemExit):
            self.parse("--concurrency-values", "129")
        with self.assertRaises(SystemExit):
            self.parse("--failure-timeout-seconds", "300.1")

    def test_case_timeout_terminates_child_and_retains_runner_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse(
                "--scenarios",
                "durable_publish",
                "--payload-sizes",
                "100",
                "--case-timeout-seconds",
                "0.1",
            )
            case = matrix.matrix_cases(args)[0]
            with patch.object(
                matrix,
                "case_command",
                return_value=[sys.executable, "-c", "import time; time.sleep(10)"],
            ):
                record = matrix.run_case(args, case, 1, Path(directory), build=False)
            self.assertEqual(record["status"], "timed_out")
            self.assertTrue(Path(record["log_path"]).is_file())
            self.assertFalse(Path(record["result_path"]).exists())

    def test_keep_going_records_failed_and_successful_cases_separately(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "matrix.json"
            args = self.parse(
                "--keep-going",
                "--scenarios",
                "durable_publish,consume_ack",
                "--payload-sizes",
                "100",
                "--output",
                str(output),
            )

            def executor(
                _args: object,
                case: dict[str, object],
                index: int,
                _artifacts_dir: Path,
                *,
                build: bool,
            ) -> dict[str, object]:
                del build
                status = "failed" if index == 1 else "complete"
                return {**case, "case_id": matrix.case_id(index, case), "status": status}

            result = matrix.run_matrix(args, executor=executor)
            envelope = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(result, 1)
        self.assertEqual(envelope["status"], "failed")
        self.assertEqual(envelope["matrix"]["attempted_cases"], 2)
        self.assertEqual(envelope["matrix"]["completed_cases"], 1)
        self.assertEqual(envelope["matrix"]["failed_cases"], 1)


if __name__ == "__main__":
    unittest.main()
