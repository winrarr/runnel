import subprocess
import unittest
from typing import Any

from pr_local_until_stable import parse_args, run_until_stable


class PrLocalUntilStableTests(unittest.TestCase):
    def test_parse_separates_wrapper_and_benchmark_arguments(self) -> None:
        args, benchmark_args = parse_args(
            ["--", "--max-attempts", "4", "--", "--messages", "500", "--payload-sizes", "100"]
        )

        self.assertEqual(args.max_attempts, 4)
        self.assertEqual(benchmark_args, ["--messages", "500", "--payload-sizes", "100"])

    def test_retries_inconclusive_runs_until_stable(self) -> None:
        results = iter([2, 2, 0])
        commands: list[list[str]] = []

        def runner(command: list[str], **_: Any) -> subprocess.CompletedProcess[object]:
            commands.append(command)
            return subprocess.CompletedProcess(command, next(results))

        self.assertEqual(run_until_stable(["--messages", "500"], 3, runner), 0)
        self.assertEqual(len(commands), 3)
        self.assertEqual(commands[0][2:], ["--messages", "500"])

    def test_stops_on_non_inconclusive_failure(self) -> None:
        results = iter([1, 0])
        calls = 0

        def runner(command: list[str], **_: Any) -> subprocess.CompletedProcess[object]:
            nonlocal calls
            calls += 1
            return subprocess.CompletedProcess(command, next(results))

        self.assertEqual(run_until_stable([], 2, runner), 1)
        self.assertEqual(calls, 1)

    def test_returns_inconclusive_after_attempt_budget(self) -> None:
        results = iter([2, 2])

        def runner(command: list[str], **_: Any) -> subprocess.CompletedProcess[object]:
            return subprocess.CompletedProcess(command, next(results))

        self.assertEqual(run_until_stable([], 2, runner), 2)


if __name__ == "__main__":
    unittest.main()
