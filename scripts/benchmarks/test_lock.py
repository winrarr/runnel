import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from lock import lock_command


SCRIPT = Path(__file__).with_name("lock.py")


class BenchmarkLockTests(unittest.TestCase):
    def test_full_benchmarks_are_exclusive_and_smoke_benchmarks_are_shared(self) -> None:
        def mode_for(workflow: str) -> str:
            command = lock_command(workflow, ["benchmark"])
            return command[command.index("--mode") + 1]

        self.assertEqual(mode_for("bench"), "exclusive")
        self.assertEqual(mode_for("profile-cluster-instrumented"), "exclusive")
        self.assertEqual(mode_for("bench-compare-cluster"), "exclusive")
        self.assertEqual(mode_for("bench-container-smoke"), "shared")
        self.assertEqual(mode_for("bench-cluster-smoke"), "shared")
        self.assertEqual(mode_for("bench-cluster-matrix"), "exclusive")
        self.assertEqual(mode_for("bench-cluster-matrix-smoke"), "shared")
        self.assertEqual(mode_for("bench-cluster-container"), "exclusive")
        self.assertEqual(mode_for("bench-cluster-container-smoke"), "shared")

    def test_shared_commands_can_run(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--mode", "shared", "--", sys.executable, "-c", "pass"],
            check=False,
        )

        self.assertEqual(result.returncode, 0)

    def test_non_waiting_command_reports_a_busy_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "benchmark.lock"
            holder = subprocess.Popen(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--mode",
                    "exclusive",
                    "--path",
                    str(lock_path),
                    "--",
                    sys.executable,
                    "-c",
                    "import time; print('ready', flush=True); time.sleep(0.5)",
                ],
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(holder.stdout.readline().strip(), "ready")
                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPT),
                        "--mode",
                        "shared",
                        "--path",
                        str(lock_path),
                        "--no-wait",
                        "--",
                        sys.executable,
                        "-c",
                        "pass",
                    ],
                    check=False,
                )
                self.assertEqual(result.returncode, 2)
            finally:
                holder.wait(timeout=2)
                holder.stdout.close()


if __name__ == "__main__":
    unittest.main()
