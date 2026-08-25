import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("lock.py")


class BenchmarkLockTests(unittest.TestCase):
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
