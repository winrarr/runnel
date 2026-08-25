import shutil
import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import isolated  # noqa: E402


class IsolationRunnerTests(unittest.TestCase):
    def tearDown(self) -> None:
        for run in getattr(self, "runs", []):
            shutil.rmtree(run.runtime_dir, ignore_errors=True)
            shutil.rmtree(run.artifact_dir, ignore_errors=True)

    def new_run(self) -> isolated.Isolation:
        run = isolated.create_isolation()
        if not hasattr(self, "runs"):
            self.runs: list[isolated.Isolation] = []
        self.runs.append(run)
        return run

    def test_runs_get_distinct_build_temp_and_artifact_resources(self) -> None:
        first = self.new_run()
        second = self.new_run()

        self.assertNotEqual(first.run_id, second.run_id)
        self.assertNotEqual(first.runtime_dir, second.runtime_dir)
        self.assertNotEqual(first.target_dir, second.target_dir)
        self.assertNotEqual(first.temp_dir, second.temp_dir)
        self.assertNotEqual(first.artifact_dir, second.artifact_dir)
        self.assertNotEqual(first.image, second.image)

    def test_environment_points_supported_workflow_state_at_the_run(self) -> None:
        run = self.new_run()
        env = isolated.environment(run)

        self.assertEqual(env["CARGO_TARGET_DIR"], str(run.target_dir))
        self.assertEqual(env["TMPDIR"], str(run.temp_dir))
        self.assertEqual(env["RUNNEL_ISOLATION_ID"], run.run_id)
        self.assertEqual(env["RUNNEL_ISOLATION_ARTIFACTS"], str(run.artifact_dir))
        self.assertEqual(env["RUST_TEST_THREADS"], "1")

    def test_workflows_use_isolated_outputs_when_they_produce_them(self) -> None:
        run = self.new_run()

        for workflow in isolated.WORKFLOWS:
            command = isolated.command_for(workflow, run)
            self.assertTrue(command)
            if workflow.startswith("bench-") or workflow == "profile-cluster":
                self.assertIn(str(run.artifact_dir), " ".join(command))
        self.assertIn(str(run.target_dir), " ".join(isolated.command_for("bench-cluster", run)))
        self.assertIn(run.image, " ".join(isolated.command_for("bench-container", run)))
        self.assertNotIn("--all-features", isolated.command_for("test", run))
        self.assertIn("--test-threads=1", isolated.command_for("cluster-test", run))

    def test_benchmark_workflows_use_the_shared_lock_wrapper(self) -> None:
        run = self.new_run()
        command = isolated.lock_command(
            "bench-cluster-smoke", isolated.command_for("bench-cluster-smoke", run)
        )

        self.assertEqual(command[0], sys.executable)
        self.assertEqual(command[1].split("/")[-1], "lock.py")
        self.assertIn("--mode", command)
        self.assertIn("shared", command)

    def test_non_benchmark_workflows_are_not_locked(self) -> None:
        run = self.new_run()
        command = isolated.lock_command("test", isolated.command_for("test", run))

        self.assertEqual(command, isolated.command_for("test", run))


if __name__ == "__main__":
    unittest.main()
