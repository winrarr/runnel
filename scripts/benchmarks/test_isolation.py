import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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

    def test_environment_can_reuse_a_caller_supplied_target(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"CARGO_TARGET_DIR": "/tmp/shared-runnel-target"}):
            env = isolated.environment(run)

        self.assertEqual(env["CARGO_TARGET_DIR"], "/tmp/shared-runnel-target")

    def test_binary_workflows_use_a_caller_supplied_target(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"CARGO_TARGET_DIR": "/tmp/shared-runnel-target"}):
            command = isolated.command_for("bench-cluster", run)

        self.assertIn(
            "/tmp/shared-runnel-target/release/runnel",
            " ".join(command),
        )

    def test_integration_cluster_container_smoke_reuses_prebuilt_image(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"RUNNEL_INTEGRATION_IMAGE_READY": "1"}):
            command = isolated.command_for("bench-cluster-container-smoke", run)

        command_text = " ".join(command)
        self.assertIn("--image runnel:dev", command_text)
        self.assertNotIn("--build", command)

    def test_standalone_cluster_container_smoke_builds_isolated_image(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"RUNNEL_INTEGRATION_IMAGE_READY": "0"}):
            command = isolated.command_for("bench-cluster-container-smoke", run)

        self.assertIn(run.image, " ".join(command))
        self.assertIn("--build", command)

    def test_peer_forwarding_smoke_is_an_isolated_multi_stream_process_run(self) -> None:
        run = self.new_run()
        command = isolated.command_for("bench-cluster-peer-forwarding-smoke", run)

        command_text = " ".join(command)
        self.assertIn("--scenarios peer_forwarding", command_text)
        self.assertIn("--peer-forwarding-stream-count 2", command_text)
        self.assertIn("--peer-forwarding-concurrency 8", command_text)
        self.assertIn("--messages 20", command_text)
        self.assertIn("--warmup 2", command_text)
        self.assertIn(str(run.artifact_dir), command_text)

    def test_peer_forwarding_container_smoke_reuses_the_integration_image(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"RUNNEL_INTEGRATION_IMAGE_READY": "1"}):
            command = isolated.command_for(
                "bench-cluster-peer-forwarding-container-smoke", run
            )

        command_text = " ".join(command)
        self.assertIn("--runtime container", command_text)
        self.assertIn("--image runnel:dev", command_text)
        self.assertIn("--scenarios peer_forwarding", command_text)
        self.assertIn("--peer-forwarding-stream-count 2", command_text)
        self.assertIn("--peer-forwarding-concurrency 8", command_text)
        self.assertIn("--messages 20", command_text)
        self.assertIn("--warmup 2", command_text)
        self.assertIn(str(run.artifact_dir / "cluster-container.json"), command_text)
        self.assertNotIn("--build", command)

    def test_peer_forwarding_container_smoke_builds_an_isolated_image_standalone(self) -> None:
        run = self.new_run()
        with patch.dict(os.environ, {"RUNNEL_INTEGRATION_IMAGE_READY": "0"}):
            command = isolated.command_for(
                "bench-cluster-peer-forwarding-container-smoke", run
            )

        self.assertIn(run.image, " ".join(command))
        self.assertIn("--build", command)

    def test_peer_forwarding_container_census_gate_accepts_direct_counts_at_both_boundaries(self) -> None:
        artifact = self.peer_forwarding_container_artifact()
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIsNone(error)

    def test_peer_forwarding_container_census_gate_reports_malformed_json_shapes(self) -> None:
        for contents, expected in (
            ("[]", "root must be a JSON object"),
            ('{"backends": []}', "no backend object"),
            (
                '{"backends": {"runnel-cluster": {"runtime": "container", "scenarios": [null]}}}',
                "no peer_forwarding scenario",
            ),
            (
                json.dumps(
                    {
                        "backends": {
                            "runnel-cluster": {
                                "runtime": "container",
                                "scenarios": [
                                    {
                                        "operation": "cluster_peer_forwarding",
                                        "metadata": {
                                            "peer_connection_census": {
                                                "sample_boundaries": [{}]
                                            }
                                        },
                                    }
                                ],
                            }
                        }
                    }
                ),
                "invalid sample boundaries",
            ),
        ):
            with self.subTest(contents=contents), tempfile.TemporaryDirectory() as directory:
                artifact_path = Path(directory) / "cluster-container.json"
                artifact_path.write_text(contents, encoding="utf-8")

                error = isolated.peer_forwarding_container_census_error(artifact_path)

            self.assertIn(expected, error or "")

    def test_peer_forwarding_container_census_gate_rejects_missing_or_unavailable_boundaries(self) -> None:
        artifact = self.peer_forwarding_container_artifact()
        census = artifact["backends"]["runnel-cluster"]["scenarios"][0]["metadata"]["peer_connection_census"]
        census["sample_boundaries"].remove("after_measured_forwarding")

        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("missing a settled sample boundary", error or "")

        census["sample_boundaries"].append("after_measured_forwarding")
        census["after_measured_forwarding"]["available"] = False
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("unavailable at after_measured_forwarding", error or "")

    def test_peer_forwarding_container_census_gate_rejects_missing_or_unavailable_nodes(self) -> None:
        artifact = self.peer_forwarding_container_artifact()
        census = artifact["backends"]["runnel-cluster"]["scenarios"][0]["metadata"]["peer_connection_census"]
        census["after_setup_warmup"]["per_node"].pop("3")

        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("must contain nodes 1, 2, and 3", error or "")

        census["after_setup_warmup"]["per_node"]["3"] = {
            "available": False,
            "established_socket_endpoint_count": 0,
        }
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("unavailable for node 3", error or "")

    def test_peer_forwarding_container_census_gate_rejects_invalid_counts_and_unverified_sources(self) -> None:
        artifact = self.peer_forwarding_container_artifact()
        census = artifact["backends"]["runnel-cluster"]["scenarios"][0]["metadata"]["peer_connection_census"]
        node_sample = census["after_setup_warmup"]["per_node"]["2"]
        node_sample["established_socket_endpoint_count"] = -1

        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("invalid count for node 2", error or "")

        node_sample["established_socket_endpoint_count"] = 0
        node_sample["observation_source"] = "configured_group_count"
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("no direct procfs source for node 2", error or "")

        node_sample["observation_source"] = "docker_exec_container_procfs_fd_inode_join"
        node_sample["container_process_identity_verified"] = False
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "cluster-container.json"
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            error = isolated.peer_forwarding_container_census_error(artifact_path)

        self.assertIn("did not verify the broker identity for node 2", error or "")

    def test_container_peer_forwarding_smoke_fails_when_artifact_gate_rejects_output(self) -> None:
        run = self.new_run()

        def write_incomplete_artifact(*_args: object, **_kwargs: object) -> object:
            (run.artifact_dir / "cluster-container.json").write_text(
                "{}", encoding="utf-8"
            )
            return isolated.subprocess.CompletedProcess(["benchmark"], 0)

        with (
            patch.object(isolated, "create_isolation", return_value=run),
            patch.object(isolated, "environment", return_value={}),
            patch.object(isolated, "command_for", return_value=["benchmark"]),
            patch.object(
                isolated,
                "lock_command",
                side_effect=lambda _workflow, command: command,
            ),
            patch.object(
                isolated.subprocess,
                "run",
                side_effect=write_incomplete_artifact,
            ),
        ):
            status = isolated.run(
                "bench-cluster-peer-forwarding-container-smoke", keep=False
            )

        self.assertEqual(status, 1)

    @staticmethod
    def peer_forwarding_container_artifact() -> dict[str, object]:
        boundaries = {}
        for boundary in isolated.PEER_FORWARDING_CENSUS_BOUNDARIES:
            boundaries[boundary] = {
                "available": True,
                "per_node": {
                    node_id: {
                        "available": True,
                        "established_socket_endpoint_count": 0,
                        "observation_source": "docker_exec_container_procfs_fd_inode_join",
                        "docker_host_pid_resolved": True,
                        "container_process_identity_verified": True,
                    }
                    for node_id in isolated.CLUSTER_NODE_IDS
                },
            }
        census = {
            "all_samples_available": True,
            "sample_boundaries": list(isolated.PEER_FORWARDING_CENSUS_BOUNDARIES),
            **boundaries,
        }
        return {
            "backends": {
                "runnel-cluster": {
                    "runtime": "container",
                    "scenarios": [
                        {
                            "operation": "cluster_peer_forwarding",
                            "metadata": {"peer_connection_census": census},
                        }
                    ],
                }
            }
        }

    def test_workflows_use_isolated_outputs_when_they_produce_them(self) -> None:
        run = self.new_run()

        for workflow in isolated.WORKFLOWS:
            command = isolated.command_for(workflow, run)
            self.assertTrue(command)
            if workflow.startswith("bench-") or workflow == "profile-cluster":
                self.assertIn(str(run.artifact_dir), " ".join(command))
        self.assertIn(str(run.target_dir), " ".join(isolated.command_for("bench-cluster", run)))
        self.assertIn(run.image, " ".join(isolated.command_for("bench-container", run)))
        container_cluster = " ".join(
            isolated.command_for("bench-cluster-container", run)
        )
        self.assertIn(run.image, container_cluster)
        self.assertIn("--runtime container", container_cluster)
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

        container_forwarding = isolated.lock_command(
            "bench-cluster-peer-forwarding-container-smoke",
            isolated.command_for(
                "bench-cluster-peer-forwarding-container-smoke", run
            ),
        )
        self.assertEqual(container_forwarding[0], sys.executable)
        self.assertIn("shared", container_forwarding)

    def test_non_benchmark_workflows_are_not_locked(self) -> None:
        run = self.new_run()
        command = isolated.lock_command("test", isolated.command_for("test", run))

        self.assertEqual(command, isolated.command_for("test", run))


if __name__ == "__main__":
    unittest.main()
