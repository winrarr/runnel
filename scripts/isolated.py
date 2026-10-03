#!/usr/bin/env python3
"""Run a supported development workflow with isolated local resources.

Each invocation receives a unique temporary directory, Cargo target directory,
temporary-file directory, and benchmark artifact directory. Workflows that
build or run Docker containers also receive a unique image tag when they build
an image; the container benchmark itself creates a private Docker network.

This intentionally exposes named workflows instead of pretending that an
arbitrary command can be made safe: commands which bind fixed ports or use
untracked external state still need their own isolation design.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from benchmarks.lock import lock_command


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKFLOW = "test"
INTEGRATION_IMAGE = "runnel:dev"
CONTAINER_PEER_FORWARDING_CENSUS_SOURCES = {
    "host_pid_procfs_fd_inode_join",
    "docker_exec_container_procfs_fd_inode_join",
}
PEER_FORWARDING_CENSUS_BOUNDARIES = (
    "after_setup_warmup",
    "after_measured_forwarding",
)
CLUSTER_NODE_IDS = {"1", "2", "3"}
WORKFLOWS = (
    "test",
    "smoke",
    "cluster-test",
    "cluster-replacement-test",
    "bench",
    "bench-container",
    "bench-container-smoke",
    "bench-cluster",
    "bench-cluster-smoke",
    "bench-cluster-peer-forwarding-smoke",
    "bench-cluster-peer-forwarding-container-smoke",
    "bench-cluster-matrix-smoke",
    "bench-cluster-container",
    "bench-cluster-container-smoke",
    "profile-cluster",
    "bench-compare",
)


@dataclass(frozen=True)
class Isolation:
    run_id: str
    runtime_dir: Path
    artifact_dir: Path
    target_dir: Path
    temp_dir: Path
    image: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="run a supported Runnel workflow with isolated local resources"
    )
    parser.add_argument(
        "workflow",
        nargs="?",
        choices=WORKFLOWS,
        default=DEFAULT_WORKFLOW,
        help="workflow to run (default: test)",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep temporary build and test state after the workflow finishes",
    )
    return parser.parse_args()


def create_isolation() -> Isolation:
    run_id = uuid.uuid4().hex[:16]
    runtime_dir = Path(tempfile.mkdtemp(prefix=f"runnel-isolated-{run_id}-"))
    target_dir = runtime_dir / "target"
    temp_dir = runtime_dir / "tmp"
    temp_dir.mkdir()
    artifact_dir = ROOT / "benchmark-results" / "isolated" / run_id
    artifact_dir.mkdir(parents=True, exist_ok=False)
    return Isolation(
        run_id=run_id,
        runtime_dir=runtime_dir,
        artifact_dir=artifact_dir,
        target_dir=target_dir,
        temp_dir=temp_dir,
        image=f"runnel:isolated-{run_id}",
    )


def environment(isolation: Isolation) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            # A sequential caller such as CI may provide a shared target to
            # reuse compilation across isolated workflows. The default stays
            # unique so independent invocations remain safe to run together.
            "CARGO_TARGET_DIR": os.environ.get(
                "CARGO_TARGET_DIR", str(isolation.target_dir)
            ),
            "TMPDIR": str(isolation.temp_dir),
            "TEMP": str(isolation.temp_dir),
            "TMP": str(isolation.temp_dir),
            "RUNNEL_ISOLATION_ID": isolation.run_id,
            "RUNNEL_ISOLATION_DIR": str(isolation.runtime_dir),
            "RUNNEL_ISOLATION_ARTIFACTS": str(isolation.artifact_dir),
            # The long-running integration suite owns several broker
            # processes; serializing test cases avoids test-level contention
            # while separate invocations remain independent.
            "RUST_TEST_THREADS": "1",
        }
    )
    return env


def cluster_command(
    isolation: Isolation,
    *,
    runtime: str,
    smoke: bool = False,
    peer_forwarding_smoke: bool = False,
) -> list[str]:
    """Build the native or container clustered benchmark command."""
    artifact = isolation.artifact_dir
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", str(isolation.target_dir)))
    command = ["python3", "scripts/benchmarks/cluster.py"]
    if runtime == "process":
        command.extend(
            [
                "--build",
                "--binary",
                str(target_dir / "release" / "runnel"),
                "--output",
                str(artifact / "cluster.json"),
                "--log-dir",
                str(artifact / "cluster-logs"),
            ]
        )
    elif runtime == "container":
        integration_image_ready = (
            os.environ.get("RUNNEL_INTEGRATION_IMAGE_READY") == "1"
        )
        command.extend(
            [
                "--runtime",
                "container",
                "--image",
                INTEGRATION_IMAGE if integration_image_ready else isolation.image,
            ]
        )
        if not integration_image_ready:
            command.append("--build")
        command.extend(
            [
                "--output",
                str(artifact / "cluster-container.json"),
            ]
        )
    else:
        raise ValueError(f"unsupported cluster runtime: {runtime}")
    if smoke:
        command.extend(
            [
                "--messages",
                "20",
                "--warmup",
                "2",
                "--payload-sizes",
                "100",
                "--skip-recovery",
            ]
        )
    if peer_forwarding_smoke:
        command.extend(
            [
                "--scenarios",
                "peer_forwarding",
                "--peer-forwarding-stream-count",
                "2",
                "--peer-forwarding-concurrency",
                "8",
                "--peer-forwarding-timeout-seconds",
                "30",
            ]
        )
    return command


def command_for(workflow: str, isolation: Isolation) -> list[str]:
    artifact = isolation.artifact_dir
    if workflow == "test":
        return ["cargo", "test", "--locked", "--workspace", "--all-targets"]
    if workflow == "smoke":
        return ["./scripts/smoke.sh"]
    if workflow in {"cluster-test", "cluster-replacement-test"}:
        recovery_args = (
            ["--features", "test-replacement-recovery"]
            if workflow == "cluster-replacement-test"
            else []
        )
        return [
            "cargo",
            "test",
            "--locked",
            "-p",
            "runnel-server",
            *recovery_args,
            "--test",
            "cluster_smoke",
            "--",
            "--nocapture",
            "--test-threads=1",
        ]
    if workflow == "bench":
        return ["cargo", "bench", "--locked", "--workspace"]
    if workflow == "bench-container":
        return [
            "python3",
            "scripts/benchmarks/run.py",
            "--build",
            "--image",
            isolation.image,
            "--output",
            str(artifact / "container.json"),
        ]
    if workflow == "bench-container-smoke":
        return [
            "python3",
            "scripts/benchmarks/run.py",
            "--image",
            "runnel:dev",
            "--messages",
            "20",
            "--warmup",
            "2",
            "--concurrency",
            "2",
            "--payload-sizes",
            "100",
            "--output",
            str(artifact / "container.json"),
        ]
    if workflow == "bench-cluster":
        return cluster_command(isolation, runtime="process")
    if workflow == "bench-cluster-smoke":
        return cluster_command(isolation, runtime="process", smoke=True)
    if workflow == "bench-cluster-peer-forwarding-smoke":
        return cluster_command(
            isolation,
            runtime="process",
            smoke=True,
            peer_forwarding_smoke=True,
        )
    if workflow == "bench-cluster-peer-forwarding-container-smoke":
        return cluster_command(
            isolation,
            runtime="container",
            smoke=True,
            peer_forwarding_smoke=True,
        )
    if workflow == "bench-cluster-matrix-smoke":
        target_dir = Path(os.environ.get("CARGO_TARGET_DIR", str(isolation.target_dir)))
        return [
            "python3",
            "scripts/benchmarks/matrix.py",
            "--build",
            "--binary",
            str(target_dir / "release" / "runnel"),
            "--messages",
            "10",
            "--warmup",
            "2",
            "--payload-sizes",
            "100",
            "--scenarios",
            "durable_publish,slow_consumer,leader_failure_recovery,follower_failure_recovery",
            "--slow-consumer-delays-ms",
            "1",
            "--output",
            str(isolation.artifact_dir / "cluster-matrix.json"),
            "--artifacts-dir",
            str(isolation.artifact_dir / "cluster-matrix-cases"),
        ]
    if workflow == "bench-cluster-container":
        return cluster_command(isolation, runtime="container")
    if workflow == "bench-cluster-container-smoke":
        return cluster_command(isolation, runtime="container", smoke=True)
    if workflow == "profile-cluster":
        target_dir = Path(os.environ.get("CARGO_TARGET_DIR", str(isolation.target_dir)))
        binary = target_dir / "release" / "runnel"
        return [
            "python3",
            "scripts/benchmarks/profile.py",
            "--build",
            "--binary",
            str(binary),
            "--output",
            str(artifact / "profile"),
        ]
    if workflow == "bench-compare":
        return [
            "python3",
            "scripts/benchmarks/compare.py",
            "--build-runnel",
            "--runnel-image",
            isolation.image,
            "--output",
            str(artifact / "compare.json"),
        ]
    raise ValueError(f"unsupported workflow: {workflow}")


def peer_forwarding_container_census_error(artifact_path: Path) -> str | None:
    """Require direct socket counts for both settled container boundaries."""
    try:
        result = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return f"could not read benchmark artifact: {error}"

    if not isinstance(result, dict):
        return "benchmark artifact root must be a JSON object"
    backends = result.get("backends")
    if not isinstance(backends, dict):
        return "benchmark artifact has no backend object"
    backend = backends.get("runnel-cluster")
    if not isinstance(backend, dict):
        return "benchmark artifact has no clustered Runnel backend"
    if backend.get("runtime") != "container":
        return "benchmark artifact is not from the container runtime"
    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list):
        return "container benchmark artifact has no scenario list"
    scenario = next(
        (
            item
            for item in scenarios
            if isinstance(item, dict)
            and item.get("operation") == "cluster_peer_forwarding"
        ),
        None,
    )
    if scenario is None:
        return "benchmark artifact has no peer_forwarding scenario"
    metadata = scenario.get("metadata")
    if not isinstance(metadata, dict):
        return "peer_forwarding artifact has no scenario metadata object"
    census = metadata.get("peer_connection_census")
    if not isinstance(census, dict):
        return "peer_forwarding artifact has no socket census"
    sample_boundaries = census.get("sample_boundaries")
    if not isinstance(sample_boundaries, list) or any(
        not isinstance(boundary, str) for boundary in sample_boundaries
    ):
        return "peer_forwarding census has invalid sample boundaries"
    boundaries = set(sample_boundaries)
    if not set(PEER_FORWARDING_CENSUS_BOUNDARIES).issubset(boundaries):
        return "peer_forwarding census is missing a settled sample boundary"
    if census.get("all_samples_available") is not True:
        return "peer_forwarding census reports unavailable samples"

    for boundary in PEER_FORWARDING_CENSUS_BOUNDARIES:
        sample = census.get(boundary)
        if not isinstance(sample, dict) or sample.get("available") is not True:
            return f"peer_forwarding census is unavailable at {boundary}"
        per_node = sample.get("per_node")
        if not isinstance(per_node, dict) or set(per_node) != CLUSTER_NODE_IDS:
            return f"peer_forwarding census at {boundary} must contain nodes 1, 2, and 3"
        for node_id, node_sample in per_node.items():
            if not isinstance(node_sample, dict) or node_sample.get("available") is not True:
                return f"peer_forwarding census is unavailable for node {node_id} at {boundary}"
            count = node_sample.get("established_socket_endpoint_count")
            if type(count) is not int or count < 0:
                return f"peer_forwarding census has an invalid count for node {node_id} at {boundary}"
            if node_sample.get("observation_source") not in CONTAINER_PEER_FORWARDING_CENSUS_SOURCES:
                return f"peer_forwarding census has no direct procfs source for node {node_id} at {boundary}"
            if node_sample.get("docker_host_pid_resolved") is not True:
                return f"peer_forwarding census did not resolve node {node_id}'s Docker host PID"
            if (
                node_sample["observation_source"] == "docker_exec_container_procfs_fd_inode_join"
                and node_sample.get("container_process_identity_verified") is not True
            ):
                return f"peer_forwarding census did not verify the broker identity for node {node_id}"
    return None


def remove_owned_image(isolation: Isolation) -> None:
    try:
        subprocess.run(
            ["docker", "image", "rm", "--force", isolation.image],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        # The workflow could only have built an image if Docker was available;
        # cleanup must not replace a successful benchmark result with a local
        # Docker-disconnect error.
        pass


def run(workflow: str, *, keep: bool) -> int:
    isolation = create_isolation()
    env = environment(isolation)
    command = lock_command(workflow, command_for(workflow, isolation))
    print(f"isolated run {isolation.run_id}: {shlex.join(command)}", flush=True)
    print(f"benchmark artifacts: {isolation.artifact_dir}", flush=True)
    completed = False
    try:
        result = subprocess.run(command, cwd=ROOT, env=env, check=False)
        if result.returncode != 0:
            return result.returncode
        if workflow == "bench-cluster-peer-forwarding-container-smoke":
            validation_error = peer_forwarding_container_census_error(
                isolation.artifact_dir / "cluster-container.json"
            )
            if validation_error is not None:
                print(
                    f"container peer-forwarding census validation failed: {validation_error}",
                    file=sys.stderr,
                    flush=True,
                )
                return 1
        completed = True
        return 0
    finally:
        if keep or not completed:
            print(f"isolated state retained at {isolation.runtime_dir}", file=sys.stderr, flush=True)
        else:
            if workflow in {
                "bench-container",
                "bench-cluster-container",
                "bench-cluster-container-smoke",
                "bench-cluster-peer-forwarding-container-smoke",
                "bench-compare",
            }:
                remove_owned_image(isolation)
            shutil.rmtree(isolation.runtime_dir, ignore_errors=True)
            try:
                isolation.artifact_dir.rmdir()
            except OSError:
                # Benchmark workflows intentionally leave their JSON results;
                # non-benchmark workflows normally leave this directory empty.
                pass


def main() -> int:
    args = parse_args()
    return run(args.workflow, keep=args.keep)


if __name__ == "__main__":
    raise SystemExit(main())
