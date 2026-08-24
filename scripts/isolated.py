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
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKFLOW = "test"
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
            "CARGO_TARGET_DIR": str(isolation.target_dir),
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


def command_for(workflow: str, isolation: Isolation) -> list[str]:
    artifact = isolation.artifact_dir
    binary = isolation.target_dir / "release" / "runnel"
    cluster = [
        "python3",
        "scripts/benchmarks/cluster.py",
        "--build",
        "--binary",
        str(binary),
        "--output",
        str(artifact / "cluster.json"),
        "--log-dir",
        str(artifact / "cluster-logs"),
    ]
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
        return cluster
    if workflow == "bench-cluster-smoke":
        return [
            *cluster,
            "--messages",
            "20",
            "--warmup",
            "2",
            "--payload-sizes",
            "100",
            "--skip-recovery",
        ]
    if workflow == "profile-cluster":
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
    command = command_for(workflow, isolation)
    print(f"isolated run {isolation.run_id}: {shlex.join(command)}", flush=True)
    print(f"benchmark artifacts: {isolation.artifact_dir}", flush=True)
    completed = False
    try:
        result = subprocess.run(command, cwd=ROOT, env=env, check=False)
        completed = result.returncode == 0
        return result.returncode
    finally:
        if keep or not completed:
            print(f"isolated state retained at {isolation.runtime_dir}", file=sys.stderr, flush=True)
        else:
            if workflow in {"bench-container", "bench-compare"}:
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
