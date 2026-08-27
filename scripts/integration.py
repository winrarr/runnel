#!/usr/bin/env python3
"""Run the process, cluster, image, and container-smoke integration checks."""

from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path

from benchmarks.lock import lock_command


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="image tag used by the container smoke check")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        required=True,
        help="directory for the container benchmark result",
    )
    parser.add_argument(
        "--skip-image-build",
        action="store_true",
        help="use an image built by the caller instead of building one",
    )
    return parser.parse_args()


def run(command: list[str]) -> None:
    print(f"integration step: {shlex.join(command)}", flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    args = parse_args()
    args.artifact_dir.mkdir(parents=True, exist_ok=True)

    run(["./scripts/smoke.sh"])
    run(
        [
            "cargo",
            "test",
            "--locked",
            "-p",
            "runnel-server",
            "--test",
            "cluster_smoke",
            "--",
            "--nocapture",
            "--test-threads=1",
        ]
    )
    if not args.skip_image_build:
        run(["docker", "build", "--tag", args.image, str(ROOT)])

    benchmark = [
        "python3",
        "scripts/benchmarks/run.py",
        "--image",
        args.image,
        "--messages",
        "20",
        "--warmup",
        "2",
        "--concurrency",
        "2",
        "--payload-sizes",
        "100",
        "--output",
        str(args.artifact_dir / "container.json"),
    ]
    run(lock_command("bench-container-smoke", benchmark))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
