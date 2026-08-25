#!/usr/bin/env python3
"""Run a benchmark command under the shared local benchmark lock."""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DEFAULT_LOCK_PATH = Path("/tmp/runnel-benchmark.lock")


class BenchmarkLockBusy(RuntimeError):
    """Another benchmark currently owns the requested lock."""


@contextmanager
def benchmark_lock(
    mode: str,
    *,
    path: Path = DEFAULT_LOCK_PATH,
    wait: bool = True,
) -> Iterator[None]:
    if mode not in {"shared", "exclusive"}:
        raise ValueError(f"unsupported benchmark lock mode: {mode}")

    path.parent.mkdir(parents=True, exist_ok=True)
    flags = fcntl.LOCK_SH if mode == "shared" else fcntl.LOCK_EX
    with path.open("a+") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), flags | fcntl.LOCK_NB)
        except BlockingIOError:
            if not wait:
                raise BenchmarkLockBusy(
                    f"benchmark lock is busy: {path}; wait for the current benchmark or retry with the default waiting mode"
                ) from None
            print(f"Waiting for benchmark lock: {path}", flush=True)
            fcntl.flock(lock_file.fileno(), flags)

        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def lock_command(workflow: str, command: list[str]) -> list[str]:
    mode = {
        "bench": "shared",
        "bench-container": "exclusive",
        "bench-container-smoke": "shared",
        "bench-cluster": "exclusive",
        "bench-cluster-smoke": "shared",
        "profile-cluster": "exclusive",
        "bench-compare": "exclusive",
    }.get(workflow)
    if mode is None:
        return command
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        mode,
        "--",
        *command,
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("shared", "exclusive"), required=True)
    parser.add_argument(
        "--path",
        type=Path,
        default=Path(os.environ.get("RUNNEL_BENCHMARK_LOCK", DEFAULT_LOCK_PATH)),
        help="shared lock path (defaults to /tmp/runnel-benchmark.lock)",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="fail instead of waiting for another benchmark to release the lock",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command.pop(0)
    if not args.command:
        parser.error("a benchmark command is required after --")
    return args


def main() -> int:
    args = parse_args()
    try:
        with benchmark_lock(args.mode, path=args.path, wait=not args.no_wait):
            return subprocess.run(args.command, check=False).returncode
    except BenchmarkLockBusy as error:
        print(f"benchmark lock: {error}", file=sys.stderr)
        return 2
    except OSError as error:
        print(f"benchmark lock: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
