#!/usr/bin/env python3
"""Run the clean-slate Runnel benchmark prototype.

Use ``python3 -m scripts.benchmarks2.run`` from the repository root. This
prototype is intentionally not wired into the project's canonical commands.
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from .api import Limits, Workload
from .core import run_suite, summarize
from .runnel import RunnelBackend


def _sizes(value: str) -> tuple[int, ...]:
    try:
        sizes = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as error:
        raise argparse.ArgumentTypeError("payload sizes must be integers") from error
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("payload sizes must be positive")
    return sizes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="runnel:bench")
    parser.add_argument("--cpus", default="2")
    parser.add_argument("--memory", default="1g")
    parser.add_argument("--messages", type=int, default=1_000)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--payload-sizes", type=_sizes, default=(100, 1024))
    parser.add_argument("--scenarios", default=None, help="comma-separated scenario names")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        args.workload = Workload(
            args.messages, args.payload_sizes, args.warmup, args.concurrency
        )
    except ValueError as error:
        parser.error(str(error))
    args.selected = tuple(args.scenarios.split(",")) if args.scenarios else None
    return args


def main() -> int:
    args = parse_args()
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    output = args.output or Path("benchmark-results") / f"benchmark2-{timestamp}.json"
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    ).stdout.strip() or "unknown"
    result = run_suite(
        RunnelBackend(args.image),
        args.workload,
        limits=Limits(args.cpus, args.memory),
        selected=args.selected,
        metadata={"image": args.image, "revision": revision},
        output=output,
    )
    for scenario in result.scenarios:
        metrics = summarize(scenario)
        print(
            f"{metrics['operation']} {metrics['message_size_bytes']}B: "
            f"{metrics['throughput_messages_per_second']:.1f} messages/s"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
