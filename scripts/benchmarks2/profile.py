#!/usr/bin/env python3
"""Capture Linux perf profiles while exercising a real Runnel cluster.

The output is intentionally kept outside source control. Each node gets a
perf.data sample file and a human-readable perf report. Profiling is an
investigation workflow, not a correctness check or a CI gate.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import re
import shutil
import signal
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cluster import Cluster, build_binary
from common import (
    BenchmarkError,
    ROOT,
    acknowledge,
    create_stream,
    default_binary,
    git_revision,
    percentile,
    poll,
    publish,
)

DEFAULT_BINARY = default_binary()


class ProfilingError(RuntimeError):
    """The profiling prerequisites or workload could not be completed."""


TIMING_RE = re.compile(
    r'\bstage=(?P<stage>"[^"]+"|\S+).*?\belapsed_us=(?P<elapsed>\d+)'
)


def summarize_timing_values(values: dict[str, list[int]]) -> dict[str, Any]:
    return {
        stage: {
            "samples": len(samples),
            "p50_us": percentile(samples, 50),
            "p99_us": percentile(samples, 99),
            "max_us": max(samples),
        }
        for stage, samples in sorted(values.items())
    }


def summarize_timing_logs(log_dir: Path) -> dict[str, Any]:
    values: dict[str, list[int]] = defaultdict(list)
    for path in sorted(log_dir.glob("node-*.log")):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            match = TIMING_RE.search(line)
            if match:
                stage = match.group("stage").strip('"')
                elapsed = int(match.group("elapsed"))
                values[stage].append(elapsed)

    return {
        "log_directory": str(log_dir),
        "stages": summarize_timing_values(values),
    }


def continuous_worker(
    cluster: Cluster,
    worker_index: int,
    deadline: float,
    payload: str,
) -> int:
    client = cluster.client(worker_index)
    stream = f"profile-stream-{worker_index}"
    consumer = f"profile-consumer-{worker_index}"
    offset = 0
    completed = 0
    try:
        while time.monotonic() < deadline:
            published, _ = publish(client, stream, payload)
            if published != offset:
                raise ProfilingError(f"worker {worker_index} expected offset {offset}, got {published}")
            poll(client, stream, consumer, offset)
            acknowledge(client, stream, consumer, offset)
            offset += 1
            completed += 1
    finally:
        client.close()
    return completed


def prepare_workload(cluster: Cluster, workers: int) -> None:
    client = cluster.client(0)
    try:
        for worker_index in range(workers):
            create_stream(client, f"profile-stream-{worker_index}")
    finally:
        client.close()


def reset_broker_logs(log_dir: Path, node_count: int) -> None:
    """Start the timing log window after cluster setup has completed."""
    for index in range(node_count):
        (log_dir / f"node-{index + 1}.log").write_text("", encoding="utf-8")


def start_perf(node_pid: int, output: Path, frequency: int) -> subprocess.Popen[str]:
    command = [
        "perf",
        "record",
        "--freq",
        str(frequency),
        "--call-graph",
        "dwarf",
        "--output",
        str(output),
        "--pid",
        str(node_pid),
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(0.2)
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        raise ProfilingError(f"perf could not attach to PID {node_pid}: {stdout}{stderr}")
    return process


def stop_perf(process: subprocess.Popen[str]) -> tuple[int, str, str]:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        stdout, stderr = process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    return process.returncode or 0, stdout, stderr


def make_reports(profile_dir: Path, node_count: int) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for index in range(node_count):
        data = profile_dir / f"node-{index + 1}.perf.data"
        report = profile_dir / f"node-{index + 1}.perf.txt"
        if not data.is_file():
            continue
        result = subprocess.run(
            ["perf", "report", "--stdio", "--input", str(data)],
            capture_output=True,
            text=True,
            check=False,
        )
        report.write_text(result.stdout + result.stderr, encoding="utf-8")
        reports.append({"node": index + 1, "data": str(data), "report": str(report), "exit_code": result.returncode})
    return reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--features", help="Cargo features to use when building the broker")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--frequency", type=int, default=99)
    parser.add_argument("--payload-size", type=int, default=100)
    parser.add_argument("--ack-timeout-ms", type=int, default=1_000)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--skip-perf",
        action="store_true",
        help="run the workload and internal timing summary without attaching Linux perf",
    )
    args = parser.parse_args()
    if args.duration <= 0 or args.workers <= 0 or args.nodes < 3 or args.frequency <= 0 or args.payload_size <= 0:
        parser.error("duration, workers, frequency, and payload size must be positive; at least three nodes are required")
    if not args.skip_perf and shutil.which("perf") is None:
        parser.error("Linux perf is required; install the linux-tools package for this kernel")
    return args


def main() -> int:
    args = parse_args()
    if args.build:
        build_binary(args.binary, features=args.features)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    profile_dir = args.output or ROOT / "benchmark-results" / f"profile-{run_id}"
    profile_dir.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(
        args.binary,
        node_count=args.nodes,
        ack_timeout_ms=args.ack_timeout_ms,
        log_dir=profile_dir / "broker-logs",
    )
    profilers: list[tuple[int, subprocess.Popen[str]]] = []
    profiler_logs: list[dict[str, Any]] = []
    completed_messages = 0
    try:
        cluster.start()
        prepare_workload(cluster, args.workers)
        if not args.skip_perf:
            for index, node in enumerate(cluster.nodes):
                if node.process is None:
                    raise ProfilingError(f"node {index + 1} is not running")
                profilers.append(
                    (
                        index,
                        start_perf(
                            node.process.pid,
                            profile_dir / f"node-{index + 1}.perf.data",
                            args.frequency,
                        ),
                    )
                )
        reset_broker_logs(profile_dir / "broker-logs", args.nodes)
        deadline = time.monotonic() + args.duration
        payload = "x" * args.payload_size
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(continuous_worker, cluster, index, deadline, payload)
                for index in range(args.workers)
            ]
            completed_messages = sum(future.result() for future in futures)
    finally:
        for index, profiler in profilers:
            exit_code, stdout, stderr = stop_perf(profiler)
            profiler_logs.append(
                {"node": index + 1, "exit_code": exit_code, "stdout": stdout, "stderr": stderr}
            )
        cluster.close()

    reports = make_reports(profile_dir, args.nodes) if not args.skip_perf else []
    internal_timing = summarize_timing_logs(profile_dir / "broker-logs")
    result = {
        "schema_version": 1,
        "generated_at": timestamp.isoformat(),
        "profile": "internal-timing" if args.skip_perf else "linux-perf",
        "git_revision": git_revision(),
        "binary": str(args.binary),
        "features": args.features,
        "workload": {
            "duration_seconds": args.duration,
            "workers": args.workers,
            "nodes": args.nodes,
            "payload_size_bytes": args.payload_size,
            "ack_timeout_ms": args.ack_timeout_ms,
            "completed_messages": completed_messages,
            "operation_counts": {
                "publish": completed_messages,
                "poll": completed_messages,
                "ack": completed_messages,
            },
            "operation": "publish, non-grouped poll, and acknowledgement per worker stream",
        },
        "sampling": {"frequency_hz": args.frequency, "call_graph": "dwarf"},
        "internal_timing": internal_timing,
        "reports": reports,
        "profiler_processes": profiler_logs,
    }
    (profile_dir / "profile.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"profile artifacts written to {profile_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BenchmarkError, ProfilingError) as error:
        raise SystemExit(f"profiling failed: {error}") from error
