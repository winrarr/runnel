#!/usr/bin/env python3
"""Run a bounded, repeatable matrix of clustered benchmark cases.

Each case is a fresh invocation of ``cluster.py``. This keeps fault and
recovery probes from sharing state with throughput cases and leaves every raw
result and runner log available beside one machine-readable matrix envelope.
The matrix is an engineering baseline; it does not combine unlike cases into
a performance claim.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
CLUSTER = SCRIPT_DIR / "cluster.py"
sys.path.insert(0, str(SCRIPT_DIR))

from cluster import (  # noqa: E402
    DEFAULT_ACK_TIMEOUT_MS,
    DEFAULT_BINARY,
    DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
    DEFAULT_MESSAGES,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_SLOW_CONSUMER_DELAY_MS,
    DEFAULT_WARMUP,
    MAX_LEADER_FAILURE_TIMEOUT_SECONDS,
    MAX_PEER_FORWARDING_CONCURRENCY,
    MAX_PEER_FORWARDING_TIMEOUT_SECONDS,
    MAX_PEER_RESPONSE_DELAY_MS,
    MAX_PUBLISH_BATCH_SIZE,
    parse_nonnegative_int,
    parse_positive_float,
    parse_scenarios,
    parse_sizes,
)
from common import (  # noqa: E402
    BenchmarkError,
    result_metadata,
    write_json_result,
)
from resource_scope import (  # noqa: E402
    ResourceScopeError,
    resource_limits,
    resource_scope_command,
)


DEFAULT_SCENARIOS = (
    "durable_publish",
    "consume_ack",
    "slow_consumer",
    "restart_recovery",
    "cluster_retained_recovery",
    "leader_failure_recovery",
    "follower_failure_recovery",
)
DEFAULT_CONCURRENCY_VALUES = [2, 8]
DEFAULT_SLOW_CONSUMER_DELAYS_MS = [DEFAULT_SLOW_CONSUMER_DELAY_MS]
DEFAULT_RETAINED_MESSAGE_VALUES = [DEFAULT_RETAINED_RECOVERY_MESSAGES]
DEFAULT_RUNTIME_VALUES = ["process"]
DEFAULT_CASE_TIMEOUT_SECONDS = 300.0
MAX_CASE_TIMEOUT_SECONDS = 3_600.0
DEFAULT_MAX_CASES = 128
MAX_MATRIX_CASES = 512
MAX_REPETITIONS = 20


def parse_integer_values(value: str, *, minimum: int, label: str) -> list[int]:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(f"{label} cannot be empty")
    try:
        values = [int(part) for part in parts]
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{label} must be integers") from error
    if any(number < minimum for number in values):
        raise argparse.ArgumentTypeError(
            f"{label} must be at least {minimum}"
        )
    if len(values) != len(set(values)):
        raise argparse.ArgumentTypeError(f"{label} must not contain duplicates")
    return values


def parse_runtimes(value: str) -> list[str]:
    runtimes = [part.strip() for part in value.split(",") if part.strip()]
    if not runtimes:
        raise argparse.ArgumentTypeError("runtimes cannot be empty")
    if any(runtime not in {"process", "container"} for runtime in runtimes):
        raise argparse.ArgumentTypeError("runtimes must be process or container")
    if len(runtimes) != len(set(runtimes)):
        raise argparse.ArgumentTypeError("runtimes must not contain duplicates")
    return runtimes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--image", default="runnel:bench")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--ack-timeout-ms", type=int, default=DEFAULT_ACK_TIMEOUT_MS)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument(
        "--scenarios",
        type=parse_scenarios,
        default=list(DEFAULT_SCENARIOS),
        help="comma-separated scenario cases to repeat independently",
    )
    parser.add_argument(
        "--concurrency-values",
        type=lambda value: parse_integer_values(value, minimum=1, label="concurrency values"),
        default=DEFAULT_CONCURRENCY_VALUES,
        help="comma-separated parallel/forwarding concurrency values",
    )
    parser.add_argument(
        "--slow-consumer-delays-ms",
        type=lambda value: parse_integer_values(
            value, minimum=0, label="slow-consumer delays"
        ),
        default=DEFAULT_SLOW_CONSUMER_DELAYS_MS,
    )
    parser.add_argument(
        "--retained-message-values",
        type=lambda value: parse_integer_values(
            value, minimum=1_025, label="retained message values"
        ),
        default=DEFAULT_RETAINED_MESSAGE_VALUES,
    )
    parser.add_argument(
        "--runtimes",
        type=parse_runtimes,
        default=DEFAULT_RUNTIME_VALUES,
        help="comma-separated runtime cases: process or container",
    )
    parser.add_argument("--cpus", default="2", help="per-broker container or scope CPU limit")
    parser.add_argument("--memory", default="2g", help="per-broker container or scope memory limit")
    parser.add_argument(
        "--batch-size", type=int, default=32, help="records per publish_batch request"
    )
    parser.add_argument("--peer-response-delay-ms", type=parse_nonnegative_int, default=0)
    parser.add_argument(
        "--peer-forwarding-timeout-seconds",
        type=parse_positive_float,
        default=60.0,
    )
    parser.add_argument(
        "--failure-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
        help="bounded leader/follower fault-case timeout",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=1,
        help="independent repetitions per matrix case",
    )
    parser.add_argument(
        "--case-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_CASE_TIMEOUT_SECONDS,
        help="outer wall-clock budget for each child benchmark",
    )
    parser.add_argument(
        "--max-cases",
        type=int,
        default=DEFAULT_MAX_CASES,
        help="maximum planned cases after expanding the matrix",
    )
    parser.add_argument(
        "--native-resource-scope",
        action="store_true",
        help="run native process cases inside the bounded systemd user scope",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="run remaining cases after a failed or timed-out case; exit nonzero",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--artifacts-dir", type=Path)
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3:
        parser.error("messages and nodes must be positive; warmup cannot be negative")
    if args.ack_timeout_ms <= 0:
        parser.error("ack timeout must be positive")
    if any(delay >= args.ack_timeout_ms for delay in args.slow_consumer_delays_ms):
        parser.error("slow consumer delays must be shorter than the acknowledgement timeout")
    if args.batch_size <= 0 or args.batch_size > MAX_PUBLISH_BATCH_SIZE:
        parser.error(
            f"batch size must be between 1 and {MAX_PUBLISH_BATCH_SIZE} records"
        )
    if any(value > MAX_PEER_FORWARDING_CONCURRENCY for value in args.concurrency_values):
        parser.error(
            "concurrency values exceed the bounded maximum "
            f"of {MAX_PEER_FORWARDING_CONCURRENCY}"
        )
    if args.peer_response_delay_ms > MAX_PEER_RESPONSE_DELAY_MS:
        parser.error(
            "peer response delay exceeds the bounded maximum "
            f"of {MAX_PEER_RESPONSE_DELAY_MS} ms"
        )
    if args.peer_forwarding_timeout_seconds > MAX_PEER_FORWARDING_TIMEOUT_SECONDS:
        parser.error(
            "peer forwarding timeout exceeds the bounded maximum "
            f"of {MAX_PEER_FORWARDING_TIMEOUT_SECONDS:g} seconds"
        )
    if args.failure_timeout_seconds > MAX_LEADER_FAILURE_TIMEOUT_SECONDS:
        parser.error(
            "failure timeout exceeds the bounded maximum "
            f"of {MAX_LEADER_FAILURE_TIMEOUT_SECONDS:g} seconds"
        )
    if args.repetitions <= 0 or args.repetitions > MAX_REPETITIONS:
        parser.error(f"repetitions must be between 1 and {MAX_REPETITIONS}")
    if args.max_cases <= 0:
        parser.error("max cases must be positive")
    if args.max_cases > MAX_MATRIX_CASES:
        parser.error(f"max cases must not exceed {MAX_MATRIX_CASES}")
    if args.case_timeout_seconds > MAX_CASE_TIMEOUT_SECONDS:
        parser.error(
            f"case timeout exceeds the bounded maximum of {MAX_CASE_TIMEOUT_SECONDS:g} seconds"
        )
    if args.peer_response_delay_ms and "container" in args.runtimes:
        parser.error("peer response delay requires process-only matrix runtimes")
    return args


def matrix_cases(args: argparse.Namespace) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for scenario in args.scenarios:
        concurrency_values = (
            args.concurrency_values
            if scenario in {"parallel_grouped_consume_ack", "peer_forwarding"}
            else args.concurrency_values[:1]
        )
        delay_values = (
            args.slow_consumer_delays_ms
            if scenario == "slow_consumer"
            else args.slow_consumer_delays_ms[:1]
        )
        retained_values = (
            args.retained_message_values
            if scenario in {"cluster_retained_recovery", "retained_hot_path"}
            else args.retained_message_values[:1]
        )
        for runtime in args.runtimes:
            for payload_size in args.payload_sizes:
                for concurrency in concurrency_values:
                    for delay_ms in delay_values:
                        for retained_messages in retained_values:
                            for repetition in range(1, args.repetitions + 1):
                                cases.append(
                                    {
                                        "scenario": scenario,
                                        "runtime": runtime,
                                        "payload_size": payload_size,
                                        "concurrency": concurrency,
                                        "slow_consumer_delay_ms": delay_ms,
                                        "retained_messages": retained_messages,
                                        "repetition": repetition,
                                    }
                                )
    if len(cases) > args.max_cases:
        raise BenchmarkError(
            f"matrix expands to {len(cases)} cases, exceeding --max-cases {args.max_cases}"
        )
    return cases


def case_command(
    args: argparse.Namespace,
    case: dict[str, Any],
    output: Path,
    log_dir: Path,
    *,
    build: bool,
) -> list[str]:
    command = [sys.executable, str(CLUSTER)]
    if build:
        command.append("--build")
    command.extend(
        [
            "--binary",
            str(args.binary),
            "--runtime",
            case["runtime"],
            "--image",
            args.image,
            "--cpus",
            args.cpus,
            "--memory",
            args.memory,
            "--messages",
            str(args.messages),
            "--warmup",
            str(args.warmup),
            "--nodes",
            str(args.nodes),
            "--concurrency",
            str(case["concurrency"]),
            "--scenarios",
            case["scenario"],
            "--ack-timeout-ms",
            str(args.ack_timeout_ms),
            "--slow-consumer-delay-ms",
            str(case["slow_consumer_delay_ms"]),
            "--batch-size",
            str(args.batch_size),
            "--retained-messages",
            str(case["retained_messages"]),
            "--peer-forwarding-concurrency",
            str(case["concurrency"]),
            "--peer-response-delay-ms",
            str(args.peer_response_delay_ms),
            "--peer-forwarding-timeout-seconds",
            str(args.peer_forwarding_timeout_seconds),
            "--leader-failure-timeout-seconds",
            str(args.failure_timeout_seconds),
            "--payload-sizes",
            str(case["payload_size"]),
            "--output",
            str(output),
            "--log-dir",
            str(log_dir),
        ]
    )
    return command


def case_id(index: int, case: dict[str, Any]) -> str:
    scenario = case["scenario"].replace("_", "-")
    return (
        f"case-{index:03d}-{scenario}-{case['runtime']}-"
        f"payload-{case['payload_size']}-c{case['concurrency']}-"
        f"delay-{case['slow_consumer_delay_ms']}-retained-{case['retained_messages']}-"
        f"r{case['repetition']}"
    )


def stop_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def run_case(
    args: argparse.Namespace,
    case: dict[str, Any],
    index: int,
    artifacts_dir: Path,
    *,
    build: bool,
) -> dict[str, Any]:
    identifier = case_id(index, case)
    case_dir = artifacts_dir / identifier
    case_dir.mkdir(parents=True, exist_ok=False)
    output = case_dir / "result.json"
    log_dir = case_dir / "broker-logs"
    command = case_command(args, case, output, log_dir, build=build)
    native_limits: dict[str, str] | None = None
    if args.native_resource_scope and case["runtime"] == "process":
        native_limits = resource_limits(cpus=args.cpus, memory=args.memory)
        command = resource_scope_command(
            command,
            unit=f"runnel-matrix-{os.getpid()}-{index}",
            cpus=args.cpus,
            memory=args.memory,
        )
    environment = os.environ.copy()
    if native_limits is not None:
        environment.update(
            {
                "RUNNEL_BENCHMARK_CPU_LIMIT": native_limits["cpu"],
                "RUNNEL_BENCHMARK_MEMORY_LIMIT": native_limits["memory"],
            }
        )
    started = datetime.now(UTC)
    started_ns = time.monotonic_ns()
    with (case_dir / "runner.log").open("wb") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        timed_out = False
        try:
            return_code = process.wait(timeout=args.case_timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_process_group(process)
            return_code = None
    finished = datetime.now(UTC)
    record: dict[str, Any] = {
        **case,
        "case_id": identifier,
        "command": command,
        "result_path": str(output),
        "log_path": str(case_dir / "runner.log"),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": (time.monotonic_ns() - started_ns) / 1_000_000_000,
        "return_code": return_code,
    }
    if timed_out:
        record.update(
            {
                "status": "timed_out",
                "error": f"case exceeded {args.case_timeout_seconds:g}-second timeout",
            }
        )
        return record
    if return_code != 0:
        record.update(
            {
                "status": "failed",
                "error": f"child benchmark exited with code {return_code}",
            }
        )
        return record
    try:
        result = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        record.update({"status": "failed", "error": f"invalid child result: {error}"})
        return record
    if not isinstance(result, dict) or result.get("status") != "complete":
        record.update({"status": "failed", "error": "child result was not complete"})
        return record
    record.update({"status": "complete", "result": result})
    return record


def run_matrix(
    args: argparse.Namespace,
    *,
    executor: Callable[..., dict[str, Any]] = run_case,
) -> int:
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"cluster-matrix-{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    cases = matrix_cases(args)
    artifacts_dir = args.artifacts_dir or output.parent / f"{output.stem}-cases"
    if artifacts_dir.exists():
        raise BenchmarkError(f"matrix artifacts directory already exists: {artifacts_dir}")
    artifacts_dir.mkdir(parents=True)

    records: list[dict[str, Any]] = []
    built_runtimes: set[str] = set()
    for index, case in enumerate(cases, start=1):
        build = args.build and case["runtime"] not in built_runtimes
        print(
            f"Running matrix case {index}/{len(cases)}: {case_id(index, case)}",
            flush=True,
        )
        try:
            record = executor(args, case, index, artifacts_dir, build=build)
        except (BenchmarkError, ResourceScopeError, OSError, subprocess.SubprocessError) as error:
            record = {
                **case,
                "case_id": case_id(index, case),
                "status": "failed",
                "error": str(error),
            }
        records.append(record)
        if args.build:
            built_runtimes.add(case["runtime"])
        if record.get("status") != "complete" and not args.keep_going:
            break

    failed = [record for record in records if record.get("status") != "complete"]
    status = "complete" if not failed and len(records) == len(cases) else "failed"
    workload = {
        "messages": args.messages,
        "warmup": args.warmup,
        "nodes": args.nodes,
        "scenarios": args.scenarios,
        "payload_sizes_bytes": args.payload_sizes,
        "concurrency_values": args.concurrency_values,
        "slow_consumer_delay_values_ms": args.slow_consumer_delays_ms,
        "retained_message_values": args.retained_message_values,
        "runtimes": args.runtimes,
        "repetitions": args.repetitions,
        "ack_timeout_ms": args.ack_timeout_ms,
        "case_timeout_seconds": args.case_timeout_seconds,
        "failure_timeout_seconds": args.failure_timeout_seconds,
        "durability": "current clustered broker quorum and local durable state",
        "protocol": "line-delimited JSON with UTF-8 string payloads",
        "protocol_version": "provisional-line-json-v1",
    }
    result = {
        **result_metadata(
            run_id,
            timestamp,
            benchmark_suite="cluster-matrix",
            comparison_mode="cluster-baseline-matrix",
            docker="container" in args.runtimes,
        ),
        "status": status,
        "resource_limits": {
            "cpu": args.cpus,
            "memory": args.memory,
            "native_process_scope": args.native_resource_scope,
            "scope_note": (
                "systemd user scope covers native client and broker processes"
                if args.native_resource_scope
                else "container cases apply per-broker Docker limits; native cases are host-scheduled"
            ),
        },
        "workload": workload,
        "matrix": {
            "planned_cases": len(cases),
            "attempted_cases": len(records),
            "completed_cases": sum(
                record.get("status") == "complete" for record in records
            ),
            "failed_cases": len(failed),
            "keep_going": args.keep_going,
            "artifacts_dir": str(artifacts_dir),
        },
        "cases": records,
    }
    write_json_result(output, result)
    return 0 if status == "complete" else 1


def main() -> int:
    args = parse_args()
    try:
        return run_matrix(args)
    except (BenchmarkError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"could not run benchmark matrix: {error}") from error


if __name__ == "__main__":
    raise SystemExit(main())
