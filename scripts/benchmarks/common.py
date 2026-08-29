"""Shared protocol and measurement helpers for the benchmark runners."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TIMEOUT_SECONDS = 30.0


class BenchmarkError(RuntimeError):
    """An expected benchmark setup or protocol failure."""


class LineClient:
    """A persistent client for Runnel's current line-delimited protocol."""

    def __init__(
        self,
        host: str,
        port: int,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.socket = socket.create_connection((host, port), timeout=timeout_seconds)
        self.reader = self.socket.makefile("rb")

    def request(self, request: dict[str, Any]) -> tuple[dict[str, Any], int]:
        encoded = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
        started = time.perf_counter_ns()
        self.socket.sendall(encoded)
        line = self.reader.readline()
        elapsed = time.perf_counter_ns() - started
        if not line:
            raise BenchmarkError("broker closed the protocol connection")
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise BenchmarkError(f"invalid broker response: {line!r}") from error
        if response.get("type") == "error":
            raise BenchmarkError(
                f"broker rejected {request.get('op')}: {response.get('code')}: "
                f"{response.get('message')}"
            )
        return response, elapsed

    def close(self) -> None:
        try:
            self.reader.close()
        finally:
            self.socket.close()


def request_ok(
    client: LineClient,
    request: dict[str, Any],
    response_type: str,
) -> tuple[dict[str, Any], int]:
    response, elapsed = client.request(request)
    if response.get("type") != response_type:
        raise BenchmarkError(f"unexpected response to {request.get('op')}: {response}")
    return response, elapsed


def create_stream(client: LineClient, stream: str) -> None:
    request_ok(client, {"op": "create_stream", "stream": stream}, "stream_created")


def publish(client: LineClient, stream: str, payload: str) -> tuple[int, int]:
    response, elapsed = request_ok(
        client,
        {"op": "publish", "stream": stream, "payload": payload},
        "published",
    )
    return int(response["offset"]), elapsed


def poll(
    client: LineClient,
    stream: str,
    consumer: str,
    expected_offset: int | None = None,
) -> tuple[dict[str, Any], int]:
    response, elapsed = request_ok(
        client,
        {"op": "poll", "stream": stream, "consumer": consumer},
        "message",
    )
    if expected_offset is not None and response.get("offset") != expected_offset:
        raise BenchmarkError(f"expected offset {expected_offset}, got {response}")
    return response, elapsed


def acknowledge(client: LineClient, stream: str, consumer: str, offset: int) -> int:
    _, elapsed = request_ok(
        client,
        {"op": "ack", "stream": stream, "consumer": consumer, "offset": offset},
        "acknowledged",
    )
    return elapsed


def percentile(values: list[int], percentage: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def metric(
    operation: str,
    latencies_ns: list[int],
    elapsed_ns: int,
    *,
    message_size: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not latencies_ns:
        raise BenchmarkError(f"scenario {operation} produced no measured messages")
    elapsed_seconds = elapsed_ns / 1_000_000_000
    result: dict[str, Any] = {
        "operation": operation,
        "messages": len(latencies_ns),
        "message_size_bytes": message_size,
        "elapsed_seconds": elapsed_seconds,
        "throughput_messages_per_second": len(latencies_ns) / elapsed_seconds,
        "latency_microseconds": {
            "p50": percentile(latencies_ns, 50) / 1_000,
            "p99": percentile(latencies_ns, 99) / 1_000,
            "p999": percentile(latencies_ns, 99.9) / 1_000,
            "max": max(latencies_ns) / 1_000,
        },
    }
    if metadata:
        result["metadata"] = metadata
    return result


def measure_scenario(
    measurements: Any, operation: Callable[[], dict[str, Any]]
) -> dict[str, Any]:
    token = measurements.begin()
    try:
        result = operation()
    except BaseException:
        measurements.end(token)
        raise
    result["resource_samples"] = measurements.end(token)
    return result


def wait_for_ready(http_port: int, *, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout_seconds
    url = f"http://127.0.0.1:{http_port}/health/ready"
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
        time.sleep(0.05)
    raise BenchmarkError(f"node on HTTP port {http_port} did not become ready: {last_error}")


def parse_sizes(value: str) -> list[int]:
    try:
        sizes = [int(part) for part in value.split(",") if part.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("payload sizes must be integers") from error
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("payload sizes must be positive integers")
    return sizes


def parse_nonnegative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if number < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return number


def default_binary() -> Path:
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", "target"))
    if not target_dir.is_absolute():
        target_dir = ROOT / target_dir
    return target_dir / "release" / "runnel"


def git_revision(*, short: bool = True, cwd: Path = ROOT) -> str:
    command = ["git", "rev-parse"]
    if short:
        command.append("--short")
    command.append("HEAD")
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 and result.stdout.strip() else "unknown"
