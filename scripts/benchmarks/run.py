#!/usr/bin/env python3
"""Run repeatable, resource-limited end-to-end benchmarks against a Runnel container.

The current protocol is deliberately used as-is. Results therefore describe the
current development protocol and broker semantics; they are not a claim about a
future binary protocol or a different durability mode.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_IMAGE = "runnel:bench"
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_TIMEOUT_SECONDS = 15.0


class BenchmarkError(RuntimeError):
    """An expected benchmark setup or protocol failure."""


class LineClient:
    """A persistent client for the current line-delimited JSON protocol."""

    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.create_connection((host, port), timeout=DEFAULT_TIMEOUT_SECONDS)
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


class DockerBroker:
    """Own a short-lived broker container and its temporary durable volume."""

    def __init__(self, image: str, cpus: str, memory: str) -> None:
        self.image = image
        self.cpus = cpus
        self.memory = memory
        self.name = f"runnel-bench-{os.getpid()}-{time.time_ns()}"
        self.data_dir = Path(tempfile.mkdtemp(prefix="runnel-bench-"))
        self.data_dir.chmod(0o777)
        self.image_id: str | None = None
        self.client_port: int | None = None
        self.http_port: int | None = None
        self.startup_ns: int | None = None
        self.stats = StatsSampler(self)

    def start(self) -> None:
        image = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", self.image],
            check=True,
            capture_output=True,
            text=True,
        )
        self.image_id = image.stdout.strip()
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--label",
            "runnel.benchmark=true",
            "--cpus",
            self.cpus,
            "--memory",
            self.memory,
            "--publish",
            "127.0.0.1::4222",
            "--publish",
            "127.0.0.1::8080",
            "--volume",
            f"{self.data_dir}:/var/lib/runnel",
            self.image,
        ]
        started = time.perf_counter_ns()
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            self.client_port = self._published_port(4222)
            self.http_port = self._published_port(8080)
            wait_for_ready(self.http_port)
        except (subprocess.CalledProcessError, BenchmarkError) as error:
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True, check=False
            )
            raise BenchmarkError(
                f"failed to start broker container: {error}\n{logs.stdout}{logs.stderr}"
            ) from error
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def restart(self) -> int:
        started = time.perf_counter_ns()
        subprocess.run(["docker", "restart", self.name], check=True, capture_output=True)
        self.client_port = self._published_port(4222)
        self.http_port = self._published_port(8080)
        try:
            wait_for_ready(self.http_port)
        except BenchmarkError as error:
            state = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} error={{.State.Error}}",
                    self.name,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True, check=False
            )
            raise BenchmarkError(
                f"broker was not ready after restart: {error}\n"
                f"{state.stdout.strip()}\n{logs.stdout}{logs.stderr}"
            ) from error
        return time.perf_counter_ns() - started

    def _published_port(self, container_port: int) -> int:
        result = subprocess.run(
            ["docker", "port", self.name, f"{container_port}/tcp"],
            check=True,
            capture_output=True,
            text=True,
        )
        match = re.search(r":(\d+)\s*$", result.stdout.strip())
        if match is None:
            raise BenchmarkError(f"could not parse published port: {result.stdout!r}")
        return int(match.group(1))

    def close(self) -> None:
        self.stats.close()
        subprocess.run(["docker", "rm", "--force", self.name], check=False, capture_output=True)
        shutil.rmtree(self.data_dir, ignore_errors=True)


class StatsSampler:
    """Capture coarse container resource samples without adding a client dependency."""

    def __init__(self, broker: DockerBroker) -> None:
        self.broker = broker
        self.samples: list[dict[str, float]] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="docker-stats", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            sample = read_stats(self.broker.name)
            if sample is not None:
                self.samples.append(sample)
            self.stop_event.wait(0.25)


def read_stats(container: str) -> dict[str, float] | None:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        raw = json.loads(result.stdout)
        return {
            "cpu_percent": float(raw["CPUPerc"].rstrip("%")),
            "memory_bytes": parse_size(raw["MemUsage"].split(" / ", 1)[0]),
            "memory_percent": float(raw["MemPerc"].rstrip("%")),
        }
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def parse_size(value: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]?i?B)\s*", value)
    if match is None:
        return 0.0
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return float(match.group(1)) * units[match.group(2)]


def wait_for_ready(http_port: int) -> None:
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SECONDS
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
    raise BenchmarkError(f"broker did not become ready: {last_error}")


def create_stream(client: LineClient, stream: str) -> None:
    response, _ = client.request({"op": "create_stream", "stream": stream})
    if response.get("type") != "stream_created":
        raise BenchmarkError(f"unexpected stream creation response: {response}")


def publish(client: LineClient, stream: str, payload: str) -> int:
    response, elapsed = client.request(
        {"op": "publish", "stream": stream, "payload": payload}
    )
    if response.get("type") != "published":
        raise BenchmarkError(f"unexpected publish response: {response}")
    return elapsed


def poll(client: LineClient, stream: str, consumer: str, expected_offset: int) -> int:
    response, elapsed = client.request(
        {"op": "poll", "stream": stream, "consumer": consumer}
    )
    if response.get("type") != "message" or response.get("offset") != expected_offset:
        raise BenchmarkError(f"unexpected poll response: {response}")
    return elapsed


def acknowledge(client: LineClient, stream: str, consumer: str, offset: int) -> int:
    response, elapsed = client.request(
        {"op": "ack", "stream": stream, "consumer": consumer, "offset": offset}
    )
    if response.get("type") != "acknowledged":
        raise BenchmarkError(f"unexpected acknowledgement response: {response}")
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
    name: str,
    latencies_ns: list[int],
    elapsed_ns: int,
    *,
    message_size: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    count = len(latencies_ns)
    result: dict[str, Any] = {
        "name": name,
        "messages": count,
        "message_size_bytes": message_size,
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "throughput_messages_per_second": count / (elapsed_ns / 1_000_000_000),
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


def new_stream(run_id: str, name: str, size: int) -> str:
    return f"bench_{run_id}_{name}_{size}"


def run_durable_publish(
    broker: DockerBroker, stream: str, payload: str, messages: int, warmup: int
) -> dict[str, Any]:
    if broker.client_port is None:
        raise BenchmarkError("broker client port was not discovered")
    client = LineClient("127.0.0.1", broker.client_port)
    try:
        create_stream(client, stream)
        for _ in range(warmup):
            publish(client, stream, payload)
        latencies: list[int] = []
        started = time.perf_counter_ns()
        for _ in range(messages):
            latencies.append(publish(client, stream, payload))
        elapsed = time.perf_counter_ns() - started
    finally:
        client.close()
    return metric("durable_publish", latencies, elapsed, message_size=len(payload))


def run_concurrent_publish(
    broker: DockerBroker,
    stream: str,
    payload: str,
    messages: int,
    concurrency: int,
) -> dict[str, Any]:
    if broker.client_port is None:
        raise BenchmarkError("broker client port was not discovered")
    setup = LineClient("127.0.0.1", broker.client_port)
    create_stream(setup, stream)
    setup.close()
    per_worker = [(messages // concurrency) + (index < messages % concurrency) for index in range(concurrency)]

    def worker(worker_messages: int) -> list[int]:
        client = LineClient("127.0.0.1", broker.client_port or 0)
        try:
            return [publish(client, stream, payload) for _ in range(worker_messages)]
        finally:
            client.close()

    started = time.perf_counter_ns()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, count) for count in per_worker if count]
        latencies = [latency for future in futures for latency in future.result()]
    elapsed = time.perf_counter_ns() - started
    return metric(
        "concurrent_publish",
        latencies,
        elapsed,
        message_size=len(payload),
        metadata={"concurrency": concurrency},
    )


def run_consume_ack(
    broker: DockerBroker, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    if broker.client_port is None:
        raise BenchmarkError("broker client port was not discovered")
    producer = LineClient("127.0.0.1", broker.client_port)
    create_stream(producer, stream)
    for _ in range(messages):
        publish(producer, stream, payload)
    producer.close()

    consumer = LineClient("127.0.0.1", broker.client_port)
    latencies: list[int] = []
    started = time.perf_counter_ns()
    try:
        for offset in range(messages):
            poll_started = time.perf_counter_ns()
            poll(consumer, stream, "benchmark-consumer", offset)
            acknowledge(consumer, stream, "benchmark-consumer", offset)
            latencies.append(time.perf_counter_ns() - poll_started)
    finally:
        consumer.close()
    elapsed = time.perf_counter_ns() - started
    return metric(
        "consume_ack",
        latencies,
        elapsed,
        message_size=len(payload),
        metadata={"publish_setup_excluded": True},
    )


def run_roundtrip(
    broker: DockerBroker, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    if broker.client_port is None:
        raise BenchmarkError("broker client port was not discovered")
    producer = LineClient("127.0.0.1", broker.client_port)
    consumer = LineClient("127.0.0.1", broker.client_port)
    create_stream(producer, stream)
    latencies: list[int] = []
    started = time.perf_counter_ns()
    try:
        for offset in range(messages):
            roundtrip_started = time.perf_counter_ns()
            publish(producer, stream, payload)
            poll(consumer, stream, "roundtrip-consumer", offset)
            acknowledge(consumer, stream, "roundtrip-consumer", offset)
            latencies.append(time.perf_counter_ns() - roundtrip_started)
    finally:
        producer.close()
        consumer.close()
    elapsed = time.perf_counter_ns() - started
    return metric("publish_consume_ack_roundtrip", latencies, elapsed, message_size=len(payload))


def run_restart_recovery(
    broker: DockerBroker, stream: str, payload: str
) -> dict[str, Any]:
    if broker.client_port is None:
        raise BenchmarkError("broker client port was not discovered")
    client = LineClient("127.0.0.1", broker.client_port)
    create_stream(client, stream)
    publish(client, stream, payload)
    poll(client, stream, "restart-consumer", 0)
    client.close()
    restart_ns = broker.restart()
    recovered = LineClient("127.0.0.1", broker.client_port)
    try:
        poll(recovered, stream, "restart-consumer", 0)
        acknowledge(recovered, stream, "restart-consumer", 0)
    finally:
        recovered.close()
    return {
        "name": "restart_recovery",
        "messages": 1,
        "message_size_bytes": len(payload),
        "restart_ready_seconds": restart_ns / 1_000_000_000,
        "metadata": {"unacknowledged_message_redelivered": True},
    }


def build_image(image: str) -> None:
    subprocess.run(["docker", "build", "--tag", image, str(ROOT)], check=True)


def git_revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


def parse_sizes(value: str) -> list[int]:
    sizes = [int(part) for part in value.split(",") if part.strip()]
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("payload sizes must be positive integers")
    return sizes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--build", action="store_true", help="build the image before running")
    parser.add_argument("--cpus", default="2", help="Docker CPU limit, for example 2 or 1.5")
    parser.add_argument("--memory", default="1g", help="Docker memory limit, for example 1g")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--output", type=Path, help="result JSON path; defaults to benchmark-results/<timestamp>.json")
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.concurrency <= 0:
        parser.error("messages and concurrency must be positive; warmup cannot be negative")
    return args


def stats_summary(samples: list[dict[str, float]]) -> dict[str, Any]:
    if not samples:
        return {"samples": 0}
    return {
        "samples": len(samples),
        "cpu_percent_max": max(sample["cpu_percent"] for sample in samples),
        "memory_bytes_max": max(sample["memory_bytes"] for sample in samples),
        "memory_percent_max": max(sample["memory_percent"] for sample in samples),
    }


def main() -> int:
    args = parse_args()
    if args.build:
        build_image(args.image)

    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    broker = DockerBroker(args.image, args.cpus, args.memory)
    scenarios: list[dict[str, Any]] = []
    try:
        broker.start()
        for size in args.payload_sizes:
            payload = "x" * size
            scenarios.append(
                run_durable_publish(
                    broker,
                    new_stream(run_id, "publish", size),
                    payload,
                    args.messages,
                    args.warmup,
                )
            )
            scenarios.append(
                run_concurrent_publish(
                    broker,
                    new_stream(run_id, "concurrent", size),
                    payload,
                    args.messages,
                    args.concurrency,
                )
            )
            scenarios.append(
                run_consume_ack(
                    broker,
                    new_stream(run_id, "consume", size),
                    payload,
                    args.messages,
                )
            )
            scenarios.append(
                run_roundtrip(
                    broker,
                    new_stream(run_id, "roundtrip", size),
                    payload,
                    args.messages,
                )
            )
            if not args.skip_restart and size == args.payload_sizes[0]:
                scenarios.append(
                    run_restart_recovery(
                        broker,
                        new_stream(run_id, "restart", size),
                        payload,
                    )
                )
    finally:
        broker.close()

    summary = {
        "schema_version": 1,
        "generated_at": timestamp.isoformat(),
        "backend": "runnel",
        "engine": "local",
        "git_revision": git_revision(),
        "host": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
        },
        "container": {
            "image": args.image,
            "image_id": broker.image_id,
            "cpu_limit": args.cpus,
            "memory_limit": args.memory,
            "startup_seconds": (broker.startup_ns or 0) / 1_000_000_000,
            "resource_samples": stats_summary(broker.stats.samples),
        },
        "workload": {
            "messages": args.messages,
            "warmup": args.warmup,
            "concurrency": args.concurrency,
            "payload_sizes_bytes": args.payload_sizes,
            "protocol": "line-delimited JSON with UTF-8 string payloads",
            "durability": "current broker default; see engine and implementation configuration",
        },
        "scenarios": scenarios,
    }
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"results written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
