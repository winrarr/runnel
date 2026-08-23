#!/usr/bin/env python3
"""Benchmark a real three-node Runnel cluster through its public protocol.

This is a development baseline, not a production benchmark harness. It keeps
the workload and durability boundary explicit: every measured publish is sent
through the line-delimited JSON protocol and every delivery scenario includes
an acknowledgement. The broker uses the current static three-node Raft
backend with its normal durable storage.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def default_binary() -> Path:
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", "target"))
    if not target_dir.is_absolute():
        target_dir = ROOT / target_dir
    return target_dir / "release" / "runnel"


DEFAULT_BINARY = default_binary()
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_NODES = 3
# Match the broker's default so constrained benchmark hosts do not turn slow
# but valid operations into redeliveries while measuring unrelated scenarios.
DEFAULT_ACK_TIMEOUT_MS = 30_000
COMMAND_TIMEOUT_SECONDS = 30.0


class BenchmarkError(RuntimeError):
    """An expected benchmark setup or protocol failure."""


class LineClient:
    """A persistent client for the current line-delimited JSON protocol."""

    def __init__(self, port: int) -> None:
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=COMMAND_TIMEOUT_SECONDS)
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


@dataclass
class Node:
    node_id: int
    broker_port: int
    http_port: int
    peer_port: int
    data_dir: Path
    process: subprocess.Popen[bytes] | None = None


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_ready(http_port: int) -> None:
    deadline = time.monotonic() + COMMAND_TIMEOUT_SECONDS
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


def process_stats(pid: int) -> tuple[float, int] | None:
    """Return process CPU seconds and resident bytes on Linux."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields = stat[stat.rfind(")") + 2 :].split()
        clock_ticks = os.sysconf("SC_CLK_TCK")
        cpu_seconds = (int(fields[11]) + int(fields[12])) / clock_ticks
        rss = 0
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
                break
        return cpu_seconds, rss
    except (IndexError, OSError, ValueError):
        return None


class ProcessStats:
    """Sample aggregate broker CPU time and resident memory for each scenario."""

    def __init__(self, cluster: "Cluster") -> None:
        self.cluster = cluster
        self.samples: list[dict[str, float]] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="runnel-cluster-stats", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)

    def begin(self) -> tuple[int, float, int]:
        self._record()
        with self.lock:
            sample_index = len(self.samples)
            cpu_start = self.samples[-1]["cpu_seconds"] if self.samples else 0.0
        return sample_index, cpu_start, time.perf_counter_ns()

    def end(self, token: tuple[int, float, int]) -> dict[str, Any]:
        sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        self._record()
        with self.lock:
            samples = list(self.samples[sample_index:])
        cpu_end = samples[-1]["cpu_seconds"] if samples else cpu_start
        result: dict[str, Any] = {
            "samples": len(samples),
            "cpu_seconds": max(0.0, cpu_end - cpu_start),
            "elapsed_seconds": max(0.0, (ended_ns - started_ns) / 1_000_000_000),
        }
        if samples:
            result["memory_bytes_max"] = max(sample["memory_bytes"] for sample in samples)
            result["memory_bytes_avg"] = sum(sample["memory_bytes"] for sample in samples) / len(samples)
        return result

    def summary(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.samples)
        result: dict[str, Any] = {"samples": len(samples)}
        if samples:
            result["memory_bytes_max"] = max(sample["memory_bytes"] for sample in samples)
            result["memory_bytes_avg"] = sum(sample["memory_bytes"] for sample in samples) / len(samples)
        return result

    def _record(self) -> None:
        cpu = 0.0
        memory = 0
        for node in self.cluster.nodes:
            if node.process is None:
                continue
            sample = process_stats(node.process.pid)
            if sample is not None:
                cpu += sample[0]
                memory += sample[1]
        with self.lock:
            self.samples.append({"cpu_seconds": cpu, "memory_bytes": float(memory)})

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self._record()
            self.stop_event.wait(0.1)


class Cluster:
    """Own a short-lived static Runnel cluster and its durable test data."""

    def __init__(
        self,
        binary: Path,
        *,
        node_count: int,
        ack_timeout_ms: int,
        log_dir: Path | None = None,
    ) -> None:
        self.binary = binary
        self.node_count = node_count
        self.ack_timeout_ms = ack_timeout_ms
        self.root = Path(tempfile.mkdtemp(prefix="runnel-cluster-bench-"))
        self.root.chmod(0o777)
        self.log_dir = log_dir
        if log_dir is not None:
            log_dir.mkdir(parents=True, exist_ok=True)
        self.nodes = [
            Node(
                node_id=index + 1,
                broker_port=free_port(),
                http_port=free_port(),
                peer_port=free_port(),
                data_dir=self.root / f"node-{index + 1}",
            )
            for index in range(node_count)
        ]
        self.stats = ProcessStats(self)
        self.startup_ns = 0

    def start(self) -> None:
        if not self.binary.is_file():
            raise BenchmarkError(f"broker binary does not exist: {self.binary}")
        started = time.perf_counter_ns()
        for index in range(self.node_count):
            self._start_node(index, bootstrap=index == 0)
        for node in self.nodes:
            wait_for_ready(node.http_port)
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def client(self, index: int) -> LineClient:
        return LineClient(self.nodes[index % self.node_count].broker_port)

    def stop_node(self, index: int) -> None:
        node = self.nodes[index]
        if node.process is None:
            return
        if node.process.poll() is None:
            node.process.terminate()
            try:
                node.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                node.process.kill()
                node.process.wait()
        node.process = None

    def restart_node(self, index: int) -> int:
        self.stop_node(index)
        started = time.perf_counter_ns()
        self._start_node(index, bootstrap=False)
        wait_for_ready(self.nodes[index].http_port)
        return time.perf_counter_ns() - started

    def close(self) -> None:
        self.stats.close()
        for index in range(self.node_count):
            self.stop_node(index)
        shutil.rmtree(self.root, ignore_errors=True)

    def _start_node(self, index: int, *, bootstrap: bool) -> None:
        node = self.nodes[index]
        node.data_dir.mkdir(parents=True, exist_ok=True)
        addresses = [f"{other.node_id}=127.0.0.1:{other.peer_port}" for other in self.nodes]
        command = [
            str(self.binary),
            "--engine",
            "raft",
            "--node-id",
            str(node.node_id),
            "--cluster-name",
            f"runnel-benchmark-{os.getpid()}",
            "--data-dir",
            str(node.data_dir),
            "--listen",
            f"127.0.0.1:{node.broker_port}",
            "--http-listen",
            f"127.0.0.1:{node.http_port}",
            "--peer-listen",
            f"127.0.0.1:{node.peer_port}",
            "--ack-timeout-ms",
            str(self.ack_timeout_ms),
        ]
        for address in addresses:
            command.extend(["--cluster-node", address])
        if bootstrap:
            command.append("--bootstrap")
        stdout: Any = subprocess.DEVNULL
        stderr: Any = subprocess.DEVNULL
        if self.log_dir is not None:
            log = (self.log_dir / f"node-{node.node_id}.log").open("ab")
            stdout = log
            stderr = subprocess.STDOUT
        node.process = subprocess.Popen(command, stdout=stdout, stderr=stderr)


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
    if not latencies_ns:
        raise BenchmarkError(f"scenario {name} produced no measured messages")
    elapsed_seconds = elapsed_ns / 1_000_000_000
    result: dict[str, Any] = {
        "operation": name,
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


def measured(cluster: Cluster, operation: Any) -> dict[str, Any]:
    token = cluster.stats.begin()
    try:
        result = operation()
    except BaseException:
        cluster.stats.end(token)
        raise
    result["resource_samples"] = cluster.stats.end(token)
    return result


def request_ok(client: LineClient, request: dict[str, Any], response_type: str) -> tuple[dict[str, Any], int]:
    response, elapsed = client.request(request)
    if response.get("type") != response_type:
        raise BenchmarkError(f"unexpected response to {request.get('op')}: {response}")
    return response, elapsed


def create_stream(client: LineClient, stream: str) -> None:
    response, _ = client.request({"op": "create_stream", "stream": stream})
    if response.get("type") != "stream_created":
        raise BenchmarkError(f"unexpected stream creation response: {response}")


def publish(client: LineClient, stream: str, payload: str) -> tuple[int, int]:
    response, elapsed = request_ok(
        client,
        {"op": "publish", "stream": stream, "payload": payload},
        "published",
    )
    return int(response["offset"]), elapsed


def poll(client: LineClient, stream: str, consumer: str, expected_offset: int) -> tuple[dict[str, Any], int]:
    response, elapsed = request_ok(
        client,
        {"op": "poll", "stream": stream, "consumer": consumer},
        "message",
    )
    if response.get("offset") != expected_offset:
        raise BenchmarkError(f"expected offset {expected_offset}, got {response}")
    return response, elapsed


def acknowledge(client: LineClient, stream: str, consumer: str, offset: int) -> int:
    _, elapsed = request_ok(
        client,
        {"op": "ack", "stream": stream, "consumer": consumer, "offset": offset},
        "acknowledged",
    )
    return elapsed


def poll_group(
    client: LineClient, stream: str, consumer: str, member: str
) -> tuple[dict[str, Any], int]:
    response, elapsed = request_ok(
        client,
        {"op": "poll_group", "stream": stream, "consumer": consumer, "member": member},
        "message",
    )
    return response, elapsed


def acknowledge_group(
    client: LineClient,
    stream: str,
    consumer: str,
    member: str,
    offset: int,
    token: str,
) -> int:
    _, elapsed = request_ok(
        client,
        {
            "op": "ack_group",
            "stream": stream,
            "consumer": consumer,
            "member": member,
            "offset": offset,
            "delivery_token": token,
        },
        "acknowledged",
    )
    return elapsed


def run_durable_publish(
    cluster: Cluster, stream: str, payload: str, messages: int, warmup: int
) -> dict[str, Any]:
    setup = cluster.client(0)
    create_stream(setup, stream)
    for _ in range(warmup):
        publish(setup, stream, payload)
    setup.close()
    clients = [cluster.client(index) for index in range(cluster.node_count)]
    try:
        def operation() -> dict[str, Any]:
            latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                published_offset, elapsed = publish(clients[offset % len(clients)], stream, payload)
                if published_offset != warmup + offset:
                    raise BenchmarkError(f"expected published offset {warmup + offset}, got {published_offset}")
                latencies.append(elapsed)
            return metric(
                "cluster_durable_publish",
                latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={"nodes": cluster.node_count, "any_node_routing": True},
            )

        return measured(cluster, operation)
    finally:
        for client in clients:
            client.close()


def preload(cluster: Cluster, stream: str, payload: str, messages: int) -> None:
    client = cluster.client(0)
    try:
        create_stream(client, stream)
        for expected_offset in range(messages):
            offset, _ = publish(client, stream, payload)
            if offset != expected_offset:
                raise BenchmarkError(f"expected preload offset {expected_offset}, got {offset}")
    finally:
        client.close()


def run_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    clients = [cluster.client(index) for index in range(cluster.node_count)]
    try:
        def operation() -> dict[str, Any]:
            latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                poll_client = clients[offset % len(clients)]
                ack_client = clients[(offset + 1) % len(clients)]
                roundtrip_started = time.perf_counter_ns()
                poll(poll_client, stream, "cluster-consumer", offset)
                acknowledge(ack_client, stream, "cluster-consumer", offset)
                latencies.append(time.perf_counter_ns() - roundtrip_started)
            return metric(
                "cluster_consume_ack",
                latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={"nodes": cluster.node_count, "publish_setup_excluded": True},
            )

        return measured(cluster, operation)
    finally:
        for client in clients:
            client.close()


def run_grouped_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    clients = [cluster.client(index) for index in range(cluster.node_count)]
    try:
        def operation() -> dict[str, Any]:
            latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                member = "member-a" if offset % 2 == 0 else "member-b"
                poll_client = clients[offset % len(clients)]
                ack_client = clients[(offset + 1) % len(clients)]
                roundtrip_started = time.perf_counter_ns()
                response, _ = poll_group(poll_client, stream, "cluster-workers", member)
                if response.get("offset") != offset:
                    raise BenchmarkError(f"expected grouped offset {offset}, got {response}")
                acknowledge_group(
                    ack_client,
                    stream,
                    "cluster-workers",
                    member,
                    offset,
                    str(response["delivery_token"]),
                )
                latencies.append(time.perf_counter_ns() - roundtrip_started)
            return metric(
                "cluster_grouped_consume_ack",
                latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={"nodes": cluster.node_count, "members": 2, "parallel": False},
            )

        return measured(cluster, operation)
    finally:
        for client in clients:
            client.close()


def run_parallel_grouped(
    cluster: Cluster, stream: str, payload: str, messages: int, concurrency: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    lock = threading.Lock()
    latencies: list[int] = []
    processed = 0
    deadline = time.monotonic() + COMMAND_TIMEOUT_SECONDS

    def worker(worker_index: int) -> None:
        nonlocal processed
        client = cluster.client(worker_index)
        member = f"parallel-member-{worker_index}"
        try:
            while True:
                with lock:
                    if processed >= messages:
                        return
                if time.monotonic() >= deadline:
                    raise BenchmarkError("parallel grouped benchmark did not drain its messages")
                started = time.perf_counter_ns()
                response, _ = client.request(
                    {
                        "op": "poll_group",
                        "stream": stream,
                        "consumer": "parallel-workers",
                        "member": member,
                    }
                )
                if response.get("type") == "empty":
                    time.sleep(0.001)
                    continue
                if response.get("type") != "message":
                    raise BenchmarkError(f"unexpected parallel poll response: {response}")
                acknowledge_group(
                    client,
                    stream,
                    "parallel-workers",
                    member,
                    int(response["offset"]),
                    str(response["delivery_token"]),
                )
                with lock:
                    latencies.append(time.perf_counter_ns() - started)
                    processed += 1
        finally:
            client.close()

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, index) for index in range(concurrency)]
            for future in futures:
                future.result()
        if len(latencies) != messages:
            raise BenchmarkError(f"parallel grouped benchmark processed {len(latencies)} of {messages}")
        return metric(
            "cluster_parallel_grouped_consume_ack",
            latencies,
            time.perf_counter_ns() - started,
            message_size=len(payload),
            metadata={"nodes": cluster.node_count, "members": concurrency, "parallel": True},
        )

    return measured(cluster, operation)


def run_restart_recovery(cluster: Cluster, stream: str, payload: str) -> dict[str, Any]:
    client = cluster.client(0)
    create_stream(client, stream)
    publish(client, stream, payload)
    poll(client, stream, "recovery-consumer", 0)
    client.close()
    time.sleep(cluster.ack_timeout_ms / 1_000 + 0.05)

    def operation() -> dict[str, Any]:
        restart_ns = cluster.restart_node(0)
        recovered = cluster.client(1)
        try:
            response, elapsed = poll(recovered, stream, "recovery-consumer", 0)
            if response.get("delivery_attempt") != 2:
                raise BenchmarkError(f"expected recovery delivery attempt 2, got {response}")
            acknowledge(recovered, stream, "recovery-consumer", 0)
        finally:
            recovered.close()
        return {
            "operation": "cluster_restart_recovery",
            "messages": 1,
            "message_size_bytes": len(payload),
            "elapsed_seconds": elapsed / 1_000_000_000,
            "throughput_messages_per_second": 1 / (elapsed / 1_000_000_000),
            "latency_microseconds": {"p50": elapsed / 1_000, "p99": elapsed / 1_000, "p999": elapsed / 1_000},
            "restart_ready_seconds": restart_ns / 1_000_000_000,
            "metadata": {"unacknowledged_message_redelivered": True, "delivery_attempt": response.get("delivery_attempt")},
        }

    return measured(cluster, operation)


def build_binary(binary: Path, *, features: str | None = None) -> None:
    command = ["cargo", "build", "--locked", "-p", "runnel-server", "--release"]
    if features:
        command.extend(["--features", features])
    subprocess.run(command, cwd=ROOT, check=True)
    if not binary.is_file():
        raise BenchmarkError(f"release build did not produce {binary}")


def git_revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


def parse_sizes(value: str) -> list[int]:
    sizes = [int(part) for part in value.split(",") if part.strip()]
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("payload sizes must be positive integers")
    return sizes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--ack-timeout-ms", type=int, default=DEFAULT_ACK_TIMEOUT_MS)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument("--skip-recovery", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-dir", type=Path)
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3 or args.concurrency <= 0:
        parser.error("messages, nodes, and concurrency must be positive; at least three nodes are required")
    if args.ack_timeout_ms <= 0:
        parser.error("ack timeout must be positive")
    return args


def main() -> int:
    args = parse_args()
    if args.build:
        build_binary(args.binary)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"cluster-{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(
        args.binary,
        node_count=args.nodes,
        ack_timeout_ms=args.ack_timeout_ms,
        log_dir=args.log_dir,
    )
    scenarios: list[dict[str, Any]] = []
    try:
        cluster.start()
        for size in args.payload_sizes:
            payload = "x" * size
            scenarios.append(
                run_durable_publish(
                    cluster,
                    f"cluster_{run_id}_publish_{size}",
                    payload,
                    args.messages,
                    args.warmup,
                )
            )
            scenarios.append(
                run_consume_ack(cluster, f"cluster_{run_id}_consume_{size}", payload, args.messages)
            )
            scenarios.append(
                run_grouped_consume_ack(cluster, f"cluster_{run_id}_grouped_{size}", payload, args.messages)
            )
            scenarios.append(
                run_parallel_grouped(
                    cluster,
                    f"cluster_{run_id}_parallel_{size}",
                    payload,
                    args.messages,
                    args.concurrency,
                )
            )
            if not args.skip_recovery and size == args.payload_sizes[0]:
                scenarios.append(
                    run_restart_recovery(cluster, f"cluster_{run_id}_recovery_{size}", payload)
                )
    finally:
        cluster.close()

    startup_seconds = cluster.startup_ns / 1_000_000_000
    result = {
        "schema_version": 1,
        "generated_at": timestamp.isoformat(),
        "comparison_mode": "cluster-baseline",
        "benchmark_suite": "cluster",
        "git_revision": git_revision(),
        "host": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
        },
        "resource_limits": {"processes": "host-scheduled; no cgroup limit"},
        "workload": {
            "messages": args.messages,
            "warmup": args.warmup,
            "concurrency": args.concurrency,
            "nodes": args.nodes,
            "ack_timeout_ms": args.ack_timeout_ms,
            "payload_sizes_bytes": args.payload_sizes,
            "protocol": "line-delimited JSON with UTF-8 string payloads",
            "durability": "committed by the current three-node Raft quorum and local durable state",
        },
        "backends": {
            "runnel-cluster": {
                "image": str(args.binary),
                "image_id": None,
                "acknowledgement": "durable quorum commit",
                "replication": f"{args.nodes}-node static Multi-Raft",
                "measurement_boundary": "public line-delimited JSON protocol",
                "measurement_client": "scripts/benchmarks/cluster.py",
                "startup_seconds": startup_seconds,
                "resource_samples": cluster.stats.summary(),
                "scenarios": scenarios,
            }
        },
    }
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"results written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
