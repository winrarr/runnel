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
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import (
    BenchmarkError,
    LineClient,
    ROOT,
    acknowledge,
    build_image,
    consume_ack_messages,
    create_stream,
    default_binary,
    metric,
    measure_message_batch,
    measure_scenario,
    parse_nonnegative_int,
    parse_sizes,
    percentile,
    poll,
    publish,
    publish_stream,
    publish_messages,
    prometheus_metrics,
    request_ok,
    result_metadata,
    wait_for_ready,
    write_json_result,
)
from resources import PeriodicSampler, read_cpu_seconds, read_stats, summarize_stats
from runtime import DockerContainer, create_network, inspect_image, remove_network

DEFAULT_BINARY = default_binary()
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_NODES = 3
# Match the broker's default so constrained benchmark hosts do not turn slow
# but valid operations into redeliveries while measuring unrelated scenarios.
DEFAULT_ACK_TIMEOUT_MS = 30_000
DEFAULT_SLOW_CONSUMER_DELAY_MS = 10
# Keep the retained-data probe beyond the local engine's bounded tail index so
# recovery measurements exercise a non-trivial retained history.
MIN_RETAINED_RECOVERY_MESSAGES = 1_025
DEFAULT_RETAINED_RECOVERY_MESSAGES = 2_048
COMMAND_TIMEOUT_SECONDS = 30.0


@dataclass
class Node:
    node_id: int
    broker_port: int
    http_port: int
    peer_port: int
    data_dir: Path
    process: subprocess.Popen[bytes] | None = None
    container: DockerContainer | None = None


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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


class ProcessStats(PeriodicSampler):
    """Sample aggregate broker CPU time and resident memory for each scenario."""

    def __init__(self, cluster: "Cluster") -> None:
        super().__init__("runnel-cluster-stats", interval_seconds=0.1)
        self.cluster = cluster
        self.node_samples: list[dict[str, dict[str, float]]] = []

    def begin(self) -> tuple[int, int, float, int]:
        self._record()
        with self.lock:
            sample_index = len(self.samples)
            node_sample_index = max(0, len(self.node_samples) - 1)
            cpu_start = self.samples[-1]["cpu_seconds"] if self.samples else 0.0
        return sample_index, node_sample_index, cpu_start, time.perf_counter_ns()

    def end(self, token: tuple[int, int, float, int]) -> dict[str, Any]:
        sample_index, node_sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        self._record()
        with self.lock:
            samples = list(self.samples[sample_index:])
            node_samples = list(self.node_samples[node_sample_index:])
        cpu_end = samples[-1]["cpu_seconds"] if samples else cpu_start
        result = summarize_stats(
            samples,
            cpu_seconds=cpu_end - cpu_start,
            elapsed_seconds=(ended_ns - started_ns) / 1_000_000_000,
        )
        result["per_node"] = self._summarize_nodes(node_samples)
        return result

    def summary(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.samples)
            node_samples = list(self.node_samples)
        result = summarize_stats(samples)
        result["per_node"] = self._summarize_nodes(node_samples)
        return result

    @staticmethod
    def _summarize_nodes(
        samples: list[dict[str, dict[str, float]]],
    ) -> dict[str, dict[str, Any]]:
        node_ids = sorted({node_id for sample in samples for node_id in sample})
        summaries: dict[str, dict[str, Any]] = {}
        for node_id in node_ids:
            values = [sample[node_id] for sample in samples if node_id in sample]
            if not values:
                continue
            memory_values = [value["memory_bytes"] for value in values]
            summary: dict[str, Any] = {
                "samples": len(values),
                "memory_bytes_avg": sum(memory_values) / len(memory_values),
                "memory_bytes_max": max(memory_values),
            }
            cpu_values = [
                value["cpu_seconds"]
                for value in values
                if isinstance(value.get("cpu_seconds"), (int, float))
            ]
            if cpu_values:
                summary["cpu_seconds"] = max(0.0, cpu_values[-1] - cpu_values[0])
            summaries[node_id] = summary
        return summaries

    def _record(self) -> None:
        cpu = 0.0
        memory = 0
        node_sample: dict[str, dict[str, float]] = {}
        for node in self.cluster.nodes:
            if self.cluster.runtime == "container":
                if node.container is None or not node.container.created:
                    continue
                node_cpu = read_cpu_seconds(node.container.name)
                sample = read_stats(node.container.name)
                if sample is not None:
                    node_value = {"memory_bytes": float(sample["memory_bytes"])}
                    if node_cpu is not None:
                        node_value["cpu_seconds"] = node_cpu
                        cpu += node_cpu
                    node_sample[str(node.node_id)] = node_value
                    memory += int(sample["memory_bytes"])
                continue
            if node.process is None:
                continue
            sample = process_stats(node.process.pid)
            if sample is not None:
                node_sample[str(node.node_id)] = {
                    "cpu_seconds": sample[0],
                    "memory_bytes": float(sample[1]),
                }
                cpu += sample[0]
                memory += sample[1]
        with self.lock:
            self.samples.append({"cpu_seconds": cpu, "memory_bytes": float(memory)})
            self.node_samples.append(node_sample)


class Cluster:
    """Own a short-lived static Runnel cluster and its durable test data."""

    def __init__(
        self,
        binary: Path,
        *,
        node_count: int,
        ack_timeout_ms: int,
        log_dir: Path | None = None,
        runtime: str = "process",
        image: str = "runnel:bench",
        cpus: str = "2",
        memory: str = "2g",
    ) -> None:
        if runtime not in {"process", "container"}:
            raise ValueError(f"unsupported cluster runtime: {runtime}")
        self.binary = binary
        self.node_count = node_count
        self.ack_timeout_ms = ack_timeout_ms
        self.runtime = runtime
        self.image = image
        self.cpus = cpus
        self.memory = memory
        self.root = Path(tempfile.mkdtemp(prefix="runnel-cluster-bench-"))
        self.root.chmod(0o777)
        self.network = f"runnel-cluster-net-{os.getpid()}-{time.time_ns()}"
        self.network_created = False
        self.image_id: str | None = None
        self.log_dir = log_dir
        if log_dir is not None:
            log_dir.mkdir(parents=True, exist_ok=True)
        self.nodes = [
            Node(
                node_id=index + 1,
                broker_port=free_port() if runtime == "process" else 0,
                http_port=free_port() if runtime == "process" else 0,
                peer_port=free_port() if runtime == "process" else 7000,
                data_dir=self.root / f"node-{index + 1}",
            )
            for index in range(node_count)
        ]
        self.stats = ProcessStats(self)
        self.startup_ns = 0

    def start(self) -> None:
        if self.runtime == "process" and not self.binary.is_file():
            raise BenchmarkError(f"broker binary does not exist: {self.binary}")
        if self.runtime == "container":
            self._prepare_container_runtime()
        started = time.perf_counter_ns()
        for index in range(self.node_count):
            self._start_node(index, bootstrap=index == 0)
        for node in self.nodes:
            wait_for_ready(node.http_port, timeout_seconds=COMMAND_TIMEOUT_SECONDS)
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def client(self, index: int) -> LineClient:
        return LineClient(
            "127.0.0.1",
            self.nodes[index % self.node_count].broker_port,
            COMMAND_TIMEOUT_SECONDS,
        )

    def metrics(self) -> dict[str, float] | None:
        """Return one flattened metrics snapshot for every live node."""
        snapshot: dict[str, float] = {}
        for node in self.nodes:
            metrics = prometheus_metrics(node.http_port)
            if metrics is None:
                return None
            snapshot.update(
                {f"node_{node.node_id}.{name}": value for name, value in metrics.items()}
            )
        return snapshot or None

    @contextmanager
    def connected_clients(self) -> Iterator[list[LineClient]]:
        clients = [self.client(index) for index in range(self.node_count)]
        try:
            yield clients
        finally:
            for client in clients:
                client.close()

    def stop_node(self, index: int) -> None:
        node = self.nodes[index]
        if self.runtime == "container":
            if node.container is None:
                return
            node.container.stop()
            node.container = None
            node.broker_port = 0
            node.http_port = 0
            return
        process = node.process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        else:
            # Reap a node that exited before readiness or during a scenario.
            # Leaving it unreaped creates a zombie and can exhaust process
            # resources during repeated benchmark runs.
            process.wait()
        node.process = None

    def restart_node(self, index: int) -> int:
        self.stop_node(index)
        started = time.perf_counter_ns()
        self._start_node(index, bootstrap=False)
        wait_for_ready(self.nodes[index].http_port, timeout_seconds=COMMAND_TIMEOUT_SECONDS)
        return time.perf_counter_ns() - started

    def close(self) -> None:
        self.stats.close()
        for index in range(self.node_count):
            self.stop_node(index)
        if self.network_created:
            remove_network(self.network)
        shutil.rmtree(self.root, ignore_errors=True)

    def _prepare_container_runtime(self) -> None:
        self.image_id = inspect_image(self.image)
        if self.image_id is None:
            raise BenchmarkError(f"Docker image does not exist: {self.image}")
        create_network(self.network)
        self.network_created = True

    def _start_node(self, index: int, *, bootstrap: bool) -> None:
        node = self.nodes[index]
        command = self._node_command(node, bootstrap=bootstrap)
        if self.runtime == "container":
            self._start_container(node, command)
            return

        node.data_dir.mkdir(parents=True, exist_ok=True)
        native_command = [str(self.binary), *command]
        stdout: Any = subprocess.DEVNULL
        stderr: Any = subprocess.DEVNULL
        if self.log_dir is not None:
            log = (self.log_dir / f"node-{node.node_id}.log").open("ab")
            stdout = log
            stderr = subprocess.STDOUT
        node.process = subprocess.Popen(native_command, stdout=stdout, stderr=stderr)

    def _node_command(self, node: Node, *, bootstrap: bool) -> list[str]:
        if self.runtime == "container":
            addresses = [
                f"{other.node_id}={self._container_name(other)}:7000" for other in self.nodes
            ]
            listen = "0.0.0.0"
            data_dir = "/var/lib/runnel"
            peer_port = 7000
        else:
            addresses = [
                f"{other.node_id}=127.0.0.1:{other.peer_port}" for other in self.nodes
            ]
            listen = "127.0.0.1"
            data_dir = str(node.data_dir)
            peer_port = node.peer_port
        command = [
            "--engine",
            "raft",
            "--node-id",
            str(node.node_id),
            "--cluster-name",
            f"runnel-benchmark-{os.getpid()}",
            "--data-dir",
            data_dir,
            "--listen",
            f"{listen}:{node.broker_port or 4222}",
            "--http-listen",
            f"{listen}:{node.http_port or 8080}",
            "--peer-listen",
            f"{listen}:{peer_port}",
            "--ack-timeout-ms",
            str(self.ack_timeout_ms),
        ]
        for address in addresses:
            command.extend(["--cluster-node", address])
        if bootstrap:
            command.append("--bootstrap")
        return command

    def _start_container(self, node: Node, command: list[str]) -> None:
        container = DockerContainer(
            name=self._container_name(node),
            image=self.image,
            network=self.network,
            cpus=self.cpus,
            memory=self.memory,
            data_dir=node.data_dir,
            data_target="/var/lib/runnel",
            command=command,
            published_ports=(4222, 8080),
        )
        node.container = container
        try:
            container.start(image_id=self.image_id)
            node.broker_port = container.published_port(4222)
            node.http_port = container.published_port(8080)
        except (subprocess.CalledProcessError, BenchmarkError) as error:
            raise BenchmarkError(
                f"failed to start container {container.name}: {error}\n{container.logs()}"
            ) from error

    def _container_name(self, node: Node) -> str:
        return f"{self.network}-node-{node.node_id}"


def poll_until_redelivered(
    client: LineClient, stream: str, consumer: str, expected_offset: int
) -> tuple[dict[str, Any], int]:
    """Wait for an expired unacknowledged message without assuming a margin."""
    deadline = time.monotonic() + COMMAND_TIMEOUT_SECONDS
    attempts = 0
    last_response: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        attempts += 1
        response, elapsed = client.request(
            {"op": "poll", "stream": stream, "consumer": consumer}
        )
        last_response = response
        if response.get("type") == "empty":
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.05, remaining))
            continue
        if response.get("type") != "message":
            raise BenchmarkError(f"unexpected recovery poll response: {response}")
        if response.get("offset") != expected_offset:
            raise BenchmarkError(f"expected recovery offset {expected_offset}, got {response}")
        if response.get("delivery_attempt") != 2:
            raise BenchmarkError(f"expected recovery delivery attempt 2, got {response}")
        return response, attempts
    raise BenchmarkError(
        f"message at offset {expected_offset} was not redelivered before the deadline; "
        f"last response: {last_response}"
    )


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
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()
    with cluster.connected_clients() as clients:
        return measure_message_batch(
            cluster.stats,
            "cluster_durable_publish",
            len(payload),
            lambda: publish_messages(
                lambda offset: clients[offset % len(clients)],
                stream,
                payload,
                messages,
                expected_offset=warmup,
            ),
            metadata={"nodes": cluster.node_count, "any_node_routing": True},
            metrics=cluster.metrics,
        )


def preload(cluster: Cluster, stream: str, payload: str, messages: int) -> None:
    client = cluster.client(0)
    try:
        publish_stream(client, stream, payload, messages)
    finally:
        client.close()


def run_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
        return measure_message_batch(
            cluster.stats,
            "cluster_consume_ack",
            len(payload),
            lambda: consume_ack_messages(
                lambda offset: clients[offset % len(clients)],
                stream,
                "cluster-consumer",
                messages,
                ack_client_for=lambda offset: clients[(offset + 1) % len(clients)],
            ),
            metadata={"nodes": cluster.node_count, "publish_setup_excluded": True},
            metrics=cluster.metrics,
        )


def run_slow_consumer(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    processing_delay_ms: int,
) -> dict[str, Any]:
    """Drain a bounded preloaded backlog with a fixed delay before each ack.

    The intentional processing delay is excluded from request latency samples
    but included in drain throughput. This keeps broker request latency
    comparable with the normal consume/ack scenario while making the slow
    consumer condition explicit in the result metadata.
    """
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
        def operation() -> dict[str, Any]:
            request_latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                client = clients[offset % len(clients)]
                response, poll_elapsed = poll(client, stream, "slow-consumer", offset)
                time.sleep(processing_delay_ms / 1_000)
                ack_elapsed = acknowledge(client, stream, "slow-consumer", offset)
                request_latencies.append(poll_elapsed + ack_elapsed)
                if response.get("payload") != payload:
                    raise BenchmarkError(f"slow consumer received unexpected payload at offset {offset}")
            result = metric(
                "cluster_slow_consumer",
                request_latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={
                    "nodes": cluster.node_count,
                    "processing_delay_ms": processing_delay_ms,
                    "preloaded_messages": messages,
                    "publish_setup_excluded": True,
                    "redelivery_expected": False,
                    "latency_scope": "poll_and_ack_request_time_excludes_processing_delay",
                    "throughput_scope": "preloaded_backlog_drain_includes_processing_delay",
                },
            )
            return result

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_grouped_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
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

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


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

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_restart_recovery(cluster: Cluster, stream: str, payload: str) -> dict[str, Any]:
    client = cluster.client(0)
    create_stream(client, stream)
    publish(client, stream, payload)
    poll(client, stream, "recovery-consumer", 0)
    client.close()
    # Let the configured lease expire. The measured operation below polls for
    # actual eligibility instead of relying on a fixed scheduling margin.
    time.sleep(cluster.ack_timeout_ms / 1_000)

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        restart_ns = cluster.restart_node(0)
        recovered = cluster.client(0)
        try:
            response, poll_attempts = poll_until_redelivered(
                recovered, stream, "recovery-consumer", 0
            )
            acknowledge(recovered, stream, "recovery-consumer", 0)
        finally:
            recovered.close()
        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            "cluster_restart_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "unacknowledged_message_redelivered": True,
                "delivery_attempt": response.get("delivery_attempt"),
                "latency_scope": "restart_ready_to_redelivered_acknowledgement",
                "redelivery_poll_attempts": poll_attempts,
                "restarted_node": cluster.nodes[0].node_id,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_retained_recovery(
    cluster: Cluster, stream: str, payload: str, retained_messages: int
) -> dict[str, Any]:
    """Measure restart recovery after preloading a bounded retained history.

    The preload is deliberately excluded from the measured interval. The
    measured probe restarts one node, waits for readiness, replays the earliest
    record, and acknowledges it. This exercises recovery and cold replay with a
    known retained-data size without inventing retention or batch semantics.
    """
    preload(cluster, stream, payload, retained_messages)

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        restart_ns = cluster.restart_node(0)
        recovered = cluster.client(0)
        try:
            response, _ = poll(recovered, stream, "retained-recovery", 0)
            if response.get("payload") != payload:
                raise BenchmarkError(
                    "retained recovery returned an unexpected payload at offset 0"
                )
            acknowledge(recovered, stream, "retained-recovery", 0)
        finally:
            recovered.close()
        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            "cluster_retained_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "retained_messages": retained_messages,
                "retained_logical_payload_bytes": retained_messages * len(payload),
                "recovery_probe_offset": 0,
                "publish_setup_excluded": True,
                "latency_scope": "restart_ready_to_earliest_replay_acknowledgement",
                "redelivery_expected": False,
                "restarted_node": cluster.nodes[0].node_id,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def build_binary(binary: Path, *, features: str | None = None) -> None:
    command = ["cargo", "build", "--locked", "-p", "runnel-server", "--release"]
    if features:
        command.extend(["--features", features])
    subprocess.run(command, cwd=ROOT, check=True)
    if not binary.is_file():
        raise BenchmarkError(f"release build did not produce {binary}")


def resource_limits(
    *, runtime: str, cpus: str, memory: str
) -> dict[str, str]:
    """Describe the optional cgroup budget supplied by a local harness."""
    if runtime == "container":
        return {
            "processes": "Docker containers; benchmark client remains host-side",
            "cpu_per_broker": cpus,
            "memory_per_broker": memory,
        }
    native_cpu = os.environ.get("RUNNEL_BENCHMARK_CPU_LIMIT")
    native_memory = os.environ.get("RUNNEL_BENCHMARK_MEMORY_LIMIT")
    if native_cpu and native_memory:
        return {
            "processes": "systemd user scope; benchmark client and broker nodes",
            "cpu": native_cpu,
            "memory": native_memory,
        }
    return {"processes": "host-scheduled; no cgroup limit"}


def parse_retained_messages(value: str) -> int:
    try:
        messages = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("retained messages must be an integer") from error
    if messages < MIN_RETAINED_RECOVERY_MESSAGES:
        raise argparse.ArgumentTypeError(
            "retained messages must exceed the current 1,024-record tail index "
            f"(minimum: {MIN_RETAINED_RECOVERY_MESSAGES})"
        )
    return messages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument(
        "--runtime",
        choices=("process", "container"),
        default="process",
        help="run native broker processes or bounded Docker broker containers",
    )
    parser.add_argument("--image", default="runnel:bench")
    parser.add_argument("--cpus", default="2", help="per-container CPU limit")
    parser.add_argument("--memory", default="2g", help="per-container memory limit")
    parser.add_argument("--build", action="store_true", help="build the selected broker artifact")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--ack-timeout-ms", type=int, default=DEFAULT_ACK_TIMEOUT_MS)
    parser.add_argument(
        "--slow-consumer-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_SLOW_CONSUMER_DELAY_MS,
        help="fixed processing delay before each slow-consumer acknowledgement",
    )
    parser.add_argument(
        "--retained-messages",
        type=parse_retained_messages,
        default=DEFAULT_RETAINED_RECOVERY_MESSAGES,
        help=(
            "retained records preloaded for the restart-recovery growth probe "
            f"(minimum: {MIN_RETAINED_RECOVERY_MESSAGES})"
        ),
    )
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument(
        "--skip-recovery",
        action="store_true",
        help="skip both restart-recovery scenarios",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-dir", type=Path)
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3 or args.concurrency <= 0:
        parser.error("messages, nodes, and concurrency must be positive; at least three nodes are required")
    if args.ack_timeout_ms <= 0:
        parser.error("ack timeout must be positive")
    if args.slow_consumer_delay_ms >= args.ack_timeout_ms:
        parser.error("slow consumer delay must be shorter than the acknowledgement timeout")
    return args


def main() -> int:
    args = parse_args()
    if args.build:
        if args.runtime == "container":
            build_image(args.image)
        else:
            build_binary(args.binary)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"cluster-{run_id}.json"
    cluster = Cluster(
        args.binary,
        node_count=args.nodes,
        ack_timeout_ms=args.ack_timeout_ms,
        log_dir=args.log_dir,
        runtime=args.runtime,
        image=args.image,
        cpus=args.cpus,
        memory=args.memory,
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
                run_slow_consumer(
                    cluster,
                    f"cluster_{run_id}_slow_consumer_{size}",
                    payload,
                    args.messages,
                    args.slow_consumer_delay_ms,
                )
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
                scenarios.append(
                    run_retained_recovery(
                        cluster,
                        f"cluster_{run_id}_retained_recovery_{size}",
                        payload,
                        args.retained_messages,
                    )
                )
    finally:
        cluster.close()

    startup_seconds = cluster.startup_ns / 1_000_000_000
    workload = {
        "messages": args.messages,
        "warmup": args.warmup,
        "concurrency": args.concurrency,
        "nodes": args.nodes,
        "ack_timeout_ms": args.ack_timeout_ms,
        "slow_consumer_delay_ms": args.slow_consumer_delay_ms,
        "payload_sizes_bytes": args.payload_sizes,
        "runtime": args.runtime,
        "protocol": "line-delimited JSON with UTF-8 string payloads",
        "protocol_version": "provisional-line-json-v1",
        "payload_encoding": "utf-8",
        "compression": "none",
        "durability": "committed by the current three-node Raft quorum and local durable state",
    }
    if not args.skip_recovery:
        workload["retained_recovery_messages"] = args.retained_messages

    result = {
        **result_metadata(
            run_id,
            timestamp,
            benchmark_suite="cluster",
            comparison_mode="cluster-baseline",
            docker=args.runtime == "container",
        ),
        "resource_limits": resource_limits(
            runtime=args.runtime, cpus=args.cpus, memory=args.memory
        ),
        "workload": workload,
        "backends": {
            "runnel-cluster": {
                "image": args.image if args.runtime == "container" else str(args.binary),
                "image_id": cluster.image_id,
                "runtime": args.runtime,
                "acknowledgement": "durable quorum commit",
                "replication": f"{args.nodes}-node static Multi-Raft",
                "measurement_boundary": "public line-delimited JSON protocol",
                "measurement_client": "scripts/benchmarks/cluster.py",
                "client_image": "host Python runtime",
                "startup_seconds": startup_seconds,
                "resource_samples": cluster.stats.summary(),
                "scenarios": scenarios,
            }
        },
    }
    write_json_result(output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
