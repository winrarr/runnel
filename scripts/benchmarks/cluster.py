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
import base64
import json
import math
import os
import shutil
import socket
import socketserver
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
from resources import (
    PeriodicSampler,
    directory_size,
    read_cpu_seconds,
    read_stats,
    summarize_stats,
)
from runtime import DockerContainer, create_network, inspect_image, remove_network

DEFAULT_BINARY = default_binary()
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_NODES = 3
# Match the broker's default so constrained benchmark hosts do not turn slow
# but valid operations into redeliveries while measuring unrelated scenarios.
DEFAULT_ACK_TIMEOUT_MS = 30_000
DEFAULT_SLOW_CONSUMER_DELAY_MS = 10
DEFAULT_PUBLISH_BATCH_SIZE = 32
MAX_PUBLISH_BATCH_SIZE = 1_024
HOT_ORDERING_HOT_KEY = "hot-key"
DEFAULT_HOT_KEY_MESSAGES = 64
DEFAULT_COLD_KEY_COUNT = 4
DEFAULT_COLD_MESSAGES_PER_KEY = 8
DEFAULT_HOT_ORDERING_CONCURRENCY = 4
DEFAULT_HOT_KEY_PROCESSING_DELAY_MS = 5
DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS = 60.0
MAX_HOT_ORDERING_CONCURRENCY = 128
MAX_HOT_KEY_PROCESSING_DELAY_MS = 5_000
MAX_HOT_ORDERING_TIMEOUT_SECONDS = 300.0
MAX_HOT_ORDERING_MESSAGES = 4_096
# The topology-free forwarding pool currently reserves one control lane from
# five total connections, leaving four shared forwarding lanes. Keep the
# focused workload above that boundary by default so queueing is observable.
DEFAULT_PEER_FORWARDING_CONCURRENCY = 8
DEFAULT_PEER_RESPONSE_DELAY_MS = 0
DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS = 60.0
MAX_PEER_FORWARDING_CONCURRENCY = 128
MAX_PEER_RESPONSE_DELAY_MS = 2_000
MAX_PEER_FORWARDING_TIMEOUT_SECONDS = 300.0
PEER_FORWARDING_INGRESS_NODE_INDEX = 1
DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS = 60.0
MAX_LEADER_FAILURE_TIMEOUT_SECONDS = 300.0
DEFAULT_SCENARIOS = (
    "durable_publish",
    "consume_ack",
    "slow_consumer",
    "grouped_consume_ack",
    "parallel_grouped_consume_ack",
    "restart_recovery",
    "cluster_retained_recovery",
)
SCENARIO_NAMES = (
    *DEFAULT_SCENARIOS,
    "peer_forwarding",
    "publish_batch",
    "hot_ordering",
    "leader_failure_recovery",
    "follower_failure_recovery",
)
# Keep the retained-data probe beyond the local engine's bounded tail index so
# recovery measurements exercise a non-trivial retained history.
MIN_RETAINED_RECOVERY_MESSAGES = 1_025
DEFAULT_RETAINED_RECOVERY_MESSAGES = 2_048
COMMAND_TIMEOUT_SECONDS = 30.0


def _read_proxy_frame(sock: socket.socket) -> bytes | None:
    header = _receive_exact(sock, 4)
    if header is None:
        return None
    length = int.from_bytes(header, "big")
    if length > 64 * 1024 * 1024:
        raise BenchmarkError("peer proxy frame exceeds the 64 MiB limit")
    payload = _receive_exact(sock, length)
    if payload is None:
        raise ConnectionError("peer proxy closed a partial frame")
    return header + payload


def _is_forward_response(frame: bytes) -> bool:
    try:
        response = json.loads(frame[4:])
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(response, dict) and "Forward" in response


def _receive_exact(sock: socket.socket, size: int) -> bytes | None:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            if chunks:
                raise ConnectionError("peer proxy closed a partial frame")
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class _PeerDelayProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, target_port: int, response_delay_ms: int) -> None:
        self.target_port = target_port
        self.response_delay_ms = response_delay_ms
        self.response_delay_seconds = response_delay_ms / 1_000
        self.stats_lock = threading.Lock()
        self.connection_count = 0
        self.active_connections = 0
        self.max_active_connections = 0
        self.request_count = 0
        self.response_count = 0
        self.delayed_response_count = 0
        super().__init__(("127.0.0.1", 0), _PeerDelayProxyHandler)


class _PeerDelayProxyHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _PeerDelayProxyServer)
        with server.stats_lock:
            server.connection_count += 1
            server.active_connections += 1
            server.max_active_connections = max(
                server.max_active_connections, server.active_connections
            )
        try:
            with socket.create_connection(
                ("127.0.0.1", server.target_port), timeout=COMMAND_TIMEOUT_SECONDS
            ) as target:
                self.request.settimeout(COMMAND_TIMEOUT_SECONDS)
                while True:
                    frame = _read_proxy_frame(self.request)
                    if frame is None:
                        return
                    target.sendall(frame)
                    with server.stats_lock:
                        server.request_count += 1
                    response = _read_proxy_frame(target)
                    if response is None:
                        return
                    if server.response_delay_seconds and _is_forward_response(response):
                        time.sleep(server.response_delay_seconds)
                        with server.stats_lock:
                            server.delayed_response_count += 1
                    self.request.sendall(response)
                    with server.stats_lock:
                        server.response_count += 1
        except (BenchmarkError, ConnectionError, OSError):
            return
        finally:
            with server.stats_lock:
                server.active_connections -= 1


class PeerResponseDelayProxy:
    """Delay framed peer responses while preserving the real TCP peer path."""

    def __init__(self, target_port: int, response_delay_ms: int) -> None:
        self.server = _PeerDelayProxyServer(target_port, response_delay_ms)
        self.started = False
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name=f"runnel-peer-delay-{self.server.server_address[1]}",
            daemon=True,
        )

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        if self.started:
            return
        self.thread.start()
        self.started = True

    def close(self) -> None:
        if self.started:
            self.server.shutdown()
        self.server.server_close()
        if self.started:
            self.thread.join(timeout=5)

    def summary(self) -> dict[str, Any]:
        with self.server.stats_lock:
            return {
                "target_port": self.server.target_port,
                "listen_port": self.port,
                "response_delay_ms": self.server.response_delay_ms,
                "connections": self.server.connection_count,
                "max_active_connections": self.server.max_active_connections,
                "requests": self.server.request_count,
                "responses": self.server.response_count,
                "delayed_responses": self.server.delayed_response_count,
            }


@dataclass
class Node:
    node_id: int
    broker_port: int
    http_port: int
    peer_port: int
    peer_address_port: int
    data_dir: Path
    process: subprocess.Popen[bytes] | None = None
    container: DockerContainer | None = None
    peer_proxy: PeerResponseDelayProxy | None = None


@dataclass
class HotOrderingObservation:
    """Collect bounded delivery observations for the hot-ordering probe."""

    expected_offsets_by_key: dict[str, list[int]]
    hot_key: str
    hot_key_messages: int
    delivered_offsets_by_key: dict[str, list[int]]
    completed_offsets_by_key: dict[str, list[int]]
    delivery_wait_ns_by_key: dict[str, list[int]]
    request_latency_ns_by_key: dict[str, list[int]]
    completion_elapsed_ns_by_key: dict[str, list[int]]
    delivered_attempts: dict[int, int]
    completed_offsets: set[int]
    active_offsets: set[int]
    processing_offsets: set[int]
    ack_started_offsets: set[int]
    active_by_key: dict[str, int]
    max_in_flight_by_key: dict[str, int]
    request_latencies_ns: list[int]
    hot_backlog_at_delivery: list[int]
    hot_backlog_at_cold_completion: list[int]
    cold_keys_with_progress_while_hot_backlog: set[str]
    cold_keys_completed_while_hot_backlog: set[str]
    hot_completed_messages: int = 0
    hot_drained_elapsed_ns: int | None = None
    max_in_flight_messages: int = 0

    @classmethod
    def for_records(
        cls, records: list[tuple[int, str]], hot_key: str
    ) -> "HotOrderingObservation":
        expected: dict[str, list[int]] = {}
        for offset, key in records:
            expected.setdefault(key, []).append(offset)
        return cls(
            expected_offsets_by_key=expected,
            hot_key=hot_key,
            hot_key_messages=len(expected.get(hot_key, [])),
            delivered_offsets_by_key={key: [] for key in expected},
            completed_offsets_by_key={key: [] for key in expected},
            delivery_wait_ns_by_key={key: [] for key in expected},
            request_latency_ns_by_key={key: [] for key in expected},
            completion_elapsed_ns_by_key={key: [] for key in expected},
            delivered_attempts={},
            completed_offsets=set(),
            active_offsets=set(),
            processing_offsets=set(),
            ack_started_offsets=set(),
            active_by_key={key: 0 for key in expected},
            max_in_flight_by_key={key: 0 for key in expected},
            request_latencies_ns=[],
            hot_backlog_at_delivery=[],
            hot_backlog_at_cold_completion=[],
            cold_keys_with_progress_while_hot_backlog=set(),
            cold_keys_completed_while_hot_backlog=set(),
        )

    def record_delivery(
        self,
        *,
        offset: int,
        key: str,
        delivery_attempt: int,
        delivery_wait_ns: int,
    ) -> None:
        expected_offsets = self.expected_offsets_by_key.get(key)
        if expected_offsets is None or offset not in expected_offsets:
            raise BenchmarkError(
                f"hot-ordering delivery returned unexpected key/offset: {key!r}/{offset}"
            )
        if delivery_attempt != 1:
            raise BenchmarkError(
                "hot-ordering workload observed an unexpected redelivery at "
                f"offset {offset} (attempt {delivery_attempt})"
            )
        if offset in self.delivered_attempts:
            raise BenchmarkError(
                f"hot-ordering workload delivered offset {offset} more than once"
            )
        observed_offsets = self.delivered_offsets_by_key[key]
        if observed_offsets and offset <= observed_offsets[-1]:
            raise BenchmarkError(
                f"hot-ordering key {key!r} was delivered out of order: "
                f"{observed_offsets[-1]} then {offset}"
            )
        observed_offsets.append(offset)
        self.delivered_attempts[offset] = delivery_attempt
        self.delivery_wait_ns_by_key[key].append(delivery_wait_ns)
        self.active_offsets.add(offset)
        self.processing_offsets.add(offset)
        self.active_by_key[key] += 1
        self.max_in_flight_by_key[key] = max(
            self.max_in_flight_by_key[key], self.active_by_key[key]
        )
        self.max_in_flight_messages = max(
            self.max_in_flight_messages, len(self.processing_offsets)
        )
        if key == self.hot_key:
            self.hot_backlog_at_delivery.append(
                self.hot_key_messages - self.hot_completed_messages
            )

    def record_ack_start(self, *, offset: int, key: str) -> None:
        if offset not in self.processing_offsets:
            raise BenchmarkError(
                f"hot-ordering workload acknowledged offset {offset} without processing it"
            )
        self.processing_offsets.remove(offset)
        self.active_by_key[key] -= 1
        self.ack_started_offsets.add(offset)

    def record_completion(
        self,
        *,
        offset: int,
        key: str,
        request_latency_ns: int,
        completion_elapsed_ns: int,
    ) -> None:
        if offset not in self.active_offsets or offset not in self.ack_started_offsets:
            raise BenchmarkError(
                f"hot-ordering workload completed offset {offset} without an acknowledgement"
            )
        self.active_offsets.remove(offset)
        self.ack_started_offsets.remove(offset)
        self.completed_offsets.add(offset)
        self.completed_offsets_by_key[key].append(offset)
        self.request_latencies_ns.append(request_latency_ns)
        self.request_latency_ns_by_key[key].append(request_latency_ns)
        self.completion_elapsed_ns_by_key[key].append(completion_elapsed_ns)
        if key == self.hot_key:
            self.hot_completed_messages += 1
            if self.hot_completed_messages == self.hot_key_messages:
                self.hot_drained_elapsed_ns = completion_elapsed_ns
            return
        hot_backlog = self.hot_key_messages - self.hot_completed_messages
        if hot_backlog > 0:
            self.hot_backlog_at_cold_completion.append(hot_backlog)
            self.cold_keys_with_progress_while_hot_backlog.add(key)
            if len(self.completed_offsets_by_key[key]) == len(
                self.expected_offsets_by_key[key]
            ):
                self.cold_keys_completed_while_hot_backlog.add(key)


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
        self._last_storage_scan_ns = 0
        self._storage_bytes: dict[int, int] = {}

    def begin(self) -> tuple[int, int, float, int]:
        self._record(force_storage=True)
        with self.lock:
            sample_index = len(self.samples)
            node_sample_index = max(0, len(self.node_samples) - 1)
            cpu_start = self.samples[-1]["cpu_seconds"] if self.samples else 0.0
        return sample_index, node_sample_index, cpu_start, time.perf_counter_ns()

    def end(self, token: tuple[int, int, float, int]) -> dict[str, Any]:
        sample_index, node_sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        self._record(force_storage=True)
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
            summary: dict[str, Any] = {"samples": len(values)}
            memory_values = [
                value["memory_bytes"]
                for value in values
                if isinstance(value.get("memory_bytes"), (int, float))
            ]
            if memory_values:
                summary["memory_bytes_avg"] = sum(memory_values) / len(memory_values)
                summary["memory_bytes_max"] = max(memory_values)
            storage_values = [
                value["storage_bytes"]
                for value in values
                if isinstance(value.get("storage_bytes"), (int, float))
            ]
            if storage_values:
                summary["storage_bytes_avg"] = sum(storage_values) / len(storage_values)
                summary["storage_bytes_max"] = max(storage_values)
            cpu_values = [
                value["cpu_seconds"]
                for value in values
                if isinstance(value.get("cpu_seconds"), (int, float))
            ]
            if cpu_values:
                summary["cpu_seconds"] = max(0.0, cpu_values[-1] - cpu_values[0])
            summaries[node_id] = summary
        return summaries

    def _record(self, *, force_storage: bool = False) -> None:
        cpu = 0.0
        memory = 0
        node_sample: dict[str, dict[str, float]] = {}
        now_ns = time.monotonic_ns()
        scan_storage = force_storage or now_ns - self._last_storage_scan_ns >= 1_000_000_000
        if scan_storage:
            self._last_storage_scan_ns = now_ns
        for node in self.cluster.nodes:
            if scan_storage:
                self._storage_bytes[node.node_id] = directory_size(node.data_dir)
            storage_bytes = self._storage_bytes.get(node.node_id, 0)
            if self.cluster.runtime == "container":
                node_value: dict[str, float] = {}
                if node.container is not None and node.container.created:
                    node_cpu = read_cpu_seconds(node.container.name)
                    sample = read_stats(node.container.name)
                    if sample is not None:
                        node_value["memory_bytes"] = float(sample["memory_bytes"])
                        memory += int(sample["memory_bytes"])
                    if node_cpu is not None:
                        node_value["cpu_seconds"] = node_cpu
                        cpu += node_cpu
                if node.data_dir.exists():
                    node_value["storage_bytes"] = float(storage_bytes)
                if node_value:
                    node_sample[str(node.node_id)] = node_value
                continue
            node_value = {}
            if node.process is not None:
                sample = process_stats(node.process.pid)
                if sample is not None:
                    node_value["cpu_seconds"] = sample[0]
                    node_value["memory_bytes"] = float(sample[1])
                    cpu += sample[0]
                    memory += sample[1]
            if node.data_dir.exists():
                node_value["storage_bytes"] = float(storage_bytes)
            if node_value:
                node_sample[str(node.node_id)] = node_value
        storage = sum(
            int(value.get("storage_bytes", 0)) for value in node_sample.values()
        )
        with self.lock:
            self.samples.append(
                {
                    "cpu_seconds": cpu,
                    "memory_bytes": float(memory),
                    "storage_bytes": float(storage),
                }
            )
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
        peer_response_delay_ms: int = DEFAULT_PEER_RESPONSE_DELAY_MS,
    ) -> None:
        if runtime not in {"process", "container"}:
            raise ValueError(f"unsupported cluster runtime: {runtime}")
        if peer_response_delay_ms and runtime != "process":
            raise ValueError("peer response delay requires the native process runtime")
        self.binary = binary
        self.node_count = node_count
        self.ack_timeout_ms = ack_timeout_ms
        self.runtime = runtime
        self.image = image
        self.cpus = cpus
        self.memory = memory
        self.peer_response_delay_ms = peer_response_delay_ms
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
                peer_address_port=0,
                data_dir=self.root / f"node-{index + 1}",
            )
            for index in range(node_count)
        ]
        for node in self.nodes:
            node.peer_address_port = node.peer_port
            if self.peer_response_delay_ms:
                node.peer_proxy = PeerResponseDelayProxy(
                    node.peer_port, self.peer_response_delay_ms
                )
        self.stats = ProcessStats(self)
        self.startup_ns = 0

    def start(self) -> None:
        if self.runtime == "process" and not self.binary.is_file():
            raise BenchmarkError(f"broker binary does not exist: {self.binary}")
        if self.runtime == "container":
            self._prepare_container_runtime()
        for node in self.nodes:
            if node.peer_proxy is not None:
                node.peer_address_port = node.peer_proxy.port
                node.peer_proxy.start()
        started = time.perf_counter_ns()
        for index in range(self.node_count):
            self._start_node(index, bootstrap=index == 0)
        for node in self.nodes:
            wait_for_ready(node.http_port, timeout_seconds=COMMAND_TIMEOUT_SECONDS)
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def client(
        self, index: int, *, timeout_seconds: float = COMMAND_TIMEOUT_SECONDS
    ) -> LineClient:
        return LineClient(
            "127.0.0.1",
            self.nodes[index % self.node_count].broker_port,
            timeout_seconds,
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
        for node in self.nodes:
            if node.peer_proxy is not None:
                node.peer_proxy.close()
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
                f"{other.node_id}=127.0.0.1:{other.peer_address_port}" for other in self.nodes
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

    def peer_proxy_summary(self) -> dict[str, Any]:
        if not any(node.peer_proxy is not None for node in self.nodes):
            return {
                "enabled": False,
                "response_delay_ms": self.peer_response_delay_ms,
            }
        return {
            "enabled": True,
            "response_delay_ms": self.peer_response_delay_ms,
            "per_node": {
                str(node.node_id): node.peer_proxy.summary()
                for node in self.nodes
                if node.peer_proxy is not None
            },
        }


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


def publish_batch_request(
    client: LineClient,
    stream: str,
    payload: str,
    batch_size: int,
    expected_offset: int,
) -> tuple[int, int]:
    """Publish one public batch and validate every per-record outcome."""
    encoded_payload = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    response, elapsed = client.request(
        {
            "op": "publish_batch",
            "stream": stream,
            "records": [
                {"key": None, "payload_base64": encoded_payload}
                for _ in range(batch_size)
            ],
        }
    )
    if response.get("type") != "publish_batch":
        raise BenchmarkError(f"unexpected publish batch response: {response}")
    outcomes = response.get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != batch_size:
        raise BenchmarkError(
            f"publish batch returned {len(outcomes) if isinstance(outcomes, list) else 'no'} "
            f"outcomes for {batch_size} records"
        )
    offsets: list[int] = []
    for index, outcome in enumerate(outcomes):
        if not isinstance(outcome, dict) or outcome.get("type") != "published":
            raise BenchmarkError(
                f"publish batch record {index} did not publish: {outcome}"
            )
        offset = outcome.get("offset")
        if not isinstance(offset, int) or offset != expected_offset + index:
            raise BenchmarkError(
                f"publish batch returned unexpected offset at record {index}: {outcome}"
            )
        offsets.append(offset)
    return len(offsets), elapsed


def batch_metric(
    operation: str,
    batch_latencies_ns: list[int],
    elapsed_ns: int,
    *,
    messages: int,
    message_size: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """Report record throughput while retaining one latency sample per batch."""
    result = metric(
        operation,
        batch_latencies_ns,
        elapsed_ns,
        message_size=message_size,
        metadata=metadata,
    )
    elapsed_seconds = elapsed_ns / 1_000_000_000
    result["messages"] = messages
    result["throughput_messages_per_second"] = messages / elapsed_seconds
    result["throughput_megabytes_per_second"] = (
        messages * message_size / elapsed_seconds / 1_000_000
    )
    return result


def run_publish_batch(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    warmup: int,
    batch_size: int,
) -> dict[str, Any]:
    """Measure clustered public publish_batch round trips and outcomes."""
    setup = cluster.client(0)
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()

    with cluster.connected_clients() as clients:
        def operation() -> dict[str, Any]:
            batch_latencies: list[int] = []
            published_messages = 0
            started = time.perf_counter_ns()
            while published_messages < messages:
                current_batch_size = min(batch_size, messages - published_messages)
                client = clients[len(batch_latencies) % len(clients)]
                published, elapsed = publish_batch_request(
                    client,
                    stream,
                    payload,
                    current_batch_size,
                    warmup + published_messages,
                )
                published_messages += published
                batch_latencies.append(elapsed)
            return batch_metric(
                "cluster_publish_batch",
                batch_latencies,
                time.perf_counter_ns() - started,
                messages=published_messages,
                message_size=len(payload),
                metadata={
                    "nodes": cluster.node_count,
                    "batch_size": batch_size,
                    "batches": len(batch_latencies),
                    "any_node_routing": True,
                    "setup_excluded": True,
                    "outcome_scope": "every input record returned a contiguous published outcome",
                    "latency_scope": "one public publish_batch roundtrip per sample",
                    "latency_sample_scope": "batch_requests",
                },
            )

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


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


def hot_ordering_records(
    hot_key_messages: int,
    cold_key_count: int,
    cold_messages_per_key: int,
    *,
    hot_key: str = HOT_ORDERING_HOT_KEY,
) -> list[tuple[int, str]]:
    """Build the deterministic interleaved record schedule for the probe."""
    if hot_key_messages <= 0 or cold_key_count <= 0 or cold_messages_per_key <= 0:
        raise BenchmarkError("hot-ordering workload dimensions must be positive")
    total_messages = hot_key_messages + cold_key_count * cold_messages_per_key
    if total_messages > MAX_HOT_ORDERING_MESSAGES:
        raise BenchmarkError(
            "hot-ordering workload exceeds the bounded maximum of "
            f"{MAX_HOT_ORDERING_MESSAGES} records"
        )
    records: list[tuple[int, str]] = []
    offset = 0
    rounds = max(hot_key_messages, cold_messages_per_key)
    for round_index in range(rounds):
        if round_index < hot_key_messages:
            records.append((offset, hot_key))
            offset += 1
        if round_index < cold_messages_per_key:
            for cold_key_index in range(cold_key_count):
                records.append((offset, f"cold-key-{cold_key_index}"))
                offset += 1
    return records


def publish_keyed(
    client: LineClient,
    stream: str,
    key: str,
    payload: str,
    expected_offset: int,
) -> tuple[int, int]:
    """Publish one keyed record through the public protocol and check its offset."""
    response, elapsed = request_ok(
        client,
        {"op": "publish", "stream": stream, "key": key, "payload": payload},
        "published",
    )
    offset = response.get("offset")
    if offset != expected_offset:
        raise BenchmarkError(
            f"hot-ordering setup returned offset {offset}, expected {expected_offset}"
        )
    return int(offset), elapsed


def _duration_summary_milliseconds(values_ns: list[int]) -> dict[str, Any]:
    if not values_ns:
        return {"samples": 0}
    return {
        "samples": len(values_ns),
        "p50_milliseconds": percentile(values_ns, 50) / 1_000_000,
        "p99_milliseconds": percentile(values_ns, 99) / 1_000_000,
        "max_milliseconds": max(values_ns) / 1_000_000,
    }


def _hot_ordering_metadata(
    observation: HotOrderingObservation,
    *,
    records: list[tuple[int, str]],
    cluster: Cluster,
    concurrency: int,
    processing_delay_ms: int,
    timeout_seconds: float,
    operation_elapsed_ns: int,
) -> dict[str, Any]:
    expected_offsets = observation.expected_offsets_by_key
    key_metrics: dict[str, Any] = {}
    strict_delivery_order_verified = True
    strict_completion_order_verified = True
    same_key_exclusion_verified = True
    for key, expected in expected_offsets.items():
        observed = observation.delivered_offsets_by_key[key]
        completed = observation.completed_offsets_by_key[key]
        ordering_verified = observed == expected
        strict_delivery_order_verified &= ordering_verified
        completion_order_verified = completed == expected
        strict_completion_order_verified &= completion_order_verified
        same_key_exclusion_verified &= observation.max_in_flight_by_key[key] <= 1
        completions = observation.completion_elapsed_ns_by_key[key]
        key_metrics[key] = {
            "role": "hot" if key == observation.hot_key else "cold",
            "messages": len(expected),
            "delivered_messages": len(observed),
            "completed_messages": len(completions),
            "expected_offsets": expected,
            "observed_delivery_offsets": observed,
            "delivery_order_verified": ordering_verified,
            "observed_completion_offsets": completed,
            "completion_order_verified": completion_order_verified,
            "same_key_processing_overlap_verified": (
                observation.max_in_flight_by_key[key] <= 1
            ),
            "delivery_wait": _duration_summary_milliseconds(
                observation.delivery_wait_ns_by_key[key]
            ),
            "request_latency": _duration_summary_milliseconds(
                observation.request_latency_ns_by_key[key]
            ),
            "completion_elapsed": _duration_summary_milliseconds(completions),
            "first_completion_elapsed_milliseconds": (
                min(completions) / 1_000_000 if completions else None
            ),
            "last_completion_elapsed_milliseconds": (
                max(completions) / 1_000_000 if completions else None
            ),
            "max_in_flight": observation.max_in_flight_by_key[key],
        }

    cold_keys = [key for key in expected_offsets if key != observation.hot_key]
    cold_first_completions = [
        min(observation.completion_elapsed_ns_by_key[key])
        for key in cold_keys
        if observation.completion_elapsed_ns_by_key[key]
    ]
    cold_last_completions = [
        max(observation.completion_elapsed_ns_by_key[key])
        for key in cold_keys
        if observation.completion_elapsed_ns_by_key[key]
    ]
    cold_first_completion_spread_ms = (
        (max(cold_first_completions) - min(cold_first_completions)) / 1_000_000
        if cold_first_completions
        else None
    )
    cold_last_completion_spread_ms = (
        (max(cold_last_completions) - min(cold_last_completions)) / 1_000_000
        if cold_last_completions
        else None
    )
    hot_backlog_at_cold_completion = observation.hot_backlog_at_cold_completion
    return {
        "nodes": cluster.node_count,
        "records": len(records),
        "hot_key": observation.hot_key,
        "hot_key_messages": observation.hot_key_messages,
        "cold_key_count": len(cold_keys),
        "cold_messages": sum(len(expected_offsets[key]) for key in cold_keys),
        "configured_workers": concurrency,
        "hot_key_processing_delay_ms": processing_delay_ms,
        "bounded_runtime_seconds": timeout_seconds,
        "mixed_workload_schedule": (
            "each round publishes one hot-key record followed by one record for "
            "each cold key; all records are preloaded before measurement"
        ),
        "setup_excluded": True,
        "redelivery_expected": False,
        "latency_scope": (
            "poll_and_ack_request_time_excludes_the_configured_processing_delay"
        ),
        "throughput_scope": (
            "concurrent_preloaded_grouped_backlog_drain_includes_processing_delay"
        ),
        "per_key_ordering": {
            "verified": (
                strict_delivery_order_verified
                and strict_completion_order_verified
                and same_key_exclusion_verified
            ),
            "verification": (
                "observed delivery and acknowledgement-completion offsets exactly "
                "match published offsets per key"
            ),
            "expected_offsets_by_key": expected_offsets,
            "observed_delivery_offsets_by_key": observation.delivered_offsets_by_key,
            "observed_completion_offsets_by_key": observation.completed_offsets_by_key,
            "delivery_order_verified": strict_delivery_order_verified,
            "completion_order_verified": strict_completion_order_verified,
            "same_key_processing_overlap_verified": same_key_exclusion_verified,
        },
        "key_metrics": key_metrics,
        "hot_key_backlog": {
            "definition": "preloaded hot-key records not yet durably acknowledged",
            "initial_messages": observation.hot_key_messages,
            "peak_messages": max(
                observation.hot_backlog_at_delivery,
                default=observation.hot_key_messages,
            ),
            "samples_at_hot_delivery": observation.hot_backlog_at_delivery,
            "at_first_cold_completion": (
                hot_backlog_at_cold_completion[0]
                if hot_backlog_at_cold_completion
                else None
            ),
            "at_last_cold_completion": (
                hot_backlog_at_cold_completion[-1]
                if hot_backlog_at_cold_completion
                else None
            ),
            "samples_at_cold_completion": hot_backlog_at_cold_completion,
            "drained": observation.hot_completed_messages
            == observation.hot_key_messages,
            "drained_elapsed_milliseconds": (
                observation.hot_drained_elapsed_ns / 1_000_000
                if observation.hot_drained_elapsed_ns is not None
                else None
            ),
        },
        "unrelated_key_progress": {
            "definition": "cold-key acknowledgements completed while any hot-key record remained unacknowledged",
            "cold_messages_completed_before_hot_drained": len(
                hot_backlog_at_cold_completion
            ),
            "cold_keys_with_progress_before_hot_drained": len(
                observation.cold_keys_with_progress_while_hot_backlog
            ),
            "cold_keys_with_progress_before_hot_drained_names": sorted(
                observation.cold_keys_with_progress_while_hot_backlog
            ),
            "cold_keys_completed_before_hot_drained": len(
                observation.cold_keys_completed_while_hot_backlog
            ),
            "cold_keys_completed_before_hot_drained_names": sorted(
                observation.cold_keys_completed_while_hot_backlog
            ),
            "cold_key_first_completion_spread_milliseconds": cold_first_completion_spread_ms,
            "cold_key_last_completion_spread_milliseconds": cold_last_completion_spread_ms,
            "fairness": {
                "definition": (
                    "descriptive spread of first and last completion times across "
                    "cold keys; lower spread indicates closer timing"
                ),
                "first_completion_spread_milliseconds": cold_first_completion_spread_ms,
                "last_completion_spread_milliseconds": cold_last_completion_spread_ms,
            },
        },
        "delivery_concurrency": {
            "max_processing_in_flight_messages": observation.max_in_flight_messages,
            "max_processing_in_flight_by_key": observation.max_in_flight_by_key,
            "interpretation": (
                "observed client-side processing slots from delivery through the "
                "start of the acknowledgement request; it does not claim a broker "
                "scheduling policy"
            ),
        },
        "resource_measurement": {
            "scope": (
                "scenario resource_samples cover the grouped poll/ack drain, "
                "including configured hot-key processing delay"
            ),
            "dimensions": [
                "resource_samples.cpu_seconds",
                "resource_samples.memory_bytes_avg",
                "resource_samples.memory_bytes_max",
                "resource_samples.storage_bytes_avg",
                "resource_samples.storage_bytes_max",
                "resource_samples.per_node.*",
            ],
            "server_metrics": "scenario-scoped GET /metrics delta when all node endpoints are available",
        },
        "scheduling_observation": (
            "worker scheduling and timing are runtime-dependent; this result does "
            "not imply adaptive scheduling or prove a performance improvement"
        ),
        "operation_elapsed_milliseconds": operation_elapsed_ns / 1_000_000,
    }


def run_hot_ordering(
    cluster: Cluster,
    stream: str,
    payload: str,
    *,
    hot_key_messages: int,
    cold_key_count: int,
    cold_messages_per_key: int,
    concurrency: int,
    processing_delay_ms: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Measure a bounded mixed-key grouped-consumer drain through the public protocol."""
    records = hot_ordering_records(
        hot_key_messages,
        cold_key_count,
        cold_messages_per_key,
    )
    setup = cluster.client(0)
    try:
        create_stream(setup, stream)
        for offset, key in records:
            publish_keyed(setup, stream, key, payload, offset)
    finally:
        setup.close()

    total_messages = len(records)
    client_timeout = min(COMMAND_TIMEOUT_SECONDS, timeout_seconds)
    stop_event = threading.Event()
    observation_lock = threading.Lock()

    def operation() -> dict[str, Any]:
        observation = HotOrderingObservation.for_records(
            records, HOT_ORDERING_HOT_KEY
        )
        operation_started_ns = time.perf_counter_ns()
        deadline = time.monotonic() + timeout_seconds

        def worker(worker_index: int) -> None:
            client = cluster.client(
                worker_index % cluster.node_count,
                timeout_seconds=client_timeout,
            )
            member = f"hot-ordering-member-{worker_index}"
            try:
                while not stop_event.is_set():
                    with observation_lock:
                        if len(observation.completed_offsets) >= total_messages:
                            return
                    if time.monotonic() >= deadline:
                        raise BenchmarkError(
                            "hot-ordering benchmark exceeded its bounded runtime"
                        )
                    response, poll_elapsed = client.request(
                        {
                            "op": "poll_group",
                            "stream": stream,
                            "consumer": "hot-ordering-workers",
                            "member": member,
                        }
                    )
                    if response.get("type") == "empty":
                        remaining = deadline - time.monotonic()
                        if remaining > 0:
                            time.sleep(min(0.001, remaining))
                        continue
                    if response.get("type") != "message":
                        raise BenchmarkError(
                            f"unexpected hot-ordering poll response: {response}"
                        )
                    offset = response.get("offset")
                    key = response.get("key")
                    token = response.get("delivery_token")
                    delivery_attempt = response.get("delivery_attempt")
                    if not isinstance(offset, int) or not isinstance(key, str):
                        raise BenchmarkError(
                            f"hot-ordering poll omitted offset or key: {response}"
                        )
                    if not isinstance(token, str) or not isinstance(delivery_attempt, int):
                        raise BenchmarkError(
                            f"hot-ordering poll omitted delivery fencing fields: {response}"
                        )
                    if response.get("payload") != payload:
                        raise BenchmarkError(
                            f"hot-ordering poll returned an unexpected payload at offset {offset}"
                        )
                    delivered_ns = time.perf_counter_ns()
                    with observation_lock:
                        observation.record_delivery(
                            offset=offset,
                            key=key,
                            delivery_attempt=delivery_attempt,
                            delivery_wait_ns=delivered_ns - operation_started_ns,
                        )
                    if key == HOT_ORDERING_HOT_KEY and processing_delay_ms:
                        time.sleep(processing_delay_ms / 1_000)
                    with observation_lock:
                        observation.record_ack_start(offset=offset, key=key)
                    ack_elapsed = acknowledge_group(
                        client,
                        stream,
                        "hot-ordering-workers",
                        member,
                        offset,
                        token,
                    )
                    completed_ns = time.perf_counter_ns()
                    with observation_lock:
                        observation.record_completion(
                            offset=offset,
                            key=key,
                            request_latency_ns=poll_elapsed + ack_elapsed,
                            completion_elapsed_ns=completed_ns - operation_started_ns,
                        )
            except BaseException:
                stop_event.set()
                raise
            finally:
                client.close()

        try:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(worker, worker_index)
                    for worker_index in range(concurrency)
                ]
                for future in futures:
                    future.result()
        finally:
            stop_event.set()

        if len(observation.completed_offsets) != total_messages:
            raise BenchmarkError(
                "hot-ordering benchmark processed "
                f"{len(observation.completed_offsets)} of {total_messages} messages"
            )
        if observation.active_offsets:
            raise BenchmarkError(
                "hot-ordering benchmark ended with active offsets: "
                f"{sorted(observation.active_offsets)}"
            )
        if observation.processing_offsets or observation.ack_started_offsets:
            raise BenchmarkError(
                "hot-ordering benchmark ended with incomplete client acknowledgement state"
            )
        metadata = _hot_ordering_metadata(
            observation,
            records=records,
            cluster=cluster,
            concurrency=concurrency,
            processing_delay_ms=processing_delay_ms,
            timeout_seconds=timeout_seconds,
            operation_elapsed_ns=time.perf_counter_ns() - operation_started_ns,
        )
        return metric(
            "cluster_hot_ordering",
            observation.request_latencies_ns,
            time.perf_counter_ns() - operation_started_ns,
            message_size=len(payload),
            metadata=metadata,
        )

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_peer_forwarding(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    warmup: int,
    concurrency: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Publish through a follower to exercise the topology-free peer pool.

    The setup is sent through the bootstrap node and excluded from the
    measured interval. Measured publishes use persistent public clients on a
    different node, so the Raft engine must forward each operation over its
    shared topology-free peer lane. Concurrency above the current four shared
    permits queues behind that lane; optional peer-response delay is applied by
    the run-scoped native proxy configured on ``Cluster``.
    """
    setup = cluster.client(0)
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()

    latencies: list[int] = []
    offsets: list[int] = []
    lock = threading.Lock()
    deadline = time.monotonic() + timeout_seconds
    ingress_index = PEER_FORWARDING_INGRESS_NODE_INDEX
    client_timeout = min(COMMAND_TIMEOUT_SECONDS, timeout_seconds)

    def worker(worker_index: int) -> None:
        client = cluster.client(ingress_index, timeout_seconds=client_timeout)
        try:
            for message_index in range(worker_index, messages, concurrency):
                if time.monotonic() >= deadline:
                    raise BenchmarkError(
                        "peer forwarding benchmark exceeded its bounded runtime"
                    )
                published, elapsed = publish(client, stream, payload)
                with lock:
                    offsets.append(published)
                    latencies.append(elapsed)
        finally:
            client.close()

    proxy_summary = cluster.peer_proxy_summary()

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, index) for index in range(concurrency)]
            for future in futures:
                future.result()
        ordered_offsets = sorted(offsets)
        if len(ordered_offsets) != messages or any(
            offset != warmup + index for index, offset in enumerate(ordered_offsets)
        ):
            raise BenchmarkError(
                "peer forwarding returned non-contiguous offsets: "
                f"received {len(ordered_offsets)} of {messages}"
            )
        result = metric(
            "cluster_peer_forwarding",
            latencies,
            time.perf_counter_ns() - started,
            message_size=len(payload),
            metadata={
                "nodes": cluster.node_count,
                "forwarded_operation": "publish",
                "forwarding_ingress_node": ingress_index + 1,
                "forwarding_target": "data-group leader selected by the cluster",
                "concurrency": concurrency,
                "warmup": warmup,
                "peer_response_delay_ms": cluster.peer_response_delay_ms,
                "peer_response_proxy_enabled": proxy_summary["enabled"],
                "latency_scope": (
                    "follower_public_publish_roundtrip_includes_peer_forwarding_and_pool_wait"
                ),
                "setup_excluded": True,
                "saturation_scope": (
                    "shared_forwarding_lane_queues_when_concurrency_exceeds_current_four_per_peer"
                ),
                "bounded_runtime_seconds": timeout_seconds,
            },
        )
        return result

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


def request_until_response(
    cluster: Cluster,
    node_index: int,
    request: dict[str, Any],
    response_type: str,
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], int, int]:
    """Retry a public request while a bounded leader transition is in progress."""
    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts += 1
        client: LineClient | None = None
        try:
            client = cluster.client(
                node_index, timeout_seconds=min(COMMAND_TIMEOUT_SECONDS, remaining)
            )
            response, elapsed = client.request(request)
            if response.get("type") == response_type:
                return response, elapsed, attempts
            last_error = BenchmarkError(
                f"unexpected response to {request.get('op')}: {response}"
            )
        except (BenchmarkError, OSError, TimeoutError) as error:
            last_error = error
        finally:
            if client is not None:
                try:
                    client.close()
                except OSError:
                    pass
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.05, remaining))
    node_id = cluster.nodes[node_index].node_id
    raise BenchmarkError(
        f"node {node_id} did not return {response_type} for {request.get('op')} "
        f"before the {timeout_seconds:g}-second deadline after {attempts} attempts; "
        f"last error: {last_error}"
    )


def validate_leader_failure_message(
    response: dict[str, Any], expected_offset: int, payload: str, phase: str
) -> None:
    if response.get("offset") != expected_offset:
        raise BenchmarkError(
            f"{phase} returned offset {response.get('offset')}, expected {expected_offset}"
        )
    if response.get("payload") != payload:
        raise BenchmarkError(f"{phase} returned an unexpected payload")


def run_node_failure_recovery(
    cluster: Cluster,
    stream: str,
    payload: str,
    *,
    failed_index: int,
    failure_kind: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Exercise one bounded process-stop and same-node restart.

    The public protocol does not expose leader identity, so the leader probe
    keeps its bootstrap assumption. The follower probe uses the same public
    sequence with a non-bootstrap node, separating process recovery from
    leader-election observation without adding broker-specific controls.
    """
    if failure_kind not in {"leader", "follower"}:
        raise BenchmarkError(f"unsupported failure kind: {failure_kind}")
    if not 0 <= failed_index < cluster.node_count:
        raise BenchmarkError(f"failure node index is outside the cluster: {failed_index}")
    survivor_indices = [index for index in range(cluster.node_count) if index != failed_index]
    if len(survivor_indices) < 2:
        raise BenchmarkError("node failure recovery requires at least two surviving nodes")

    setup_index = failed_index if failure_kind == "leader" else 0
    setup = cluster.client(setup_index)
    try:
        create_stream(setup, stream)
        pre_failure_offset, _ = publish(setup, stream, payload)
    finally:
        setup.close()
    if pre_failure_offset != 0:
        raise BenchmarkError(
            f"{failure_kind} failure recovery setup returned offset {pre_failure_offset}, "
            "expected 0"
        )

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        cluster.stop_node(failed_index)

        attempts: dict[str, int] = {}
        consumer = f"{failure_kind}-failure-consumer"
        request_prefix = (
            stream if failure_kind == "leader" else f"follower-failure-{stream}"
        )
        failure_phase = f"{failure_kind}-failure"
        response, _, attempts[f"publish_after_{failure_kind}_failure"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "publish",
                "stream": stream,
                "payload": payload,
                "request_id": f"{request_prefix}-after-{failure_phase}",
            },
            "published",
            timeout_seconds=timeout_seconds,
        )
        if response.get("offset") != 1:
            raise BenchmarkError(
                f"publish after {failure_kind} failure returned {response}, expected offset 1"
            )

        response, _, attempts["poll_before_restart"] = request_until_response(
            cluster,
            survivor_indices[0],
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 0, payload, "survivor poll")
        _, _, attempts["ack_before_restart"] = request_until_response(
            cluster,
            survivor_indices[1],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 0,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        response, _, attempts["poll_second_survivor"] = request_until_response(
            cluster,
            survivor_indices[1],
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 1, payload, "second survivor poll")
        _, _, attempts["ack_second_survivor"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 1,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        restart_ns = cluster.restart_node(failed_index)
        response, _, attempts["publish_after_restart"] = request_until_response(
            cluster,
            failed_index,
            {
                "op": "publish",
                "stream": stream,
                "payload": payload,
                "request_id": f"{request_prefix}-after-node-restart",
            },
            "published",
            timeout_seconds=timeout_seconds,
        )
        if response.get("offset") != 2:
            raise BenchmarkError(
                f"publish after node restart returned {response}, expected offset 2"
            )
        response, _, attempts["poll_after_restart"] = request_until_response(
            cluster,
            failed_index,
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 2, payload, "restarted node poll")
        _, _, attempts["ack_after_restart"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 2,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            f"cluster_{failure_kind}_failure_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "nodes": cluster.node_count,
                "failed_node": cluster.nodes[failed_index].node_id,
                "surviving_nodes": [cluster.nodes[index].node_id for index in survivor_indices],
                "failure_state": f"{failure_kind}_process_stop",
                "failed_node_role": failure_kind,
                "initial_leader_selection": (
                    "bootstrap_assumption"
                    if failure_kind == "leader"
                    else "not_required_for_follower_probe"
                ),
                "initial_leader_node": (
                    cluster.nodes[failed_index].node_id
                    if failure_kind == "leader"
                    else None
                ),
                "initial_leader_basis": (
                    "node 1 is the only process started with --bootstrap; the current "
                    "PersistentEngine uses that process to initialize the static metadata "
                    "group before data-stream creation"
                    if failure_kind == "leader"
                    else "follower probe does not require identifying the current leader"
                ),
                "replacement_leader_observed": failure_kind == "leader",
                "replacement_leader_identity": (
                    "not exposed by the provisional public protocol"
                    if failure_kind == "leader"
                    else "not applicable"
                ),
                "replacement_observation": (
                    "both surviving public endpoints committed, consumed, and acknowledged "
                    "records after the failed node stopped"
                ),
                "public_protocol_survivor_nodes": [
                    cluster.nodes[index].node_id for index in survivor_indices[:2]
                ],
                "post_failure_publish_offset": 1,
                "post_failure_consumed_offsets": [0, 1],
                "post_restart_publish_offset": 2,
                "restart_recovered_message_offset": 2,
                "fault_sequence_messages": 3,
                "verified_message_count": 3,
                "metrics_counter_reset_on_restart_expected": True,
                "verified": {
                    "surviving_nodes_elected_and_served": failure_kind == "leader",
                    "surviving_nodes_served": True,
                    "publish_after_failure": True,
                    "consume_after_failure": True,
                    "ack_after_failure": True,
                    "publish_after_leader_failure": True,
                    "consume_after_leader_failure": True,
                    "ack_after_leader_failure": True,
                    "stopped_node_restarted": True,
                    "restarted_node_served_and_recovered": True,
                },
                "setup_excluded": True,
                "request_identity_for_retried_publishes": "stable request_id",
                "bounded_timeout_seconds": timeout_seconds,
                "latency_scope": (
                    "stopped-bootstrap-leader-through-survivor-failover-and-restarted-node-ack"
                    if failure_kind == "leader"
                    else "stopped-follower-through-survivor-service-and-restarted-node-ack"
                ),
                "failure_scope": (
                    f"one {failure_kind} process stop in a static quorum followed by same-process restart; "
                    "network partitions, storage loss, and membership changes are excluded"
                ),
                "request_attempts": attempts,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_leader_failure_recovery(
    cluster: Cluster, stream: str, payload: str, timeout_seconds: float
) -> dict[str, Any]:
    """Exercise one bounded bootstrap-leader failure through the public protocol."""
    return run_node_failure_recovery(
        cluster,
        stream,
        payload,
        failed_index=0,
        failure_kind="leader",
        timeout_seconds=timeout_seconds,
    )


def run_follower_failure_recovery(
    cluster: Cluster, stream: str, payload: str, timeout_seconds: float
) -> dict[str, Any]:
    """Exercise one bounded non-bootstrap follower failure through the public protocol."""
    return run_node_failure_recovery(
        cluster,
        stream,
        payload,
        failed_index=1,
        failure_kind="follower",
        timeout_seconds=timeout_seconds,
    )


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


def parse_scenarios(value: str) -> list[str]:
    scenarios = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(scenarios) - set(SCENARIO_NAMES))
    if not scenarios:
        raise argparse.ArgumentTypeError("scenarios cannot be empty")
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown scenario(s): {', '.join(unknown)}; choose from {', '.join(SCENARIO_NAMES)}"
        )
    if len(scenarios) != len(set(scenarios)):
        raise argparse.ArgumentTypeError("scenarios must not contain duplicates")
    return scenarios


def parse_positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


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
    parser.add_argument(
        "--scenarios",
        type=parse_scenarios,
        default=list(DEFAULT_SCENARIOS),
        help=(
            "comma-separated scenarios to run (default: existing clustered workload; "
            "add peer_forwarding, publish_batch, hot_ordering, leader_failure_recovery, "
            "or follower_failure_recovery explicitly for focused probes)"
        ),
    )
    parser.add_argument("--ack-timeout-ms", type=int, default=DEFAULT_ACK_TIMEOUT_MS)
    parser.add_argument(
        "--slow-consumer-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_SLOW_CONSUMER_DELAY_MS,
        help="fixed processing delay before each slow-consumer acknowledgement",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_PUBLISH_BATCH_SIZE,
        help="records per publish_batch request for the opt-in publish_batch scenario",
    )
    parser.add_argument(
        "--hot-key-messages",
        type=int,
        default=DEFAULT_HOT_KEY_MESSAGES,
        help="preloaded records for the hot key in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--cold-key-count",
        type=int,
        default=DEFAULT_COLD_KEY_COUNT,
        help="number of independent cold keys in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--cold-messages-per-key",
        type=int,
        default=DEFAULT_COLD_MESSAGES_PER_KEY,
        help="preloaded records per cold key in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--hot-ordering-concurrency",
        type=int,
        default=DEFAULT_HOT_ORDERING_CONCURRENCY,
        help="grouped workers for the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--hot-key-processing-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
        help="processing delay before acknowledging hot-key records",
    )
    parser.add_argument(
        "--hot-ordering-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for each hot_ordering scenario",
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
    parser.add_argument(
        "--peer-forwarding-concurrency",
        type=int,
        default=DEFAULT_PEER_FORWARDING_CONCURRENCY,
        help="concurrent persistent follower clients for the peer-forwarding scenario",
    )
    parser.add_argument(
        "--peer-response-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_PEER_RESPONSE_DELAY_MS,
        help=(
            "delay every framed peer response in the native-only focused probe; "
            "zero keeps direct peer connections"
        ),
    )
    parser.add_argument(
        "--peer-forwarding-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for each peer-forwarding scenario",
    )
    parser.add_argument(
        "--leader-failure-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for the opt-in leader-failure scenario",
    )
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument(
        "--skip-recovery",
        action="store_true",
        help="skip restart and failure-recovery scenarios",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-dir", type=Path)
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3 or args.concurrency <= 0:
        parser.error("messages, nodes, and concurrency must be positive; at least three nodes are required")
    if args.peer_forwarding_concurrency <= 0:
        parser.error("peer forwarding concurrency must be positive")
    if args.peer_forwarding_concurrency > MAX_PEER_FORWARDING_CONCURRENCY:
        parser.error(
            "peer forwarding concurrency exceeds the bounded maximum "
            f"of {MAX_PEER_FORWARDING_CONCURRENCY}"
        )
    if args.batch_size <= 0 or args.batch_size > MAX_PUBLISH_BATCH_SIZE:
        parser.error(
            f"batch size must be between 1 and {MAX_PUBLISH_BATCH_SIZE} records"
        )
    if args.hot_key_messages <= 0:
        parser.error("hot-key messages must be positive")
    if args.cold_key_count <= 0:
        parser.error("cold-key count must be positive")
    if args.cold_messages_per_key <= 0:
        parser.error("cold messages per key must be positive")
    if args.hot_ordering_concurrency <= 0:
        parser.error("hot-ordering concurrency must be positive")
    if args.hot_ordering_concurrency > MAX_HOT_ORDERING_CONCURRENCY:
        parser.error(
            "hot-ordering concurrency exceeds the bounded maximum "
            f"of {MAX_HOT_ORDERING_CONCURRENCY}"
        )
    hot_ordering_messages = args.hot_key_messages + (
        args.cold_key_count * args.cold_messages_per_key
    )
    if hot_ordering_messages > MAX_HOT_ORDERING_MESSAGES:
        parser.error(
            "hot-ordering workload exceeds the bounded maximum of "
            f"{MAX_HOT_ORDERING_MESSAGES} records"
        )
    if args.hot_key_processing_delay_ms > MAX_HOT_KEY_PROCESSING_DELAY_MS:
        parser.error(
            "hot-key processing delay exceeds the bounded maximum "
            f"of {MAX_HOT_KEY_PROCESSING_DELAY_MS} ms"
        )
    if args.hot_ordering_timeout_seconds > MAX_HOT_ORDERING_TIMEOUT_SECONDS:
        parser.error(
            "hot-ordering timeout exceeds the bounded maximum "
            f"of {MAX_HOT_ORDERING_TIMEOUT_SECONDS:g} seconds"
        )
    if "hot_ordering" in args.scenarios and args.hot_ordering_concurrency < 2:
        parser.error("hot-ordering scenario requires at least two grouped workers")
    if (
        "hot_ordering" in args.scenarios
        and args.hot_key_processing_delay_ms >= args.ack_timeout_ms
    ):
        parser.error(
            "hot-key processing delay must be shorter than the acknowledgement timeout"
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
    if args.leader_failure_timeout_seconds > MAX_LEADER_FAILURE_TIMEOUT_SECONDS:
        parser.error(
            "leader failure timeout exceeds the bounded maximum "
            f"of {MAX_LEADER_FAILURE_TIMEOUT_SECONDS:g} seconds"
        )
    if args.peer_response_delay_ms and args.runtime != "process":
        parser.error("peer response delay requires the native process runtime")
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
        peer_response_delay_ms=args.peer_response_delay_ms,
    )
    scenarios: list[dict[str, Any]] = []
    selected_scenarios = set(args.scenarios)
    try:
        cluster.start()
        for size in args.payload_sizes:
            payload = "x" * size
            if "durable_publish" in selected_scenarios:
                scenarios.append(
                    run_durable_publish(
                        cluster,
                        f"cluster_{run_id}_publish_{size}",
                        payload,
                        args.messages,
                        args.warmup,
                    )
                )
            if "publish_batch" in selected_scenarios:
                scenarios.append(
                    run_publish_batch(
                        cluster,
                        f"cluster_{run_id}_publish_batch_{size}",
                        payload,
                        args.messages,
                        args.warmup,
                        args.batch_size,
                    )
                )
            if "consume_ack" in selected_scenarios:
                scenarios.append(
                    run_consume_ack(
                        cluster,
                        f"cluster_{run_id}_consume_{size}",
                        payload,
                        args.messages,
                    )
                )
            if "slow_consumer" in selected_scenarios:
                scenarios.append(
                    run_slow_consumer(
                        cluster,
                        f"cluster_{run_id}_slow_consumer_{size}",
                        payload,
                        args.messages,
                        args.slow_consumer_delay_ms,
                    )
                )
            if "grouped_consume_ack" in selected_scenarios:
                scenarios.append(
                    run_grouped_consume_ack(
                        cluster,
                        f"cluster_{run_id}_grouped_{size}",
                        payload,
                        args.messages,
                    )
                )
            if "parallel_grouped_consume_ack" in selected_scenarios:
                scenarios.append(
                    run_parallel_grouped(
                        cluster,
                        f"cluster_{run_id}_parallel_{size}",
                        payload,
                        args.messages,
                        args.concurrency,
                    )
                )
            if "hot_ordering" in selected_scenarios:
                scenarios.append(
                    run_hot_ordering(
                        cluster,
                        f"cluster_{run_id}_hot_ordering_{size}",
                        payload,
                        hot_key_messages=args.hot_key_messages,
                        cold_key_count=args.cold_key_count,
                        cold_messages_per_key=args.cold_messages_per_key,
                        concurrency=args.hot_ordering_concurrency,
                        processing_delay_ms=args.hot_key_processing_delay_ms,
                        timeout_seconds=args.hot_ordering_timeout_seconds,
                    )
                )
            if "peer_forwarding" in selected_scenarios:
                scenarios.append(
                    run_peer_forwarding(
                        cluster,
                        f"cluster_{run_id}_peer_forwarding_{size}",
                        payload,
                        args.messages,
                        args.warmup,
                        args.peer_forwarding_concurrency,
                        args.peer_forwarding_timeout_seconds,
                    )
                )
            if (
                not args.skip_recovery
                and size == args.payload_sizes[0]
            ):
                if "restart_recovery" in selected_scenarios:
                    scenarios.append(
                        run_restart_recovery(
                            cluster, f"cluster_{run_id}_recovery_{size}", payload
                        )
                    )
                if "cluster_retained_recovery" in selected_scenarios:
                    scenarios.append(
                        run_retained_recovery(
                            cluster,
                            f"cluster_{run_id}_retained_recovery_{size}",
                            payload,
                            args.retained_messages,
                        )
                    )
                if "leader_failure_recovery" in selected_scenarios:
                    scenarios.append(
                        run_leader_failure_recovery(
                            cluster,
                            f"cluster_{run_id}_leader_failure_{size}",
                            payload,
                            args.leader_failure_timeout_seconds,
                        )
                    )
                if "follower_failure_recovery" in selected_scenarios:
                    scenarios.append(
                        run_follower_failure_recovery(
                            cluster,
                            f"cluster_{run_id}_follower_failure_{size}",
                            payload,
                            args.leader_failure_timeout_seconds,
                        )
                    )
    finally:
        cluster.close()

    startup_seconds = cluster.startup_ns / 1_000_000_000
    workload = {
        "messages": args.messages,
        "warmup": args.warmup,
        "concurrency": args.concurrency,
        "scenarios": args.scenarios,
        "nodes": args.nodes,
        "ack_timeout_ms": args.ack_timeout_ms,
        "slow_consumer_delay_ms": args.slow_consumer_delay_ms,
        "batch_size": args.batch_size,
        "hot_ordering": {
            "hot_key_messages": args.hot_key_messages,
            "cold_key_count": args.cold_key_count,
            "cold_messages_per_key": args.cold_messages_per_key,
            "concurrency": args.hot_ordering_concurrency,
            "hot_key_processing_delay_ms": args.hot_key_processing_delay_ms,
            "timeout_seconds": args.hot_ordering_timeout_seconds,
            "max_records": MAX_HOT_ORDERING_MESSAGES,
        },
        "peer_forwarding_concurrency": args.peer_forwarding_concurrency,
        "peer_response_delay_ms": args.peer_response_delay_ms,
        "peer_forwarding_timeout_seconds": args.peer_forwarding_timeout_seconds,
        "leader_failure_timeout_seconds": args.leader_failure_timeout_seconds,
        "payload_sizes_bytes": args.payload_sizes,
        "runtime": args.runtime,
        "protocol": "line-delimited JSON with UTF-8 string payloads",
        "protocol_version": "provisional-line-json-v1",
        "payload_encoding": "utf-8",
        "compression": "none",
        "durability": "committed by the current three-node Raft quorum and local durable state",
    }
    if not args.skip_recovery and "cluster_retained_recovery" in selected_scenarios:
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
                "peer_response_proxy": cluster.peer_proxy_summary(),
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
