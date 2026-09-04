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
import math
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import (
    BenchmarkError,
    DEFAULT_TIMEOUT_SECONDS,
    LineClient,
    ROOT,
    build_image,
    default_binary,
    parse_nonnegative_int,
    parse_sizes,
    prometheus_metrics,
    result_metadata,
    wait_for_ready,
    write_json_result,
)
from cluster_resources import ProcessStats
from cluster_faults import PeerResponseDelayProxy
from cluster_scenarios import (
    DEFAULT_COLD_KEY_COUNT,
    DEFAULT_COLD_MESSAGES_PER_KEY,
    DEFAULT_HOT_KEY_MESSAGES,
    DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
    DEFAULT_HOT_ORDERING_CONCURRENCY,
    DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
    DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
    DEFAULT_PEER_FORWARDING_CONCURRENCY,
    DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS,
    DEFAULT_PEER_RESPONSE_DELAY_MS,
    DEFAULT_PUBLISH_BATCH_SIZE,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_SCENARIOS,
    DEFAULT_SLOW_CONSUMER_DELAY_MS,
    MAX_HOT_KEY_PROCESSING_DELAY_MS,
    MAX_HOT_ORDERING_CONCURRENCY,
    MAX_HOT_ORDERING_MESSAGES,
    MAX_HOT_ORDERING_TIMEOUT_SECONDS,
    MAX_LEADER_FAILURE_TIMEOUT_SECONDS,
    MAX_PEER_FORWARDING_CONCURRENCY,
    MAX_PEER_FORWARDING_TIMEOUT_SECONDS,
    MAX_PEER_RESPONSE_DELAY_MS,
    MAX_PUBLISH_BATCH_SIZE,
    MIN_RETAINED_RECOVERY_MESSAGES,
    parse_retained_messages,
    parse_scenarios,
    run_consume_ack,
    run_durable_publish,
    run_follower_failure_recovery,
    run_grouped_consume_ack,
    run_hot_ordering,
    run_leader_failure_recovery,
    run_parallel_grouped,
    run_peer_forwarding,
    run_publish_batch,
    run_restart_recovery,
    run_retained_hot_path,
    run_retained_recovery,
    run_slow_consumer,
)
from runtime import DockerContainer, create_network, inspect_image, remove_network

DEFAULT_BINARY = default_binary()
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_NODES = 3
# Match the broker's default so constrained benchmark hosts do not turn slow
# but valid operations into redeliveries while measuring unrelated scenarios.
DEFAULT_ACK_TIMEOUT_MS = 30_000
COMMAND_TIMEOUT_SECONDS = DEFAULT_TIMEOUT_SECONDS


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


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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
            "add retained_hot_path, peer_forwarding, publish_batch, hot_ordering, "
            "leader_failure_recovery, or follower_failure_recovery explicitly for "
            "focused probes)"
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
            if "retained_hot_path" in selected_scenarios:
                scenarios.append(
                    run_retained_hot_path(
                        cluster,
                        f"cluster_{run_id}_retained_hot_path_{size}",
                        payload,
                        args.messages,
                        args.retained_messages,
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
    if "retained_hot_path" in selected_scenarios:
        workload["retained_hot_path_messages"] = args.retained_messages

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
