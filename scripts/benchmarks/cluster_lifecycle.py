#!/usr/bin/env python3
"""Process and container lifecycle for the clustered benchmark."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cluster_faults import PeerResponseDelayProxy
from cluster_resources import ProcessStats
from cluster_scenarios import DEFAULT_PEER_RESPONSE_DELAY_MS
from common import (
    BenchmarkError,
    DEFAULT_TIMEOUT_SECONDS,
    LineClient,
    ROOT,
    prometheus_metrics,
    wait_for_ready,
)
from runtime import DockerContainer, create_network, inspect_image, remove_network


COMMAND_TIMEOUT_SECONDS = DEFAULT_TIMEOUT_SECONDS


@dataclass
class Node:
    """Run-scoped broker resources owned by one clustered node."""

    node_id: int
    broker_port: int
    http_port: int
    peer_port: int
    peer_address_port: int
    data_dir: Path
    process: subprocess.Popen[bytes] | None = None
    container: DockerContainer | None = None
    peer_proxy: PeerResponseDelayProxy | None = None
    log_handle: Any | None = None


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
        try:
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
        finally:
            node.process = None
            if node.log_handle is not None:
                node.log_handle.close()
                node.log_handle = None

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
        log_handle = None
        if self.log_dir is not None:
            log_handle = (self.log_dir / f"node-{node.node_id}.log").open("ab")
            stdout = log_handle
            stderr = subprocess.STDOUT
        try:
            node.process = subprocess.Popen(native_command, stdout=stdout, stderr=stderr)
        except OSError:
            if log_handle is not None:
                log_handle.close()
            raise
        node.log_handle = log_handle

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
