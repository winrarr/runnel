"""The first concrete backend: Runnel's current line-delimited protocol."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .api import (
    ActionResult,
    Backend,
    Client,
    ClientFactory,
    Endpoint,
    Limits,
    Runtime,
    Scenario,
    Workload,
)


class ProtocolError(RuntimeError):
    """The broker returned an invalid or unsuccessful response."""


class LineClient:
    def __init__(self, endpoint: Endpoint, timeout_seconds: float = 30.0) -> None:
        self._socket = socket.create_connection(
            (endpoint.host, endpoint.port), timeout=timeout_seconds
        )
        self._reader = self._socket.makefile("rb")

    def request(self, operation: str, **arguments: Any) -> Mapping[str, Any]:
        request = json.dumps({"op": operation, **arguments}, separators=(",", ":"))
        self._socket.sendall(request.encode() + b"\n")
        line = self._reader.readline()
        if not line:
            raise ProtocolError("broker closed the connection")
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise ProtocolError(f"invalid broker response: {line!r}") from error
        if response.get("type") == "error":
            raise ProtocolError(f"{response.get('code')}: {response.get('message')}")
        return response

    def close(self) -> None:
        try:
            self._reader.close()
        finally:
            self._socket.close()


@dataclass
class DockerRuntime:
    image: str = "runnel:bench"
    limits: Limits = Limits(cpus="2", memory="1g")
    ready_timeout: float = 30.0

    def __post_init__(self) -> None:
        suffix = os.environ.get("RUNNEL_ISOLATION_ID", f"{os.getpid()}-{time.time_ns()}")
        self.name = f"runnel-bench2-{suffix}"
        self.data_dir = Path(tempfile.mkdtemp(prefix="runnel-bench2-"))
        self.endpoint: Endpoint | None = None

    def start(self) -> Endpoint:
        self.data_dir.chmod(0o777)
        command = [
            "docker", "run", "--detach", "--name", self.name,
            "--label", "runnel.benchmark=true", "--cpus", self.limits.cpus or "2",
            "--memory", self.limits.memory or "1g", "--publish", "127.0.0.1::4222",
            "--publish", "127.0.0.1::8080", "--volume",
            f"{self.data_dir}:/var/lib/runnel", self.image,
        ]
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            port = self._published_port(4222)
            self._published_port(8080)
            endpoint = Endpoint("127.0.0.1", port)
            self.endpoint = endpoint
            self._wait_ready()
        except (OSError, subprocess.CalledProcessError, ProtocolError) as error:
            self.stop()
            raise ProtocolError(f"could not start Runnel container: {error}") from error
        return endpoint

    def restart(self) -> int:
        started = time.perf_counter_ns()
        subprocess.run(["docker", "restart", self.name], check=True, capture_output=True)
        port = self._published_port(4222)
        self.endpoint = Endpoint("127.0.0.1", port)
        self._wait_ready()
        return time.perf_counter_ns() - started

    def stop(self) -> None:
        subprocess.run(
            ["docker", "rm", "--force", self.name], check=False, capture_output=True
        )
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def sample(self) -> Mapping[str, Any]:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} {{.MemUsage}}", self.name],
            check=False, capture_output=True, text=True,
        )
        values = result.stdout.strip().split()
        if len(values) < 2:
            return {}
        try:
            cpu_percent = float(values[0].rstrip("%"))
            memory_bytes = _parse_bytes(values[1])
        except ValueError:
            return {}
        return {"cpu_percent": cpu_percent, "memory_bytes": memory_bytes}

    def _published_port(self, container_port: int) -> int:
        result = subprocess.run(
            ["docker", "port", self.name, f"{container_port}/tcp"],
            check=True, capture_output=True, text=True,
        )
        try:
            return int(result.stdout.rsplit(":", 1)[-1].strip())
        except ValueError as error:
            raise ProtocolError(f"invalid published port: {result.stdout!r}") from error

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self._published_port(8080)}/health/ready",
                    timeout=1,
                ) as response:
                    if response.status == 200:
                        with socket.create_connection(
                            ("127.0.0.1", self._published_port(4222)), timeout=1
                        ):
                            return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise ProtocolError("Runnel did not become ready")


def _parse_bytes(value: str) -> int:
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}
    number, unit = value[:-3].strip(), value[-3:]
    if unit not in units:
        unit = value[-1:]
        number = value[:-1].strip()
    return int(float(number) * units.get(unit, 1))


def _request(
    client: Client, operation: str, expected: str, **arguments: Any
) -> Mapping[str, Any]:
    response = client.request(operation, **arguments)
    if response.get("type") != expected:
        raise ProtocolError(f"expected {expected}, got {response}")
    return response


def _stream(client: Client, name: str) -> None:
    _request(client, "create_stream", "stream_created", stream=name)


def _publish(
    client: Client,
    stream: str,
    payload: bytes,
    expected_offset: int | None = None,
) -> int:
    started = time.perf_counter_ns()
    response = _request(
        client, "publish", "published", stream=stream, payload=payload.decode()
    )
    if expected_offset is not None and response.get("offset") != expected_offset:
        raise ProtocolError(f"expected offset {expected_offset}, got {response}")
    return time.perf_counter_ns() - started


def _publish_batch(
    client: Client,
    stream: str,
    payload: bytes,
    count: int,
    start_offset: int | None = 0,
) -> tuple[int, ...]:
    return tuple(
        _publish(
            client,
            stream,
            payload,
            start_offset + offset if start_offset is not None else None,
        )
        for offset in range(count)
    )


def _poll(client: Client, stream: str, consumer: str, offset: int) -> int:
    started = time.perf_counter_ns()
    response = _request(client, "poll", "message", stream=stream, consumer=consumer)
    if response.get("offset") != offset:
        raise ProtocolError(f"expected offset {offset}, got {response}")
    return time.perf_counter_ns() - started


def _ack(client: Client, stream: str, consumer: str, offset: int) -> int:
    started = time.perf_counter_ns()
    _request(client, "ack", "acknowledged", stream=stream, consumer=consumer, offset=offset)
    return time.perf_counter_ns() - started


def durable_publish(
    runtime: Runtime, clients: ClientFactory, workload: Workload, payload: bytes
) -> ActionResult:
    client = clients()
    stream = f"bench2-publish-{len(payload)}-{time.time_ns()}"
    try:
        _stream(client, stream)
        _publish_batch(client, stream, payload, workload.warmup)
        return ActionResult(_publish_batch(client, stream, payload, workload.messages, workload.warmup))
    finally:
        client.close()


def concurrent_publish(
    runtime: Runtime, clients: ClientFactory, workload: Workload, payload: bytes
) -> ActionResult:
    setup = clients()
    stream = f"bench2-concurrent-{len(payload)}-{time.time_ns()}"
    try:
        _stream(setup, stream)
    finally:
        setup.close()
    counts = [
        workload.messages // workload.concurrency
        + (worker < workload.messages % workload.concurrency)
        for worker in range(workload.concurrency)
    ]

    def publish_batch(count: int) -> tuple[int, ...]:
        client = clients()
        try:
            return _publish_batch(client, stream, payload, count, None)
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=workload.concurrency) as executor:
        futures = [executor.submit(publish_batch, count) for count in counts if count]
        return ActionResult(tuple(latency for future in futures for latency in future.result()), {"concurrency": workload.concurrency})


def consume_ack(
    runtime: Runtime, clients: ClientFactory, workload: Workload, payload: bytes
) -> ActionResult:
    producer = clients()
    stream = f"bench2-consume-{len(payload)}-{time.time_ns()}"
    try:
        _stream(producer, stream)
        _publish_batch(producer, stream, payload, workload.messages)
    finally:
        producer.close()
    consumer = clients()
    try:
        latencies = _consume_batch(consumer, stream, "bench2-consumer", workload.messages)
        return ActionResult(latencies, {"publish_setup_excluded": True})
    finally:
        consumer.close()


def roundtrip(
    runtime: Runtime, clients: ClientFactory, workload: Workload, payload: bytes
) -> ActionResult:
    producer, consumer = clients(), clients()
    stream = f"bench2-roundtrip-{len(payload)}-{time.time_ns()}"
    try:
        _stream(producer, stream)
        latencies = []
        for offset in range(workload.messages):
            started = time.perf_counter_ns()
            _publish(producer, stream, payload)
            _poll(consumer, stream, "bench2-roundtrip", offset)
            _ack(consumer, stream, "bench2-roundtrip", offset)
            latencies.append(time.perf_counter_ns() - started)
        return ActionResult(tuple(latencies))
    finally:
        producer.close()
        consumer.close()


def _consume_batch(
    client: Client, stream: str, consumer: str, count: int
) -> tuple[int, ...]:
    return tuple(
        _poll(client, stream, consumer, offset)
        + _ack(client, stream, consumer, offset)
        for offset in range(count)
    )


def restart_recovery(
    runtime: Runtime, clients: ClientFactory, workload: Workload, payload: bytes
) -> ActionResult:
    client = clients()
    stream = f"bench2-restart-{len(payload)}-{time.time_ns()}"
    try:
        _stream(client, stream)
        _publish(client, stream, payload)
        _poll(client, stream, "bench2-restart", 0)
    finally:
        client.close()
    restart_ns = runtime.restart()
    recovered = clients()
    try:
        _poll(recovered, stream, "bench2-restart", 0)
        _ack(recovered, stream, "bench2-restart", 0)
    finally:
        recovered.close()
    return ActionResult((restart_ns,), {"unacknowledged_message_redelivered": True})


@dataclass
class RunnelBackend:
    image: str = "runnel:bench"

    name: str = "runnel"

    def runtime(self, limits: Limits, nodes: int) -> Runtime:
        if nodes != 1:
            raise ValueError("benchmark2's first backend supports one node")
        return DockerRuntime(self.image, limits)

    def client_factory(self, runtime: Runtime) -> ClientFactory:
        def connect() -> LineClient:
            deadline = time.monotonic() + 5
            while True:
                try:
                    if runtime.endpoint is None:
                        raise ProtocolError("runtime has not started")
                    return LineClient(runtime.endpoint)
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)

        return connect

    def scenarios(self) -> Mapping[str, Scenario]:
        return {
            "durable_publish": durable_publish,
            "concurrent_publish": concurrent_publish,
            "consume_ack": consume_ack,
            "publish_consume_ack_roundtrip": roundtrip,
            "restart_recovery": restart_recovery,
        }
