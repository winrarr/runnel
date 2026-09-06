#!/usr/bin/env python3
"""Run the two local reference workloads used by the product-fit plan.

The harness exercises the public JSON-lines protocol against real broker
processes. It produces a reviewable artifact containing the pre-registered
manifest, request transcript, message ledger, Prometheus snapshots, resource
samples, broker logs, latency distributions, and an explicit claim matrix.
Automated results are repository evidence only; operator effort and product
fit remain unknown until an intended participant completes the worksheet.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "docs" / "research" / "product-fit-manifests" / "local-reference.json"
DEFAULT_OUTPUT_ROOT = ROOT / "benchmark-results" / "product-fit"
DEFAULT_TIMEOUT_SECONDS = 10.0


class ProductFitError(RuntimeError):
    """A setup, protocol, or workload assertion failed."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def percentile(values: list[float], percentage: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except OSError:
            continue
    return total


def process_sample(pid: int, data_dir: Path) -> dict[str, Any] | None:
    try:
        status = (Path("/proc") / str(pid) / "status").read_text(encoding="utf-8")
        stat = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None

    rss_bytes = None
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            fields = line.split()
            if len(fields) >= 2:
                rss_bytes = int(fields[1]) * 1024
            break
    fields = stat.rsplit(")", 1)[-1].split()
    cpu_seconds = None
    if len(fields) >= 15:
        clock_tick = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        cpu_seconds = (int(fields[11]) + int(fields[12])) / clock_tick
    return {
        "timestamp": utc_now(),
        "rss_bytes": rss_bytes,
        "cpu_seconds": cpu_seconds,
        "storage_bytes": directory_size(data_dir),
    }


class ResourceSampler:
    """Sample broker RSS, CPU, and durable directory size while a workload runs."""

    def __init__(self, broker: "RunningBroker", interval_seconds: float = 0.05) -> None:
        self.broker = broker
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="product-fit-sampler", daemon=True)
        self._started = False

    def start(self) -> None:
        self._started = True
        self._thread.start()

    def close(self) -> None:
        if not self._started:
            return
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            sample = process_sample(self.broker.pid, self.broker.data_dir)
            if sample is not None:
                self.samples.append(sample)
            self._stop.wait(self.interval_seconds)

    def summary(self) -> dict[str, Any]:
        rss = [sample["rss_bytes"] for sample in self.samples if sample["rss_bytes"] is not None]
        cpu = [sample["cpu_seconds"] for sample in self.samples if sample["cpu_seconds"] is not None]
        storage = [sample["storage_bytes"] for sample in self.samples]
        return {
            "sample_count": len(self.samples),
            "rss_peak_bytes": max(rss, default=0),
            "cpu_seconds": max(cpu, default=0.0) - min(cpu, default=0.0),
            "storage_start_bytes": min(storage, default=0),
            "storage_peak_bytes": max(storage, default=0),
            "storage_growth_bytes": max(storage, default=0) - min(storage, default=0),
        }


class ProtocolClient:
    """Persistent client that records both successful and error responses."""

    def __init__(self, broker: "RunningBroker") -> None:
        self.broker = broker
        self.socket = socket.create_connection(("127.0.0.1", broker.broker_port), DEFAULT_TIMEOUT_SECONDS)
        self.reader = self.socket.makefile("rb")

    def request(self, request: dict[str, Any]) -> tuple[dict[str, Any], float]:
        encoded = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
        started = time.perf_counter_ns()
        self.socket.sendall(encoded)
        line = self.reader.readline()
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        if not line:
            raise ProductFitError("broker closed the protocol connection")
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise ProductFitError(f"invalid broker response: {line!r}") from error
        self.broker.transcript.append(
            {
                "timestamp": utc_now(),
                "request": request,
                "response": response,
                "elapsed_ms": elapsed_ms,
            }
        )
        return response, elapsed_ms

    def close(self) -> None:
        try:
            self.reader.close()
        finally:
            self.socket.close()


class RunningBroker:
    """Own one real local broker process and its durable temporary directory."""

    def __init__(self, binary: Path, data_dir: Path, output_dir: Path, config: dict[str, Any]) -> None:
        self.binary = binary
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.config = config
        self.broker_port = 0
        self.http_port = 0
        self.process: subprocess.Popen[Any] | None = None
        self.transcript: list[dict[str, Any]] = []
        self.readiness: list[dict[str, Any]] = []
        self.exit_codes: list[int | None] = []
        self.log_path = output_dir / "broker.log"
        self._log_handle: Any = None

    @property
    def pid(self) -> int:
        if self.process is None:
            return -1
        return self.process.pid

    def start(self) -> None:
        self.broker_port = free_port()
        self.http_port = free_port()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._log_handle = self.log_path.open("a", encoding="utf-8")
        self._log_handle.write(f"\n=== broker start {utc_now()} ===\n")
        self._log_handle.flush()
        command = [
            str(self.binary),
            "--data-dir",
            str(self.data_dir),
            "--listen",
            f"127.0.0.1:{self.broker_port}",
            "--http-listen",
            f"127.0.0.1:{self.http_port}",
            "--ack-timeout-ms",
            str(self.config["ack_timeout_ms"]),
            "--max-delivery-attempts",
            str(self.config["max_delivery_attempts"]),
        ]
        self.process = subprocess.Popen(
            command,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            self.wait_ready()
        except Exception:
            self.stop()
            raise

    def wait_ready(self) -> float:
        started = time.perf_counter_ns()
        deadline = time.monotonic() + DEFAULT_TIMEOUT_SECONDS
        last_error = "not attempted"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{self.http_port}/health/ready", timeout=1
                ) as response:
                    if response.status == 200:
                        elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
                        self.readiness.append({"timestamp": utc_now(), "status": "ready", "elapsed_seconds": elapsed})
                        return elapsed
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                last_error = str(error)
            time.sleep(0.05)
        raise ProductFitError(f"broker did not become ready: {last_error}")

    def stop(self) -> None:
        if self.process is None:
            return
        process = self.process
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.exit_codes.append(process.returncode)
        self.process = None
        if self._log_handle is not None:
            self._log_handle.write(f"=== broker stop {utc_now()} exit={self.exit_codes[-1]} ===\n")
            self._log_handle.close()
            self._log_handle = None

    def restart(self) -> float:
        self.stop()
        self.start()
        return self.readiness[-1]["elapsed_seconds"]

    def metrics(self) -> dict[str, float] | None:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port}/metrics", timeout=1) as response:
                body = response.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, OSError):
            return None
        values: dict[str, float] = {}
        for line in body.splitlines():
            if not line or line.startswith("#"):
                continue
            fields = line.split()
            if len(fields) < 2:
                continue
            try:
                values[fields[0]] = float(fields[1])
            except ValueError:
                continue
        return values


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProductFitError(f"could not read manifest {path}: {error}") from error
    if manifest.get("schema_version") != 1:
        raise ProductFitError("manifest schema_version must be 1")
    workloads = manifest.get("workloads")
    if not isinstance(workloads, dict) or not {"background_work", "events_replay"}.issubset(workloads):
        raise ProductFitError("manifest must define background_work and events_replay workloads")
    for name, workload in workloads.items():
        if not isinstance(workload, dict) or not isinstance(workload.get("budgets"), dict):
            raise ProductFitError(f"workload {name} must contain a budgets object")
        for key, value in workload["budgets"].items():
            if not isinstance(value, (int, float)) or value <= 0:
                raise ProductFitError(f"workload {name} budget {key} must be positive")
    return manifest


def request_type(client: ProtocolClient, request: dict[str, Any], expected: str) -> tuple[dict[str, Any], float]:
    response, elapsed = client.request(request)
    if response.get("type") != expected:
        raise ProductFitError(f"expected {expected} for {request.get('op')}, got {response}")
    return response, elapsed


def error_response(client: ProtocolClient, request: dict[str, Any], code: str) -> tuple[dict[str, Any], float]:
    response, elapsed = client.request(request)
    if response.get("type") != "error" or response.get("code") != code:
        raise ProductFitError(f"expected error {code} for {request.get('op')}, got {response}")
    return response, elapsed


def metrics_delta(before: dict[str, float] | None, after: dict[str, float] | None) -> dict[str, Any]:
    if before is None or after is None:
        return {"available": False}
    return {
        "available": True,
        "delta": {
            key: after[key] - before[key]
            for key in sorted(before.keys() & after.keys())
        },
    }


def metrics_report(
    initial: dict[str, float] | None,
    before_restart: dict[str, float] | None,
    after_restart: dict[str, float] | None,
) -> dict[str, Any]:
    """Keep restart-separated snapshots explicit; counters reset with the process."""
    snapshots = [
        {"phase": "initial", "values": initial},
        {"phase": "before_restart", "values": before_restart},
        {"phase": "after_restart", "values": after_restart},
    ]
    available = all(snapshot["values"] is not None for snapshot in snapshots)
    return {
        "available": available,
        "snapshots": snapshots,
        "same_process_delta": metrics_delta(initial, before_restart),
        "restart_counter_delta": metrics_delta(before_restart, after_restart),
    }


def latency_summary(values: list[float]) -> dict[str, Any]:
    return {
        "sample_count": len(values),
        "milliseconds": {
            "p50": percentile(values, 50),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
            "max": max(values, default=0.0),
        },
    }


def record_ledger(ledger: list[dict[str, Any]], **event: Any) -> None:
    ledger.append({"timestamp": utc_now(), **event})


def poll_group(
    client: ProtocolClient,
    ledger: list[dict[str, Any]],
    stream: str,
    consumer: str,
    member: str,
) -> tuple[dict[str, Any], float]:
    request = {"op": "poll_group", "stream": stream, "consumer": consumer, "member": member}
    response, elapsed = client.request(request)
    if response.get("type") not in {"message", "empty"}:
        raise ProductFitError(f"unexpected grouped poll response: {response}")
    if response["type"] == "message":
        record_ledger(
            ledger,
            operation="delivery",
            stream=stream,
            consumer=consumer,
            member=member,
            offset=response["offset"],
            key=response.get("key"),
            delivery_attempt=response.get("delivery_attempt"),
            delivery_token=response.get("delivery_token"),
            outcome="delivered",
        )
    return response, elapsed


def ack_group(
    client: ProtocolClient,
    ledger: list[dict[str, Any]],
    stream: str,
    consumer: str,
    member: str,
    offset: int,
    token: str,
    expected_error: str | None = None,
) -> tuple[dict[str, Any], float]:
    request = {
        "op": "ack_group",
        "stream": stream,
        "consumer": consumer,
        "member": member,
        "offset": offset,
        "delivery_token": token,
    }
    if expected_error is None:
        response, elapsed = request_type(client, request, "acknowledged")
        outcome = "already_acknowledged" if response.get("already_acknowledged") else "acknowledged"
    else:
        response, elapsed = error_response(client, request, expected_error)
        outcome = expected_error
    record_ledger(
        ledger,
        operation="acknowledgement",
        stream=stream,
        consumer=consumer,
        member=member,
        offset=offset,
        delivery_token=token,
        outcome=outcome,
    )
    return response, elapsed


def run_background_work(
    binary: Path,
    manifest: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    workload = manifest["workloads"]["background_work"]
    config = {
        "ack_timeout_ms": workload["ack_timeout_ms"],
        "max_delivery_attempts": workload["max_delivery_attempts"],
    }
    data_dir = Path(tempfile.mkdtemp(prefix="runnel-fit-background-"))
    broker = RunningBroker(binary, data_dir, output_dir, config)
    sampler = ResourceSampler(broker)
    ledger: list[dict[str, Any]] = []
    latencies: dict[str, list[float]] = {"publish": [], "poll": [], "ack": []}
    started = time.perf_counter_ns()
    before_metrics: dict[str, float] | None = None
    pre_restart_metrics: dict[str, float] | None = None
    after_metrics: dict[str, float] | None = None
    recovery_seconds = 0.0
    try:
        broker.start()
        sampler.start()
        before_metrics = broker.metrics()
        client = ProtocolClient(broker)
        stream = workload["stream"]
        request_type(client, {"op": "create_stream", "stream": stream}, "stream_created")
        messages = int(workload["messages"])
        payload = "x" * int(workload["payload_bytes"])
        for index in range(messages):
            key = workload["keys"][index % len(workload["keys"])]
            request = {
                "op": "publish",
                "stream": stream,
                "key": key,
                "payload": payload,
                "request_id": f"fit-background-{index}",
            }
            response, elapsed = request_type(client, request, "published")
            latencies["publish"].append(elapsed)
            record_ledger(
                ledger,
                operation="publish",
                stream=stream,
                request_id=request["request_id"],
                offset=response["offset"],
                key=key,
                payload_bytes=len(payload),
                outcome="confirmed",
            )
        client.close()

        member_a = ProtocolClient(broker)
        member_b = ProtocolClient(broker)
        held = None
        response, elapsed = poll_group(member_a, ledger, stream, workload["consumer"], "worker-a")
        latencies["poll"].append(elapsed)
        if response["type"] != "message" or not response.get("delivery_token"):
            raise ProductFitError(f"expected first grouped delivery, got {response}")
        held = response
        time.sleep(config["ack_timeout_ms"] / 1_000 * 1.5)

        acknowledged: set[int] = set()
        deadline = time.monotonic() + DEFAULT_TIMEOUT_SECONDS
        while held["offset"] not in acknowledged and time.monotonic() < deadline:
            response, elapsed = poll_group(member_b, ledger, stream, workload["consumer"], "worker-b")
            latencies["poll"].append(elapsed)
            if response["type"] == "empty":
                time.sleep(0.01)
                continue
            if response["offset"] == held["offset"]:
                _, elapsed = ack_group(
                    member_a,
                    ledger,
                    stream,
                    workload["consumer"],
                    "worker-a",
                    held["offset"],
                    held["delivery_token"],
                    expected_error="stale_delivery",
                )
                latencies["ack"].append(elapsed)
                _, elapsed = ack_group(
                    member_b,
                    ledger,
                    stream,
                    workload["consumer"],
                    "worker-b",
                    response["offset"],
                    response["delivery_token"],
                )
                latencies["ack"].append(elapsed)
                acknowledged.add(response["offset"])
                break
            _, elapsed = ack_group(
                member_b,
                ledger,
                stream,
                workload["consumer"],
                "worker-b",
                response["offset"],
                response["delivery_token"],
            )
            latencies["ack"].append(elapsed)
            acknowledged.add(response["offset"])
        if held["offset"] not in acknowledged:
            raise ProductFitError("expired grouped delivery was not reassigned before timeout")

        members = [(member_a, "worker-a"), (member_b, "worker-b")]
        while len(acknowledged) < messages and time.monotonic() < deadline:
            for member_client, member_name in members:
                response, elapsed = poll_group(
                    member_client, ledger, stream, workload["consumer"], member_name
                )
                latencies["poll"].append(elapsed)
                if response["type"] == "empty":
                    continue
                _, elapsed = ack_group(
                    member_client,
                    ledger,
                    stream,
                    workload["consumer"],
                    member_name,
                    response["offset"],
                    response["delivery_token"],
                )
                latencies["ack"].append(elapsed)
                acknowledged.add(response["offset"])
            if len(acknowledged) < messages:
                time.sleep(0.01)
        member_a.close()
        member_b.close()
        if len(acknowledged) != messages:
            raise ProductFitError(f"only acknowledged {len(acknowledged)} of {messages} background messages")

        poison = ProtocolClient(broker)
        poison_stream = workload["poison_stream"]
        request_type(
            poison,
            {"op": "publish", "stream": poison_stream, "payload": "poison", "request_id": "fit-poison"},
            "published",
        )
        poison_attempts = []
        for _ in range(config["max_delivery_attempts"]):
            response, elapsed = poll_group(
                poison, ledger, poison_stream, "poison-consumer", "poison-member"
            )
            latencies["poll"].append(elapsed)
            if response["type"] != "message":
                raise ProductFitError(f"expected poison delivery, got {response}")
            poison_attempts.append(response["delivery_attempt"])
            time.sleep(config["ack_timeout_ms"] / 1_000 * 1.5)
        response, elapsed = poll_group(poison, ledger, poison_stream, "poison-consumer", "poison-member")
        latencies["poll"].append(elapsed)
        if response["type"] != "empty":
            raise ProductFitError(f"poison message was not dead-lettered: {response}")
        response, elapsed = poll_group(
            poison, ledger, f"{poison_stream}.dead-letter", "dead-letter-inspector", "inspector"
        )
        latencies["poll"].append(elapsed)
        if response["type"] != "message":
            raise ProductFitError(f"dead-letter record was not available: {response}")
        _, elapsed = ack_group(
            poison,
            ledger,
            f"{poison_stream}.dead-letter",
            "dead-letter-inspector",
            "inspector",
            response["offset"],
            response["delivery_token"],
        )
        latencies["ack"].append(elapsed)
        poison.close()

        restart = ProtocolClient(broker)
        restart_stream = workload["restart_stream"]
        request_type(
            restart,
            {"op": "publish", "stream": restart_stream, "payload": "restart", "request_id": "fit-restart"},
            "published",
        )
        response, elapsed = request_type(
            restart,
            {"op": "poll", "stream": restart_stream, "consumer": "restart-worker"},
            "message",
        )
        latencies["poll"].append(elapsed)
        record_ledger(
            ledger,
            operation="delivery",
            stream=restart_stream,
            consumer="restart-worker",
            offset=response["offset"],
            delivery_attempt=response.get("delivery_attempt"),
            outcome="held_before_restart",
        )
        restart.close()
        pre_restart_metrics = broker.metrics()
        recovery_seconds = broker.restart()
        restart = ProtocolClient(broker)
        response, elapsed = request_type(
            restart,
            {"op": "poll", "stream": restart_stream, "consumer": "restart-worker"},
            "message",
        )
        latencies["poll"].append(elapsed)
        _, elapsed = request_type(
            restart,
            {"op": "ack", "stream": restart_stream, "consumer": "restart-worker", "offset": response["offset"]},
            "acknowledged",
        )
        latencies["ack"].append(elapsed)
        record_ledger(
            ledger,
            operation="acknowledgement",
            stream=restart_stream,
            consumer="restart-worker",
            offset=response["offset"],
            outcome="acknowledged_after_restart",
        )
        restart.close()
        after_metrics = broker.metrics()
    finally:
        sampler.close()
        broker.stop()
        shutil.rmtree(data_dir, ignore_errors=True)

    summary = sampler.summary()
    metrics = metrics_report(before_metrics, pre_restart_metrics, after_metrics)
    result = build_workload_result(
        "background_work",
        workload,
        summary,
        metrics,
        latencies,
        recovery_seconds,
        ledger,
        broker,
        started,
        extra={"stale_acknowledgements": 1, "poison_attempts": poison_attempts},
    )
    result["_transcript"] = broker.transcript
    result["_ledger"] = ledger
    result["_resource_samples"] = sampler.samples
    return result


def run_events_replay(
    binary: Path,
    manifest: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    workload = manifest["workloads"]["events_replay"]
    config = {"ack_timeout_ms": workload["ack_timeout_ms"], "max_delivery_attempts": 10}
    data_dir = Path(tempfile.mkdtemp(prefix="runnel-fit-events-"))
    broker = RunningBroker(binary, data_dir, output_dir, config)
    sampler = ResourceSampler(broker)
    ledger: list[dict[str, Any]] = []
    latencies: dict[str, list[float]] = {"publish": [], "poll": [], "ack": [], "replay": []}
    started = time.perf_counter_ns()
    before_metrics: dict[str, float] | None = None
    pre_restart_metrics: dict[str, float] | None = None
    after_metrics: dict[str, float] | None = None
    recovery_seconds = 0.0
    try:
        broker.start()
        sampler.start()
        before_metrics = broker.metrics()
        client = ProtocolClient(broker)
        stream = workload["stream"]
        request_type(client, {"op": "create_stream", "stream": stream}, "stream_created")
        messages = int(workload["messages"])
        payload = "e" * int(workload["payload_bytes"])
        for index in range(messages):
            request = {
                "op": "publish",
                "stream": stream,
                "payload": payload,
                "request_id": f"fit-events-{index}",
            }
            response, elapsed = request_type(client, request, "published")
            latencies["publish"].append(elapsed)
            record_ledger(
                ledger,
                operation="publish",
                stream=stream,
                request_id=request["request_id"],
                offset=response["offset"],
                payload_bytes=len(payload),
                outcome="confirmed",
            )
        client.close()

        consumer_a = ProtocolClient(broker)
        consumer_b = ProtocolClient(broker)
        for consumer, name in ((consumer_a, "projection"), (consumer_b, "audit")):
            for offset in range(messages if name == "projection" else workload["before_restart_messages"]):
                response, elapsed = request_type(
                    consumer,
                    {"op": "poll", "stream": stream, "consumer": name},
                    "message",
                )
                latencies["poll"].append(elapsed)
                if response["offset"] != offset:
                    raise ProductFitError(f"{name} expected offset {offset}, got {response}")
                record_ledger(
                    ledger,
                    operation="delivery",
                    stream=stream,
                    consumer=name,
                    offset=offset,
                    outcome="delivered",
                )
                _, elapsed = request_type(
                    consumer,
                    {"op": "ack", "stream": stream, "consumer": name, "offset": offset},
                    "acknowledged",
                )
                latencies["ack"].append(elapsed)
                record_ledger(
                    ledger,
                    operation="acknowledgement",
                    stream=stream,
                    consumer=name,
                    offset=offset,
                    outcome="acknowledged",
                )
        consumer_a.close()
        consumer_b.close()

        pre_restart_metrics = broker.metrics()
        recovery_seconds = broker.restart()
        consumer_b = ProtocolClient(broker)
        before_restart = int(workload["before_restart_messages"])
        for offset in range(before_restart, messages):
            response, elapsed = request_type(
                consumer_b,
                {"op": "poll", "stream": stream, "consumer": "audit"},
                "message",
            )
            latencies["poll"].append(elapsed)
            if response["offset"] != offset:
                raise ProductFitError(f"audit expected offset {offset} after restart, got {response}")
            record_ledger(
                ledger,
                operation="delivery",
                stream=stream,
                consumer="audit",
                offset=offset,
                outcome="delivered_after_restart",
            )
            _, elapsed = request_type(
                consumer_b,
                {"op": "ack", "stream": stream, "consumer": "audit", "offset": offset},
                "acknowledged",
            )
            latencies["ack"].append(elapsed)
            record_ledger(
                ledger,
                operation="acknowledgement",
                stream=stream,
                consumer="audit",
                offset=offset,
                outcome="acknowledged_after_restart",
            )

        response, elapsed = request_type(
            consumer_b,
            {"op": "replay", "stream": stream, "consumer": "audit", "offset": 0},
            "replay_message",
        )
        latencies["replay"].append(elapsed)
        record_ledger(
            ledger,
            operation="replay",
            stream=stream,
            consumer="audit",
            offset=response["offset"],
            replay_outcome="available",
        )
        response, elapsed = error_response(
            consumer_b,
            {"op": "replay", "stream": stream, "consumer": "audit", "offset": messages},
            "history_unavailable",
        )
        latencies["replay"].append(elapsed)
        record_ledger(
            ledger,
            operation="replay",
            stream=stream,
            consumer="audit",
            offset=messages,
            replay_outcome=response["code"],
        )
        consumer_b.close()
        after_metrics = broker.metrics()
    finally:
        sampler.close()
        broker.stop()
        shutil.rmtree(data_dir, ignore_errors=True)

    summary = sampler.summary()
    metrics = metrics_report(before_metrics, pre_restart_metrics, after_metrics)
    result = build_workload_result(
        "events_replay",
        workload,
        summary,
        metrics,
        latencies,
        recovery_seconds,
        ledger,
        broker,
        started,
        extra={"ordinary_consumers": ["projection", "audit"], "replay_unavailable_offset": workload["messages"]},
    )
    result["_transcript"] = broker.transcript
    result["_ledger"] = ledger
    result["_resource_samples"] = sampler.samples
    return result


def build_workload_result(
    name: str,
    workload: dict[str, Any],
    resources: dict[str, Any],
    metrics: dict[str, Any],
    latencies: dict[str, list[float]],
    recovery_seconds: float,
    ledger: list[dict[str, Any]],
    broker: RunningBroker,
    started: int,
    extra: dict[str, Any],
) -> dict[str, Any]:
    elapsed_seconds = (time.perf_counter_ns() - started) / 1_000_000_000
    latency = {operation: latency_summary(samples) for operation, samples in latencies.items()}
    publish_count = len(latencies["publish"])
    throughput = publish_count / max(elapsed_seconds, 1e-9)
    budgets = workload["budgets"]
    checks = {
        "publish_p95_ms": check_upper(latency["publish"]["milliseconds"]["p95"], budgets["publish_p95_ms"]),
        "publish_p99_ms": check_upper(latency["publish"]["milliseconds"]["p99"], budgets["publish_p99_ms"]),
        "throughput_min_messages_per_second": check_lower(throughput, budgets["throughput_min_messages_per_second"]),
        "rss_peak_bytes": check_upper(resources["rss_peak_bytes"], budgets["rss_peak_bytes"]),
        "disk_growth_bytes": check_upper(resources["storage_growth_bytes"], budgets["disk_growth_bytes"]),
        "recovery_seconds": check_upper(recovery_seconds, budgets["recovery_seconds"]),
    }
    return {
        "name": name,
        "status": "pass" if all(item["status"] == "pass" for item in checks.values()) else "fail",
        "elapsed_seconds": elapsed_seconds,
        "messages": workload["messages"],
        "latency": latency,
        "throughput_messages_per_second": throughput,
        "recovery_seconds": recovery_seconds,
        "resources": resources,
        "server_metrics": metrics,
        "budget_checks": checks,
        "ledger_events": len(ledger),
        "readiness": broker.readiness,
        "exit_codes": broker.exit_codes,
        "extra": extra,
    }


def check_upper(value: float, limit: float) -> dict[str, Any]:
    return {"status": "pass" if value <= limit else "fail", "observed": value, "limit": limit}


def check_lower(value: float, limit: float) -> dict[str, Any]:
    return {"status": "pass" if value >= limit else "fail", "observed": value, "limit": limit}


def environment() -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
        "kernel": platform.release(),
    }


def revision() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or "unknown"


def write_artifact(output_dir: Path, manifest: dict[str, Any], results: list[dict[str, Any]], transcript: list[dict[str, Any]]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    workloads_pass = all(result["status"] == "pass" for result in results)
    public_results = [
        {key: value for key, value in result.items() if not key.startswith("_")}
        for result in results
    ]
    artifact = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "source": {"revision": revision(), "command": sys.argv},
        "environment": environment(),
        "manifest": manifest,
        "automated_status": "pass" if workloads_pass else "fail",
        "product_fit_status": "unknown",
        "workloads": public_results,
        "claims": {
            "repository_semantics": "pass" if workloads_pass else "fail",
            "registered_numeric_budgets": "pass" if workloads_pass else "fail",
            "operator_effort": "unknown",
            "intended_user_product_fit": "unknown",
        },
        "evidence_gaps": [
            "No intended-user onboarding or recovery participant completed the worksheet.",
            "The manifest is a representative engineering envelope, not a user-signed product SLO.",
            "Retention, disk pressure, migration, and broad network fault behavior are outside these local workloads.",
        ],
        "files": {
            "manifest": "manifest.json",
            "transcript": "transcript.json",
            "ledger": "ledger.json",
            "resources": "resources.json",
            "broker_logs": {
                result["name"]: f"{result['name']}/broker.log" for result in public_results
            },
        },
    }
    (output_dir / "result.json").write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output_dir / "transcript.json").write_text(json.dumps(transcript, indent=2) + "\n", encoding="utf-8")
    ledger = [event for result in results for event in result.get("_ledger", [])]
    (output_dir / "ledger.json").write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    resources = {
        result["name"]: {
            "summary": result["resources"],
            "samples": result.get("_resource_samples", []),
        }
        for result in results
    }
    (output_dir / "resources.json").write_text(json.dumps(resources, indent=2) + "\n", encoding="utf-8")
    return output_dir / "result.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--binary", type=Path, default=ROOT / "target" / "debug" / "runnel")
    parser.add_argument(
        "--workload",
        choices=("all", "background_work", "events_replay"),
        default="all",
        help="run one workload or both reference workloads",
    )
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args.manifest)
    if not args.binary.is_file():
        raise ProductFitError(
            f"broker binary does not exist at {args.binary}; run `cargo build --locked -p runnel-server` first"
        )
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / run_id
    results: list[dict[str, Any]] = []
    transcript: list[dict[str, Any]] = []
    if args.workload in {"all", "background_work"}:
        result = run_background_work(args.binary, manifest, output_dir / "background_work")
        results.append(result)
        transcript.extend(result.pop("_transcript", []))
    if args.workload in {"all", "events_replay"}:
        result = run_events_replay(args.binary, manifest, output_dir / "events_replay")
        results.append(result)
        transcript.extend(result.pop("_transcript", []))
    artifact = write_artifact(output_dir, manifest, results, transcript)
    print(json.dumps({"automated_status": "pass" if all(r["status"] == "pass" for r in results) else "fail", "result": str(artifact)}, indent=2))
    return 0 if all(r["status"] == "pass" for r in results) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProductFitError as error:
        print(f"product-fit validation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
