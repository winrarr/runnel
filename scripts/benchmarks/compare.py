#!/usr/bin/env python3
"""Run a first-pass native-tool comparison of Runnel, Kafka, Redpanda, and JetStream.

The default comparison preserves each broker's native benchmark client and
single-node topology. ``--nodes 3`` adds a competitor-only, durable-publish
comparison with three broker nodes and replication factor three. It deliberately
does not include Runnel or a consumer result because those paths do not yet have
matching distributed semantics in this harness.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from resources import parse_cpu_stat, parse_size, summarize_stats


ROOT = Path(__file__).resolve().parents[2]
KAFKA_IMAGE = "apache/kafka:4.3.1"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.2.1"
NATS_IMAGE = "nats:2.14.5-alpine"
NATS_BOX_IMAGE = "natsio/nats-box:0.19.7"
DEFAULT_MESSAGES = 10_000
DEFAULT_CPUS = "2"
DEFAULT_NODES = 1
THREE_NODE_COUNT = 3
# Redpanda's development container reserves approximately 1 GiB before
# application overhead, so a 1 GiB cgroup is not a viable shared default.
DEFAULT_MEMORY = "2g"
COMMAND_TIMEOUT = 180
READINESS_TIMEOUT = 45
READINESS_COMMAND_TIMEOUT = 10
# Docker's stats endpoint commonly takes just over one second even for a
# healthy local container. Keep probes bounded while allowing useful samples.
RESOURCE_COMMAND_TIMEOUT = 2
NATIVE_COMPARISON_CLASSIFICATION = "native-tool-baseline"
NATIVE_COMPARISON_REASON = (
    "native clients and operation-specific acknowledgement and measurement boundaries "
    "are not an apples-to-apples end-to-end ranking"
)
SCENARIO_COMPARISON_CLASSES = {
    "publish": "publish-only",
    "consume_ack": "consume-with-ack",
    "consume": "consume-without-ack",
}


class ComparisonError(RuntimeError):
    """A benchmark setup or native-tool failure."""


def scenario_comparison_class(operation: Any) -> str:
    """Return the explicit comparison class for a measured operation."""
    if not isinstance(operation, str) or operation not in SCENARIO_COMPARISON_CLASSES:
        supported = ", ".join(sorted(SCENARIO_COMPARISON_CLASSES))
        raise ComparisonError(
            f"unsupported comparison scenario operation {operation!r}; expected one of {supported}"
        )
    return SCENARIO_COMPARISON_CLASSES[operation]


def annotate_scenario_metadata(backend: dict[str, Any]) -> None:
    """Attach normalized comparison semantics to every measured scenario."""
    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ComparisonError("backend result must contain at least one scenario")
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend scenario {index} is not an object")
        comparison_class = scenario_comparison_class(scenario.get("operation"))
        existing_metadata = scenario.get("metadata", {})
        if not isinstance(existing_metadata, dict):
            raise ComparisonError(f"backend scenario {index} metadata is not an object")
        existing_class = existing_metadata.get("comparison_class")
        if existing_class is not None and existing_class != comparison_class:
            raise ComparisonError(
                f"backend scenario {index} declares comparison class {existing_class!r}, "
                f"expected {comparison_class!r}"
            )
        scenario["metadata"] = {
            **existing_metadata,
            "comparison_class": comparison_class,
        }


def _require_nonempty_text(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ComparisonError(f"comparison metadata is missing a non-empty {description}")
    return value


def validate_backend_record(name: str, backend: dict[str, Any]) -> None:
    """Reject a backend record that cannot be interpreted semantically."""
    if not isinstance(backend, dict):
        raise ComparisonError(f"backend {name!r} is not an object")

    acknowledgement = _require_nonempty_text(
        backend.get("acknowledgement"), f"acknowledgement boundary for {name!r}"
    )
    replication = _require_nonempty_text(
        backend.get("replication"), f"replication/topology for {name!r}"
    )
    measurement_boundary = _require_nonempty_text(
        backend.get("measurement_boundary"), f"measurement boundary for {name!r}"
    )
    measurement_client = _require_nonempty_text(
        backend.get("measurement_client"), f"client identity for {name!r}"
    )

    semantic = backend.get("semantic_metadata")
    if not isinstance(semantic, dict):
        raise ComparisonError(f"backend {name!r} is missing semantic_metadata")
    expected_boundaries = {
        "acknowledgement_boundary": acknowledgement,
        "replication_topology": replication,
        "measurement_boundary": measurement_boundary,
    }
    for field, expected in expected_boundaries.items():
        actual = _require_nonempty_text(semantic.get(field), f"{field} for {name!r}")
        if actual != expected:
            raise ComparisonError(
                f"backend {name!r} has inconsistent {field}: {actual!r} != {expected!r}"
            )

    client_identity = semantic.get("client_identity")
    if not isinstance(client_identity, dict):
        raise ComparisonError(f"backend {name!r} is missing a client_identity object")
    client_name = _require_nonempty_text(
        client_identity.get("name"), f"client identity name for {name!r}"
    )
    client_image = _require_nonempty_text(
        client_identity.get("image"), f"client identity image for {name!r}"
    )
    if client_name != measurement_client:
        raise ComparisonError(
            f"backend {name!r} has inconsistent client identity: "
            f"{client_name!r} != {measurement_client!r}"
        )
    declared_client_image = _require_nonempty_text(
        backend.get("client_image"), f"client image for {name!r}"
    )
    if client_image != declared_client_image:
        raise ComparisonError(f"backend {name!r} has inconsistent client image metadata")

    comparison = semantic.get("comparison")
    if not isinstance(comparison, dict):
        raise ComparisonError(f"backend {name!r} is missing comparison metadata")
    if comparison.get("classification") != NATIVE_COMPARISON_CLASSIFICATION:
        raise ComparisonError(f"backend {name!r} has an unknown comparison classification")
    if comparison.get("apples_to_apples") is not False:
        raise ComparisonError(f"backend {name!r} must be marked non-equivalent")
    if comparison.get("ranking_eligible") is not False:
        raise ComparisonError(f"backend {name!r} must not be ranking eligible")

    declared_classes = semantic.get("scenario_classes")
    if (
        not isinstance(declared_classes, list)
        or not declared_classes
        or any(not isinstance(value, str) or not value for value in declared_classes)
        or len(set(declared_classes)) != len(declared_classes)
    ):
        raise ComparisonError(f"backend {name!r} has incomplete scenario_classes metadata")

    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ComparisonError(f"backend {name!r} must contain at least one scenario")
    observed_classes: set[str] = set()
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend {name!r} scenario {index} is not an object")
        expected_class = scenario_comparison_class(scenario.get("operation"))
        metadata = scenario.get("metadata")
        if not isinstance(metadata, dict) or metadata.get("comparison_class") != expected_class:
            raise ComparisonError(
                f"backend {name!r} scenario {index} is missing comparison class {expected_class!r}"
            )
        observed_classes.add(expected_class)
    if observed_classes != set(declared_classes):
        raise ComparisonError(
            f"backend {name!r} declares scenario classes {declared_classes!r}, "
            f"observed {sorted(observed_classes)!r}"
        )


def comparison_guardrail_metadata(nodes: int) -> dict[str, Any]:
    """Describe why this native comparison must not be treated as a ranking."""
    return {
        "classification": NATIVE_COMPARISON_CLASSIFICATION,
        "apples_to_apples": False,
        "ranking_eligible": False,
        "scenario_scope": "publish-only" if nodes == THREE_NODE_COUNT else "publish and consume",
        "reason": NATIVE_COMPARISON_REASON,
    }


def validate_comparison_summary(summary: dict[str, Any]) -> None:
    """Validate the machine-readable guardrail on a complete raw result."""
    guardrail = summary.get("comparison_guardrail")
    if not isinstance(guardrail, dict):
        raise ComparisonError("comparison result is missing comparison_guardrail metadata")
    workload = summary.get("workload")
    nodes = workload.get("nodes") if isinstance(workload, dict) else None
    if not isinstance(nodes, int) or isinstance(nodes, bool) or nodes not in {
        DEFAULT_NODES,
        THREE_NODE_COUNT,
    }:
        raise ComparisonError("comparison result is missing a valid workload node count")
    expected_guardrail = comparison_guardrail_metadata(nodes)
    if any(guardrail.get(key) != value for key, value in expected_guardrail.items()):
        raise ComparisonError("comparison result has incomplete or inconsistent guardrail metadata")
    backends = summary.get("backends")
    if not isinstance(backends, dict) or not backends:
        raise ComparisonError("comparison result must contain backend records")
    for name, backend in backends.items():
        validate_backend_record(str(name), backend)


def benchmark_suite(nodes: int, backends: list[str]) -> str:
    """Identify the history series represented by a comparison workload."""
    if nodes == THREE_NODE_COUNT:
        return "cluster-comparison"
    if backends == ["runnel"]:
        return "runnel"
    return "native-comparison"


def git_revision() -> str:
    """Return the source revision used for the comparison, when available."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return "unknown"
    revision = result.stdout.strip()
    return revision if result.returncode == 0 and revision else "unknown"


def source_metadata() -> dict[str, Any]:
    """Capture source-control and CI identity in the raw result."""
    repository = os.environ.get("GITHUB_REPOSITORY")
    workflow_run_id = os.environ.get("GITHUB_RUN_ID")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = (
        f"{server_url}/{repository}/actions/runs/{workflow_run_id}"
        if repository and workflow_run_id
        else None
    )
    return {
        "repository": repository or "local",
        "revision": git_revision(),
        "ref": os.environ.get("GITHUB_REF_NAME")
        or os.environ.get("GITHUB_REF", "local"),
        "event": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "workflow": os.environ.get("GITHUB_WORKFLOW", "local"),
        "run_id": workflow_run_id,
        "run_url": run_url,
        "profile": os.environ.get("BENCHMARK_PROFILE", "local"),
    }


def environment_metadata() -> dict[str, Any]:
    """Capture host details needed to interpret resource measurements."""
    return {
        "host": platform.node() or "unknown",
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
    }


def run_metadata(run_id: str) -> dict[str, Any]:
    """Return stable identity and provenance for one raw comparison artifact."""
    return {
        "run_id": run_id,
        "command": list(sys.argv),
        "source": source_metadata(),
        "environment": environment_metadata(),
    }


def bounded_read_stats(container: str) -> dict[str, float] | None:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
            capture_output=True,
            text=True,
            check=False,
            timeout=RESOURCE_COMMAND_TIMEOUT,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
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


def bounded_read_cpu_seconds(container: str) -> float | None:
    for path in ("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpuacct/cpuacct.usage"):
        try:
            result = subprocess.run(
                ["docker", "exec", container, "cat", path],
                capture_output=True,
                text=True,
                check=False,
                timeout=RESOURCE_COMMAND_TIMEOUT,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            usage = parse_cpu_stat(result.stdout)
            if usage is not None:
                return usage
    return None


class BoundedStatsSampler:
    """Collect comparison resources without allowing Docker probes to hang."""

    def __init__(self, container: str) -> None:
        self.container = container
        self.samples: list[dict[str, float]] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._run, name=f"docker-stats-{container}", daemon=True
        )

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)

    def begin(self) -> tuple[int, float | None, int]:
        self._record()
        with self.lock:
            sample_index = len(self.samples)
        return sample_index, bounded_read_cpu_seconds(self.container), time.perf_counter_ns()

    def end(self, token: tuple[int, float | None, int]) -> dict[str, Any]:
        sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        cpu_end = bounded_read_cpu_seconds(self.container)
        self._record()
        with self.lock:
            samples = list(self.samples[sample_index:])
        cpu_seconds = None
        if cpu_start is not None and cpu_end is not None:
            cpu_seconds = cpu_end - cpu_start
        return summarize_stats(
            samples,
            cpu_seconds=cpu_seconds,
            elapsed_seconds=(ended_ns - started_ns) / 1_000_000_000,
        )

    def summary(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.samples)
        return summarize_stats(samples)

    def _record(self) -> None:
        sample = bounded_read_stats(self.container)
        if sample is None:
            return
        with self.lock:
            self.samples.append(sample)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self._record()
            self.stop_event.wait(0.25)


def ensure_image(image: str) -> str:
    """Return a local image ID, pulling the pinned image when necessary."""
    inspect = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    image_id = inspect.stdout.strip()
    if inspect.returncode == 0 and image_id:
        return image_id

    try:
        subprocess.run(["docker", "pull", image], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        detail = f"{error.stdout or ''}{error.stderr or ''}"
        raise ComparisonError(f"could not pull benchmark image {image}:\n{detail}") from error

    inspect = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    image_id = inspect.stdout.strip()
    if inspect.returncode != 0 or not image_id:
        raise ComparisonError(f"Docker pulled benchmark image {image} but it could not be inspected")
    return image_id


class Service:
    def __init__(
        self,
        *,
        name: str,
        image: str,
        network: str,
        cpus: str,
        memory: str,
        data_target: str,
        command: list[str] | None = None,
        environment: dict[str, str] | None = None,
        entrypoint: str | None = None,
    ) -> None:
        self.name = name
        self.image = image
        self.network = network
        self.cpus = cpus
        self.memory = memory
        self.data_dir = Path(tempfile.mkdtemp(prefix=f"{name}-"))
        self.data_dir.chmod(0o777)
        self.data_target = data_target
        self.command = command or []
        self.environment = environment or {}
        self.entrypoint = entrypoint
        self.image_id: str | None = None
        self.startup_ns: int | None = None
        self.stats = BoundedStatsSampler(name)

    def start(self) -> None:
        self.image_id = ensure_image(self.image)
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--network",
            self.network,
            "--label",
            "runnel.benchmark=true",
            "--cpus",
            self.cpus,
            "--memory",
            self.memory,
            "--volume",
            f"{self.data_dir}:{self.data_target}",
        ]
        if self.entrypoint:
            command.extend(["--entrypoint", self.entrypoint])
        for key, value in self.environment.items():
            command.extend(["--env", f"{key}={value}"])
        command.append(self.image)
        command.extend(self.command)
        started = time.perf_counter_ns()
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
            self.startup_ns = time.perf_counter_ns() - started
            self.stats.start()
        except subprocess.CalledProcessError as error:
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True, check=False
            )
            raise ComparisonError(
                f"failed to start {self.name}: {error}\n{logs.stdout}{logs.stderr}"
            ) from error

    def close(self) -> dict[str, Any]:
        self.stats.close()
        summary = self.stats.summary()
        logs = subprocess.run(
            ["docker", "logs", self.name], capture_output=True, text=True, check=False
        )
        subprocess.run(["docker", "rm", "--force", self.name], check=False, capture_output=True)
        shutil.rmtree(self.data_dir, ignore_errors=True)
        return {
            "image": self.image,
            "image_id": self.image_id,
            "cpu_limit": self.cpus,
            "memory_limit": self.memory,
            "startup_seconds": (self.startup_ns or 0) / 1_000_000_000,
            "resource_samples": summary,
            "log_tail": (logs.stdout + logs.stderr)[-4000:],
        }


def combine_resource_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Return totals for a cluster while retaining per-node measurements."""
    if len(summaries) == 1:
        return summaries[0]

    combined: dict[str, Any] = {
        "nodes": summaries,
        "samples": min(summary.get("samples", 0) for summary in summaries),
    }
    for key in ("cpu_seconds", "memory_bytes_avg", "memory_bytes_max"):
        values = [summary.get(key) for summary in summaries]
        if all(isinstance(value, (int, float)) for value in values):
            combined[key] = sum(values)
    elapsed = [summary.get("elapsed_seconds") for summary in summaries]
    if all(isinstance(value, (int, float)) for value in elapsed):
        combined["elapsed_seconds"] = max(elapsed)
    return combined


def run_tool(
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> str:
    ensure_image(image)
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "--cpus",
        cpus,
        "--memory",
        memory,
        image,
        *arguments,
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        if isinstance(error, subprocess.CalledProcessError):
            detail = f"{error.stdout or ''}{error.stderr or ''}"
        else:
            detail = "command timed out"
        raise ComparisonError(f"benchmark tool failed: {' '.join(command)}\n{detail}") from error
    return result.stdout + result.stderr


def run_measured_tool(
    services: Service | list[Service],
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> tuple[str, dict[str, Any]]:
    service_list = services if isinstance(services, list) else [services]
    tokens = [service.stats.begin() for service in service_list]
    try:
        output = run_tool(
            image,
            network,
            arguments,
            cpus=cpus,
            memory=memory,
            timeout=timeout,
        )
    except BaseException:
        for service, token in zip(service_list, tokens):
            service.stats.end(token)
        raise
    resources = [service.stats.end(token) for service, token in zip(service_list, tokens)]
    return output, combine_resource_summaries(resources)


def wait_for(check: Callable[[], None], description: str) -> None:
    deadline = time.monotonic() + READINESS_TIMEOUT
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            check()
            return
        except (ComparisonError, subprocess.CalledProcessError, OSError) as error:
            last_error = error
        time.sleep(0.5)
    raise ComparisonError(f"{description} did not become ready: {last_error}")


def parse_sizes(value: str) -> list[int]:
    try:
        sizes = [int(part) for part in value.split(",") if part.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("payload sizes must be integers") from error
    if not sizes or any(size <= 0 for size in sizes):
        raise argparse.ArgumentTypeError("payload sizes must be positive")
    return sizes


def parse_number(value: str) -> float:
    return float(value.replace(",", ""))


def parse_kafka_publish(output: str, size: int, messages: int) -> dict[str, Any]:
    matches = list(re.finditer(
        r"(?P<count>[\d,]+) records sent, (?P<throughput>[\d.]+) records/sec .*?"
        r"(?P<avg>[\d.]+) ms avg latency, (?P<max>[\d.]+) ms max latency, "
        r"(?P<p50>[\d.]+) ms 50th, (?P<p95>[\d.]+) ms 95th, "
        r"(?P<p99>[\d.]+) ms 99th, (?P<p999>[\d.]+) ms 99.9th",
        output,
    ))
    if not matches:
        raise ComparisonError(f"could not parse Kafka producer output:\n{output}")
    parsed = matches[-1].groupdict()
    return {
        "operation": "publish",
        "messages": int(parsed["count"].replace(",", "")),
        "message_size_bytes": size,
        "throughput_messages_per_second": parse_number(parsed["throughput"]),
        "latency_microseconds": {
            "avg": parse_number(parsed["avg"]) * 1000,
            "p50": parse_number(parsed["p50"]) * 1000,
            "p95": parse_number(parsed["p95"]) * 1000,
            "p99": parse_number(parsed["p99"]) * 1000,
            "p999": parse_number(parsed["p999"]) * 1000,
            "max": parse_number(parsed["max"]) * 1000,
        },
        "requested_messages": messages,
    }


def parse_kafka_consume(output: str, size: int, messages: int) -> dict[str, Any]:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    tabular = [
        line
        for line in lines
        if re.match(r"^\d{4}-\d{2}-\d{2} .*?,\s+\d{4}-\d{2}-\d{2} .*?,", line)
    ]
    if not tabular:
        raise ComparisonError(f"could not parse Kafka consumer output:\n{output}")
    fields = [field.strip() for field in tabular[-1].split(",")]
    try:
        start = datetime.strptime(fields[0], "%Y-%m-%d %H:%M:%S:%f")
        end = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S:%f")
        elapsed_ms = (end - start).total_seconds() * 1000
        mb_per_sec = parse_number(fields[3])
        consumed = int(fields[4])
        n_msgs_per_sec = parse_number(fields[5])
    except (IndexError, ValueError) as error:
        raise ComparisonError(f"could not parse Kafka consumer row: {tabular[-1]}") from error
    return {
        "operation": "consume",
        "messages": consumed,
        "message_size_bytes": size,
        "elapsed_milliseconds": elapsed_ms,
        "throughput_messages_per_second": n_msgs_per_sec,
        "throughput_megabytes_per_second": mb_per_sec,
        "requested_messages": messages,
        "latency_available": False,
    }


def parse_nats_publish(output: str, size: int, messages: int) -> dict[str, Any]:
    match = re.search(
        r"stats: (?P<throughput>[\d,.]+) msgs/sec .*?min: (?P<min>[\d,.]+)us .*?"
        r"avg: (?P<avg>[\d,.]+)us .*?max: (?P<max>[\d,.]+)us .*?"
        r"P50: (?P<p50>[\d,.]+)us .*?P90: (?P<p90>[\d,.]+)us .*?"
        r"P99: (?P<p99>[\d,.]+)us .*?P99\.9: (?P<p999>[\d,.]+)us",
        output,
        re.IGNORECASE,
    )
    if match is None:
        raise ComparisonError(f"could not parse NATS publisher output:\n{output}")
    return {
        "operation": "publish",
        "messages": messages,
        "message_size_bytes": size,
        "throughput_messages_per_second": parse_number(match["throughput"]),
        "latency_microseconds": {
            key: parse_number(match[key])
            for key in ("min", "avg", "max", "p50", "p90", "p99", "p999")
        },
        "requested_messages": messages,
    }


def parse_nats_consume(output: str, size: int, messages: int) -> dict[str, Any]:
    match = re.search(
        r"stats: (?P<throughput>[\d,.]+) msgs/sec",
        output,
        re.IGNORECASE,
    )
    if match is None:
        raise ComparisonError(f"could not parse NATS consumer output:\n{output}")
    return {
        "operation": "consume_ack",
        "messages": messages,
        "message_size_bytes": size,
        "throughput_messages_per_second": parse_number(match["throughput"]),
        "requested_messages": messages,
        "latency_available": False,
    }


def kafka_environment(name: str, node_id: int, broker_names: list[str]) -> dict[str, str]:
    voters = ",".join(
        f"{index + 1}@{broker_name}:9093" for index, broker_name in enumerate(broker_names)
    )
    replication_factor = str(len(broker_names))
    return {
        "KAFKA_NODE_ID": str(node_id),
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{name}:9092",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": voters,
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
        "KAFKA_DEFAULT_REPLICATION_FACTOR": replication_factor,
        "KAFKA_MIN_INSYNC_REPLICAS": replication_factor,
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": replication_factor,
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": replication_factor,
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": replication_factor,
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_NUM_PARTITIONS": "1",
    }


def start_kafka_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    names = [f"{resource_prefix}-kafka-{index + 1}" for index in range(nodes)]
    services = [
        Service(
            name=name,
            image=KAFKA_IMAGE,
            network=network,
            cpus=cpus,
            memory=memory,
            data_target="/var/lib/kafka/data",
            environment=kafka_environment(name, index + 1, names),
        )
        for index, name in enumerate(names)
    ]
    try:
        for service in services:
            service.start()

        def ready() -> None:
            for name in names:
                run_tool(
                    KAFKA_IMAGE,
                    network,
                    [
                        "/opt/kafka/bin/kafka-topics.sh",
                        "--bootstrap-server",
                        f"{name}:9092",
                        "--list",
                    ],
                    cpus=cpus,
                    memory=memory,
                    timeout=READINESS_COMMAND_TIMEOUT,
                )

        wait_for(ready, "Kafka")
    except BaseException:
        for service in services:
            service.close()
        raise
    return services


def redpanda_command(name: str, node_id: int, seed_name: str | None) -> list[str]:
    command = [
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--smp",
        "1",
        "--node-id",
        str(node_id),
        "--kafka-addr",
        "internal://0.0.0.0:9092",
        "--advertise-kafka-addr",
        f"internal://{name}:9092",
        "--rpc-addr",
        f"{name}:33145",
        "--advertise-rpc-addr",
        f"{name}:33145",
    ]
    if seed_name is not None:
        command.extend(["--seeds", f"{seed_name}:33145"])
    return command


def require_redpanda_broker_count(output: str, expected: int) -> None:
    rows = re.findall(r"^\s*\d+\*?\s+\S+\s+\d+\s*$", output, re.MULTILINE)
    if len(rows) < expected:
        raise ComparisonError(
            f"Redpanda cluster has {len(rows)} brokers; expected {expected}:\n{output}"
        )


def start_redpanda_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    names = [f"{resource_prefix}-redpanda-{index}" for index in range(nodes)]
    services = []
    for index, name in enumerate(names):
        services.append(
            Service(
                name=name,
                image=REDPANDA_IMAGE,
                network=network,
                cpus=cpus,
                memory=memory,
                data_target="/var/lib/redpanda/data",
                command=redpanda_command(name, index, names[0] if index else None),
                environment={"REDPANDA_DATA_DIRECTORY": "/var/lib/redpanda/data"},
            )
        )
    try:
        for service in services:
            service.start()

        def ready() -> None:
            output = run_tool(
                REDPANDA_IMAGE,
                network,
                ["cluster", "info", "-X", f"brokers={','.join(f'{name}:9092' for name in names)}"],
                cpus=cpus,
                memory=memory,
                timeout=READINESS_COMMAND_TIMEOUT,
            )
            require_redpanda_broker_count(output, nodes)

        wait_for(ready, "Redpanda")
    except BaseException:
        for service in services:
            service.close()
        raise
    return services


def nats_server_command(name: str, names: list[str], cluster_name: str) -> list[str]:
    if len(names) == 1:
        return ["-js", "-sd", "/data"]
    routes = ",".join(f"nats://{other}:6222" for other in names if other != name)
    return [
        "-js",
        "-sd",
        "/data",
        "--name",
        name,
        "--cluster_name",
        cluster_name,
        "--cluster",
        "nats://0.0.0.0:6222",
        "--routes",
        routes,
    ]


def start_nats_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    names = [f"{resource_prefix}-nats-{index + 1}" for index in range(nodes)]
    cluster_name = f"{resource_prefix}-nats-cluster"
    services = []
    for name in names:
        services.append(
            Service(
                name=name,
                image=NATS_IMAGE,
                network=network,
                cpus=cpus,
                memory=memory,
                data_target="/data",
                command=nats_server_command(name, names, cluster_name),
            )
        )
    try:
        for service in services:
            service.start()

        def ready() -> None:
            for service in services:
                run_tool(
                    NATS_BOX_IMAGE,
                    network,
                    ["nats", "stream", "ls", "--server", f"nats://{service.name}:4222"],
                    cpus=cpus,
                    memory=memory,
                    timeout=READINESS_COMMAND_TIMEOUT,
                )

        wait_for(ready, "NATS JetStream")
    except BaseException:
        for service in services:
            service.close()
        raise
    return services


def start_kafka_service(network: str, cpus: str, memory: str) -> Service:
    return start_kafka_services(network, cpus, memory, 1, "bench")[0]


def start_redpanda_service(network: str, cpus: str, memory: str) -> Service:
    return start_redpanda_services(network, cpus, memory, 1, "bench")[0]


def start_nats_service(network: str, cpus: str, memory: str) -> Service:
    return start_nats_services(network, cpus, memory, 1, "bench")[0]


def run_kafka_family(
    *,
    backend: str,
    services: list[Service],
    network: str,
    client_cpus: str,
    client_memory: str,
    messages: int,
    sizes: list[int],
) -> dict[str, Any]:
    service = services[0]
    nodes = len(services)
    scenarios: list[dict[str, Any]] = []
    raw: dict[str, str] = {}
    for size in sizes:
        topic = f"bench-{backend}-{size}"
        run_tool(
            KAFKA_IMAGE,
            network,
            [
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                f"{service.name}:9092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                str(nodes),
                "--config",
                f"min.insync.replicas={nodes}",
            ],
            cpus=client_cpus,
            memory=client_memory,
        )
        producer_output, producer_resources = run_measured_tool(
            services,
            KAFKA_IMAGE,
            network,
            [
                "/opt/kafka/bin/kafka-producer-perf-test.sh",
                "--bootstrap-server",
                f"{service.name}:9092",
                "--topic",
                topic,
                "--num-records",
                str(messages),
                "--record-size",
                str(size),
                "--throughput",
                "-1",
                "--producer-props",
                "acks=all",
                "enable.idempotence=true",
                "linger.ms=0",
                "compression.type=none",
                "--print-metrics",
            ],
            cpus=client_cpus,
            memory=client_memory,
        )
        publish_scenario = parse_kafka_publish(producer_output, size, messages)
        publish_scenario["resource_samples"] = producer_resources
        scenarios.append(publish_scenario)
        raw[f"publish_{size}"] = producer_output[-12_000:]

        if nodes == 1:
            consumer_output, consumer_resources = run_measured_tool(
                services,
                KAFKA_IMAGE,
                network,
                [
                    "/opt/kafka/bin/kafka-consumer-perf-test.sh",
                    "--bootstrap-server",
                    f"{service.name}:9092",
                    "--topic",
                    topic,
                    "--num-records",
                    str(messages),
                    "--group",
                    f"bench-{backend}-{size}-{time.time_ns()}",
                    "--print-metrics",
                ],
                cpus=client_cpus,
                memory=client_memory,
            )
            consume_scenario = parse_kafka_consume(consumer_output, size, messages)
            consume_scenario["resource_samples"] = consumer_resources
            scenarios.append(consume_scenario)
            raw[f"consume_{size}"] = consumer_output[-12_000:]
    return {"scenarios": scenarios, "raw_tool_output": raw}


def run_nats(
    *,
    services: list[Service],
    network: str,
    client_cpus: str,
    client_memory: str,
    messages: int,
    sizes: list[int],
) -> dict[str, Any]:
    service = services[0]
    nodes = len(services)
    scenarios: list[dict[str, Any]] = []
    raw: dict[str, str] = {}
    for size in sizes:
        stream = f"bench{size}"
        subject = f"bench.subject.{size}"
        if nodes > 1:
            run_tool(
                NATS_BOX_IMAGE,
                network,
                [
                    "nats",
                    "stream",
                    "add",
                    stream,
                    "--server",
                    f"nats://{service.name}:4222",
                    "--subjects",
                    subject,
                    "--storage",
                    "file",
                    "--replicas",
                    str(nodes),
                    "--defaults",
                ],
                cpus=client_cpus,
                memory=client_memory,
            )
        producer_arguments = [
            "nats",
            "bench",
            "js",
            "pub",
            "sync",
            "--server",
            f"nats://{service.name}:4222",
            "--storage=file",
            f"--replicas={nodes}",
            f"--stream={stream}",
            f"--msgs={messages}",
            f"--size={size}B",
            "--no-progress",
            subject,
        ]
        if nodes == 1:
            producer_arguments.insert(7, "--create")
        producer_output, producer_resources = run_measured_tool(
            services,
            NATS_BOX_IMAGE,
            network,
            producer_arguments,
            cpus=client_cpus,
            memory=client_memory,
        )
        publish_scenario = parse_nats_publish(producer_output, size, messages)
        publish_scenario["resource_samples"] = producer_resources
        scenarios.append(publish_scenario)
        raw[f"publish_{size}"] = producer_output[-12_000:]

        if nodes == 1:
            consumer = f"bench-consumer-{size}"
            run_tool(
                NATS_BOX_IMAGE,
                network,
                [
                    "nats",
                    "consumer",
                    "add",
                    stream,
                    consumer,
                    "--server",
                    f"nats://{service.name}:4222",
                    "--pull",
                    "--ack=explicit",
                    "--deliver=all",
                    "--replay=instant",
                    f"--filter={subject}",
                    "--defaults",
                ],
                cpus=client_cpus,
                memory=client_memory,
            )
            consumer_output, consumer_resources = run_measured_tool(
                services,
                NATS_BOX_IMAGE,
                network,
                [
                    "nats",
                    "bench",
                    "js",
                    "consume",
                    "--server",
                    f"nats://{service.name}:4222",
                    f"--stream={stream}",
                    f"--consumer={consumer}",
                    f"--msgs={messages}",
                    f"--size={size}B",
                    "--batch=1",
                    "--acks=explicit",
                    "--doubleack",
                    "--no-progress",
                ],
                cpus=client_cpus,
                memory=client_memory,
            )
            consume_scenario = parse_nats_consume(consumer_output, size, messages)
            consume_scenario["resource_samples"] = consumer_resources
            scenarios.append(consume_scenario)
            raw[f"consume_{size}"] = consumer_output[-12_000:]
    return {"scenarios": scenarios, "raw_tool_output": raw}


def run_runnel(
    *,
    image: str,
    cpus: str,
    memory: str,
    messages: int,
    sizes: list[int],
) -> dict[str, Any]:
    fd, output_name = tempfile.mkstemp(prefix="runnel-compare-", suffix=".json")
    os.close(fd)
    output = Path(output_name)
    command = [
        sys.executable,
        str(Path(__file__).with_name("run.py")),
        "--image",
        image,
        "--cpus",
        cpus,
        "--memory",
        memory,
        "--messages",
        str(messages),
        "--warmup",
        "100",
        "--concurrency",
        "1",
        "--payload-sizes",
        ",".join(str(size) for size in sizes),
        "--scenarios",
        "durable_publish,consume_ack",
        "--skip-restart",
        "--output",
        str(output),
    ]
    try:
        subprocess.run(command, check=True, cwd=ROOT, capture_output=True, text=True)
        result = json.loads(output.read_text(encoding="utf-8"))
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError) as error:
        raise ComparisonError(f"Runnel benchmark failed: {error}") from error
    finally:
        output.unlink(missing_ok=True)
    scenarios = [
        scenario
        for scenario in result["scenarios"]
        if scenario["name"] in {"durable_publish", "consume_ack"}
    ]
    return {
        "image": result["container"]["image"],
        "image_id": result["container"].get("image_id"),
        "cpu_limit": cpus,
        "memory_limit": memory,
        "startup_seconds": result["container"]["startup_seconds"],
        "resource_samples": result["container"]["resource_samples"],
        "scenarios": [
            {
                "operation": "publish" if scenario["name"] == "durable_publish" else "consume_ack",
                "messages": scenario["messages"],
                "message_size_bytes": scenario["message_size_bytes"],
                "throughput_messages_per_second": scenario[
                    "throughput_messages_per_second"
                ],
                "latency_microseconds": scenario["latency_microseconds"],
                "resource_samples": scenario.get("resource_samples", {}),
            }
            for scenario in scenarios
        ],
        "measurement_client": "host Python socket client",
    }


def close_services(services: list[Service]) -> dict[str, Any]:
    summaries = [service.close() for service in services]
    if len(summaries) == 1:
        return summaries[0]
    return {
        "image": summaries[0]["image"],
        "image_id": summaries[0]["image_id"],
        "image_ids": [summary["image_id"] for summary in summaries],
        "cpu_limit": summaries[0]["cpu_limit"],
        "memory_limit": summaries[0]["memory_limit"],
        "startup_seconds": max(summary["startup_seconds"] for summary in summaries),
        "resource_samples": combine_resource_summaries(
            [summary["resource_samples"] for summary in summaries]
        ),
        "nodes": summaries,
    }


def remove_network(network: str) -> None:
    """Remove a run network, disconnecting only containers attached to it if needed."""
    for _ in range(3):
        result = subprocess.run(
            ["docker", "network", "rm", network],
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            return
        time.sleep(0.2)

    inspect = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            network,
            "--format",
            "{{range $id, $container := .Containers}}{{$id}} {{end}}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    for container in inspect.stdout.split():
        subprocess.run(
            ["docker", "network", "disconnect", "--force", network, container],
            check=False,
            capture_output=True,
        )
    subprocess.run(["docker", "network", "rm", network], check=False, capture_output=True)


def backend_metadata(name: str, nodes: int) -> dict[str, Any]:
    if name == "runnel":
        acknowledgement = (
            "request response after the current local durable append; consume acknowledgement "
            "persists a consumer checkpoint"
        )
        replication = "single local broker engine"
        measurement_boundary = "Runnel's current line-delimited JSON protocol"
        client_image = "host Python runtime"
        client_name = "host Python socket client"
        scenario_classes = ["publish-only", "consume-with-ack"]
    elif name in {"kafka", "redpanda"}:
        client_image = KAFKA_IMAGE
        client_name = "Kafka producer/consumer performance clients"
        scenario_classes = ["publish-only", "consume-without-ack"]
        if nodes == THREE_NODE_COUNT:
            acknowledgement = (
                "Kafka producer performance client with acks=all and idempotence enabled; "
                "topic min.insync.replicas=3"
            )
            replication = (
                "three broker nodes, one partition, replication factor three, "
                "min.insync.replicas three"
            )
            measurement_boundary = (
                "Kafka native producer performance client over the Kafka protocol; "
                "durable publish only"
            )
            client_name = "Kafka producer performance client"
            scenario_classes = ["publish-only"]
        else:
            acknowledgement = (
                "Kafka producer performance client with acks=all; consumer perf client "
                "measures fetch throughput without per-record application acknowledgement"
            )
            replication = "single broker, one partition, replication factor one"
            measurement_boundary = (
                "Kafka producer/consumer performance clients over the Kafka protocol"
            )
    elif nodes == THREE_NODE_COUNT:
        acknowledgement = (
            "JetStream synchronous publish PubAck to a file-backed stream configured with "
            "three replicas"
        )
        replication = "three NATS servers, file storage, three stream replicas"
        measurement_boundary = "nats bench js native synchronous publisher; durable publish only"
        client_image = NATS_BOX_IMAGE
        client_name = "nats bench js native synchronous publisher"
        scenario_classes = ["publish-only"]
    else:
        acknowledgement = (
            "JetStream synchronous publish PubAck; durable consumer explicit acknowledgement "
            "with synchronous double acknowledgement"
        )
        replication = "single NATS server, file storage, one replica"
        measurement_boundary = "nats bench js native benchmark client"
        client_image = NATS_BOX_IMAGE
        client_name = "nats bench js native benchmark client"
        scenario_classes = ["publish-only", "consume-with-ack"]

    return {
        "acknowledgement": acknowledgement,
        "replication": replication,
        "measurement_boundary": measurement_boundary,
        "measurement_client": client_name,
        "client_image": client_image,
        "semantic_metadata": {
            "acknowledgement_boundary": acknowledgement,
            "replication_topology": replication,
            "measurement_boundary": measurement_boundary,
            "client_identity": {"name": client_name, "image": client_image},
            "scenario_classes": scenario_classes,
            "comparison": {
                "classification": NATIVE_COMPARISON_CLASSIFICATION,
                "apples_to_apples": False,
                "ranking_eligible": False,
            },
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", default="runnel,kafka,redpanda,nats")
    parser.add_argument("--runnel-image", default="runnel:bench")
    parser.add_argument("--build-runnel", action="store_true")
    parser.add_argument("--cpus", default=DEFAULT_CPUS)
    parser.add_argument("--memory", default=DEFAULT_MEMORY)
    parser.add_argument("--client-cpus", default=DEFAULT_CPUS)
    parser.add_argument("--client-memory", default=DEFAULT_MEMORY)
    parser.add_argument(
        "--nodes",
        type=int,
        choices=(1, THREE_NODE_COUNT),
        default=DEFAULT_NODES,
        help="broker count; 3 enables competitor-only replicated durable publish",
    )
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    args.backends = [backend.strip() for backend in args.backends.split(",") if backend.strip()]
    valid = {"runnel", "kafka", "redpanda", "nats"}
    if not args.backends or any(backend not in valid for backend in args.backends):
        parser.error(f"backends must be selected from: {', '.join(sorted(valid))}")
    if args.nodes == THREE_NODE_COUNT and "runnel" in args.backends:
        parser.error("--nodes 3 supports only kafka, redpanda, and nats; Runnel has no comparison adapter")
    if args.nodes == THREE_NODE_COUNT and args.build_runnel:
        parser.error("--build-runnel is only valid for the single-node comparison")
    if args.messages <= 0:
        parser.error("messages must be positive")
    return args


def main() -> int:
    args = parse_args()
    if args.build_runnel:
        subprocess.run(["docker", "build", "--tag", args.runnel_image, str(ROOT)], check=True)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    metadata = run_metadata(run_id)
    output = args.output or ROOT / "benchmark-results" / f"compare-{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    resource_prefix = f"runnel-compare-{os.getpid()}-{time.time_ns()}"
    network = resource_prefix
    subprocess.run(["docker", "network", "create", network], check=True, capture_output=True)
    backends: dict[str, Any] = {}
    try:
        for backend in args.backends:
            services: list[Service] = []
            if backend == "runnel":
                result = run_runnel(
                    image=args.runnel_image,
                    cpus=args.cpus,
                    memory=args.memory,
                    messages=args.messages,
                    sizes=args.payload_sizes,
                )
            else:
                try:
                    if backend == "kafka":
                        services = start_kafka_services(
                            network, args.cpus, args.memory, args.nodes, resource_prefix
                        )
                    elif backend == "redpanda":
                        services = start_redpanda_services(
                            network, args.cpus, args.memory, args.nodes, resource_prefix
                        )
                    else:
                        services = start_nats_services(
                            network, args.cpus, args.memory, args.nodes, resource_prefix
                        )
                    if backend == "nats":
                        benchmark = run_nats(
                            services=services,
                            network=network,
                            client_cpus=args.client_cpus,
                            client_memory=args.client_memory,
                            messages=args.messages,
                            sizes=args.payload_sizes,
                        )
                    else:
                        benchmark = run_kafka_family(
                            backend=backend,
                            services=services,
                            network=network,
                            client_cpus=args.client_cpus,
                            client_memory=args.client_memory,
                            messages=args.messages,
                            sizes=args.payload_sizes,
                        )
                    result = {
                        **close_services(services),
                        **benchmark,
                    }
                    services = []
                except BaseException:
                    if services:
                        close_services(services)
                    raise
            backend_record = {
                **backend_metadata(backend, args.nodes),
                **result,
            }
            annotate_scenario_metadata(backend_record)
            backends[backend] = backend_record
    finally:
        remove_network(network)

    summary = {
        "schema_version": 1,
        **metadata,
        "generated_at": timestamp.isoformat(),
        "comparison_mode": (
            "three-node replicated durable publish; native broker tools; publish-only first slice"
            if args.nodes == THREE_NODE_COUNT
            else "native broker tools; first-pass, not a final apples-to-apples claim"
        ),
        "comparison_guardrail": comparison_guardrail_metadata(args.nodes),
        "benchmark_suite": benchmark_suite(args.nodes, args.backends),
        "resource_limits": {
            "broker_cpu": args.cpus,
            "broker_memory": args.memory,
            "client_cpu": args.client_cpus,
            "client_memory": args.client_memory,
        },
        "workload": {
            "messages": args.messages,
            "payload_sizes_bytes": args.payload_sizes,
            "single_node": args.nodes == 1,
            "nodes": args.nodes,
            "replication_factor": args.nodes,
            "operations": ["publish"] if args.nodes == THREE_NODE_COUNT else ["publish", "consume"],
            "compression": "disabled where the native client exposes the setting",
        },
        "backends": backends,
    }
    validate_comparison_summary(summary)
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"results written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
