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
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from common import (
    ROOT,
    parse_sizes,
    result_metadata,
    write_json_result,
)
from runtime import DockerContainer, MeasuredContainer, create_network, inspect_image, remove_network
from compare_adapters import (
    ComparisonError,
    KAFKA_IMAGE,
    NATS_BOX_IMAGE,
    NATS_IMAGE,
    REDPANDA_IMAGE,
    kafka_environment,
    nats_server_command,
    parse_kafka_consume,
    parse_kafka_publish,
    parse_nats_consume,
    parse_nats_publish,
    parse_number,
    redpanda_command,
    require_redpanda_broker_count,
)
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
    "native clients and operation-specific acknowledgement, durability, replication, "
    "delivery, batching, client, and latency boundaries are not an apples-to-apples "
    "end-to-end ranking"
)
COMPARISON_MISMATCH_DIMENSIONS = (
    "acknowledgement",
    "durability",
    "replication",
    "delivery",
    "batching",
    "client",
    "latency",
)
SCENARIO_BOUNDARY_FIELDS = (
    "acknowledgement_boundary",
    "durability_boundary",
    "replication_topology",
    "delivery_boundary",
    "batching_boundary",
    "client_boundary",
    "latency_boundary",
)
SCENARIO_COMPARISON_CLASSES = {
    "publish": "publish-only",
    "consume_ack": "consume-with-ack",
    "consume": "consume-without-ack",
}


def scenario_operation(scenario: dict[str, Any]) -> str:
    """Read the current operation field while accepting older result artifacts."""
    operation = scenario.get("operation", scenario.get("name"))
    if not isinstance(operation, str):
        raise ComparisonError(f"benchmark scenario has no operation: {scenario!r}")
    return operation


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
    semantic = backend.get("semantic_metadata")
    if not isinstance(semantic, dict):
        raise ComparisonError("backend result is missing semantic_metadata")
    scenario_boundaries = semantic.get("scenario_boundaries")
    if not isinstance(scenario_boundaries, dict):
        raise ComparisonError("backend result is missing scenario_boundaries metadata")
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend scenario {index} is not an object")
        comparison_class = scenario_comparison_class(scenario_operation(scenario))
        boundaries = scenario_boundaries.get(comparison_class)
        if not isinstance(boundaries, dict):
            raise ComparisonError(
                f"backend scenario {index} is missing semantic boundaries for "
                f"{comparison_class!r}"
            )
        existing_metadata = scenario.get("metadata", {})
        if not isinstance(existing_metadata, dict):
            raise ComparisonError(f"backend scenario {index} metadata is not an object")
        existing_class = existing_metadata.get("comparison_class")
        if existing_class is not None and existing_class != comparison_class:
            raise ComparisonError(
                f"backend scenario {index} declares comparison class {existing_class!r}, "
                f"expected {comparison_class!r}"
            )
        existing_boundaries = existing_metadata.get("semantic_boundaries")
        if existing_boundaries is not None and existing_boundaries != boundaries:
            raise ComparisonError(
                f"backend scenario {index} declares semantic boundaries inconsistent "
                f"with {comparison_class!r}"
            )
        scenario["metadata"] = {
            **existing_metadata,
            "comparison_class": comparison_class,
            "semantic_boundaries": dict(boundaries),
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

    declared_classes = semantic.get("scenario_classes")
    if (
        not isinstance(declared_classes, list)
        or not declared_classes
        or any(not isinstance(value, str) or not value for value in declared_classes)
        or len(set(declared_classes)) != len(declared_classes)
    ):
        raise ComparisonError(f"backend {name!r} has incomplete scenario_classes metadata")

    scenario_boundaries = semantic.get("scenario_boundaries")
    if not isinstance(scenario_boundaries, dict):
        raise ComparisonError(f"backend {name!r} is missing scenario_boundaries metadata")
    if set(scenario_boundaries) != set(declared_classes):
        raise ComparisonError(
            f"backend {name!r} scenario boundaries do not match declared scenario classes"
        )
    for comparison_class in declared_classes:
        boundaries = scenario_boundaries.get(comparison_class)
        if not isinstance(boundaries, dict):
            raise ComparisonError(
                f"backend {name!r} has no semantic boundaries for {comparison_class!r}"
            )
        for field in SCENARIO_BOUNDARY_FIELDS:
            boundary = _require_nonempty_text(
                boundaries.get(field),
                f"{field} for {name!r} {comparison_class!r}",
            )
            if field == "replication_topology" and boundary != replication:
                raise ComparisonError(
                    f"backend {name!r} {comparison_class!r} has inconsistent replication topology"
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
    if comparison.get("experimental") is not True:
        raise ComparisonError(f"backend {name!r} must be marked experimental")
    mismatch_dimensions = comparison.get("mismatch_dimensions")
    if mismatch_dimensions != list(COMPARISON_MISMATCH_DIMENSIONS):
        raise ComparisonError(
            f"backend {name!r} has incomplete comparison mismatch dimensions"
        )

    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ComparisonError(f"backend {name!r} must contain at least one scenario")
    observed_classes: set[str] = set()
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend {name!r} scenario {index} is not an object")
        expected_class = scenario_comparison_class(scenario_operation(scenario))
        metadata = scenario.get("metadata")
        if not isinstance(metadata, dict) or metadata.get("comparison_class") != expected_class:
            raise ComparisonError(
                f"backend {name!r} scenario {index} is missing comparison class "
                f"{expected_class!r}"
            )
        if metadata.get("semantic_boundaries") != scenario_boundaries[expected_class]:
            raise ComparisonError(
                f"backend {name!r} scenario {index} is missing semantic metadata for "
                f"{expected_class!r}"
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
        "experimental": True,
        "mismatch_dimensions": list(COMPARISON_MISMATCH_DIMENSIONS),
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


def ensure_image(image: str) -> str:
    """Return a local image ID, pulling the pinned image when necessary."""
    image_id = inspect_image(image)
    if image_id is not None:
        return image_id

    try:
        subprocess.run(["docker", "pull", image], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        detail = f"{error.stdout or ''}{error.stderr or ''}"
        raise ComparisonError(f"could not pull benchmark image {image}:\n{detail}") from error

    image_id = inspect_image(image)
    if image_id is None:
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
        self.container = MeasuredContainer(
            DockerContainer(
                name=name,
                image=image,
                network=network,
                cpus=cpus,
                memory=memory,
                data_dir=Path(tempfile.mkdtemp(prefix=f"{name}-")),
                data_target=data_target,
                command=command or [],
                environment=environment or {},
                entrypoint=entrypoint,
            ),
            probe_timeout_seconds=RESOURCE_COMMAND_TIMEOUT,
        )
        self.stats = self.container.stats

    @property
    def image_id(self) -> str | None:
        return self.container.image_id

    @property
    def startup_ns(self) -> int | None:
        return self.container.startup_ns

    def start(self) -> None:
        image_id = ensure_image(self.image)
        try:
            self.container.start(image_id=image_id)
        except subprocess.CalledProcessError as error:
            raise ComparisonError(
                f"failed to start {self.name}: {error}\n{self.container.logs()}"
            ) from error

    def close(self) -> dict[str, Any]:
        logs = self.container.close()
        summary = self.stats.summary()
        return {
            "image": self.image,
            "image_id": self.image_id,
            "cpu_limit": self.cpus,
            "memory_limit": self.memory,
            "startup_seconds": (self.startup_ns or 0) / 1_000_000_000,
            "resource_samples": summary,
            "log_tail": logs[-4000:],
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


def start_services(
    services: list[Service], ready: Callable[[], None], description: str
) -> list[Service]:
    try:
        for service in services:
            service.start()
        wait_for(ready, description)
    except BaseException:
        for service in services:
            service.close()
        raise
    return services


def record_tool_scenario(
    scenarios: list[dict[str, Any]],
    raw: dict[str, str],
    key: str,
    output: str,
    resources: dict[str, Any],
    parser: Callable[[str, int, int], dict[str, Any]],
    size: int,
    messages: int,
) -> None:
    scenario = parser(output, size, messages)
    scenario["resource_samples"] = resources
    scenarios.append(scenario)
    raw[key] = output[-12_000:]


def run_tool(
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> str:
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

    return start_services(services, ready, "Kafka")


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

    return start_services(services, ready, "Redpanda")


def start_nats_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    ensure_image(NATS_BOX_IMAGE)
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

    return start_services(services, ready, "NATS JetStream")


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
        record_tool_scenario(
            scenarios,
            raw,
            f"publish_{size}",
            producer_output,
            producer_resources,
            parse_kafka_publish,
            size,
            messages,
        )

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
            record_tool_scenario(
                scenarios,
                raw,
                f"consume_{size}",
                consumer_output,
                consumer_resources,
                parse_kafka_consume,
                size,
                messages,
            )
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
        record_tool_scenario(
            scenarios,
            raw,
            f"publish_{size}",
            producer_output,
            producer_resources,
            parse_nats_publish,
            size,
            messages,
        )

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
            record_tool_scenario(
                scenarios,
                raw,
                f"consume_{size}",
                consumer_output,
                consumer_resources,
                parse_nats_consume,
                size,
                messages,
            )
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
    source_backend = result.get("backends", {}).get("runnel")
    if not isinstance(source_backend, dict):
        container = result.get("container")
        scenarios = result.get("scenarios")
        if not isinstance(container, dict) or not isinstance(scenarios, list):
            raise ComparisonError("Runnel benchmark result has no canonical backend")
        source_backend = {**container, "scenarios": scenarios}
    scenarios = [
        scenario
        for scenario in source_backend["scenarios"]
        if scenario_operation(scenario) in {"durable_publish", "consume_ack"}
    ]
    return {
        "image": source_backend["image"],
        "image_id": source_backend.get("image_id"),
        "runtime": source_backend.get("runtime", "container"),
        "cpu_limit": cpus,
        "memory_limit": memory,
        "startup_seconds": source_backend["startup_seconds"],
        "resource_samples": source_backend["resource_samples"],
        "scenarios": [
            {
                "scenario_id": f"{'publish' if scenario_operation(scenario) == 'durable_publish' else 'consume_ack'}:{scenario['message_size_bytes']}",
                "operation": (
                    "publish"
                    if scenario_operation(scenario) == "durable_publish"
                    else "consume_ack"
                ),
                "messages": scenario["messages"],
                "message_size_bytes": scenario["message_size_bytes"],
                "throughput_messages_per_second": scenario[
                    "throughput_messages_per_second"
                ],
                "throughput_megabytes_per_second": scenario.get(
                    "throughput_megabytes_per_second"
                ),
                "elapsed_seconds": scenario.get("elapsed_seconds"),
                "elapsed_milliseconds": scenario.get("elapsed_milliseconds"),
                "latency_sample_count": scenario.get("latency_sample_count"),
                "latency_microseconds": scenario["latency_microseconds"],
                "resource_samples": scenario.get("resource_samples", {}),
                "server_metrics": scenario.get("server_metrics"),
                "metadata": scenario.get("metadata", {}),
            }
            for scenario in scenarios
        ],
        "measurement_client": "host Python socket client",
        "client_image": "host Python runtime",
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
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": "publish response after the local durable append",
                "durability_boundary": (
                    "current local broker durable-append default; no replica quorum"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "one in-flight publish request per message; concurrency=1; no client batching"
                ),
                "client_boundary": "host Python socket client using the host Python runtime",
                "latency_boundary": (
                    "per-message publish request send-to-response; p50/p99/p99.9/max are recorded"
                ),
            },
            "consume-with-ack": {
                "acknowledgement_boundary": (
                    "ack response after the local consumer checkpoint is persisted"
                ),
                "durability_boundary": "consumer checkpoint persistence on the single local broker",
                "replication_topology": replication,
                "delivery_boundary": (
                    "one poll followed by one acknowledgement per message; at-least-once delivery"
                ),
                "batching_boundary": (
                    "one poll-and-ack sequence per message; concurrency=1; no client batching"
                ),
                "client_boundary": "host Python socket client using the host Python runtime",
                "latency_boundary": (
                    "per-message poll-and-ack sequence; p50/p99/p99.9/max are recorded"
                ),
            },
        }
    elif name in {"kafka", "redpanda"}:
        client_image = KAFKA_IMAGE
        client_name = "Kafka producer/consumer performance clients"
        scenario_classes = ["publish-only", "consume-without-ack"]
        if nodes == THREE_NODE_COUNT:
            publish_acknowledgement = (
                "Kafka producer performance client with acks=all and idempotence enabled; "
                "topic min.insync.replicas=3"
            )
            acknowledgement = publish_acknowledgement
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
            publish_durability = (
                "one-partition broker log with replication factor three and min.insync.replicas=3; "
                "filesystem flush behavior is not asserted"
            )
        else:
            publish_acknowledgement = "Kafka producer performance client with acks=all"
            acknowledgement = (
                f"{publish_acknowledgement}; consumer perf client measures fetch throughput "
                "without per-record application acknowledgement"
            )
            replication = "single broker, one partition, replication factor one"
            measurement_boundary = (
                "Kafka producer/consumer performance clients over the Kafka protocol"
            )
            publish_durability = (
                "one-partition broker log with replication factor one and acks=all; "
                "filesystem flush behavior is not asserted"
            )
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": publish_acknowledgement,
                "durability_boundary": publish_durability,
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "Kafka producer perf client uses linger.ms=0 and the default batch.size; "
                    "native client batching remains possible"
                ),
                "client_boundary": f"{client_name} from {client_image}",
                "latency_boundary": (
                    "native Kafka producer latency includes client-side batching and reports "
                    "avg/p50/p95/p99/p99.9/max"
                ),
            },
        }
        if nodes == 1:
            scenario_boundaries["consume-without-ack"] = {
                "acknowledgement_boundary": (
                    "native consumer perf fetch throughput; no per-record application "
                    "acknowledgement"
                ),
                "durability_boundary": (
                    "reads the single broker's log with replication factor one; consumer fetch is "
                    "not a durability acknowledgement"
                ),
                "replication_topology": replication,
                "delivery_boundary": (
                    "native consumer performance client fetches records through a consumer group; "
                    "no application-level delivery acknowledgement"
                ),
                "batching_boundary": (
                    "native consumer fetch batching; application batch size and per-message "
                    "processing are not measured"
                ),
                "client_boundary": f"Kafka consumer performance client from {client_image}",
                "latency_boundary": (
                    "per-record consumer latency is unavailable; output reports elapsed "
                    "fetch throughput"
                ),
            }
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
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": (
                    "synchronous JetStream PubAck after publishing to the three-replica stream"
                ),
                "durability_boundary": (
                    "file-backed stream with three replicas; synchronous PubAck; exact filesystem "
                    "flush behavior is not asserted"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "nats bench js pub sync publishes one message at a time; no explicit "
                    "client batch"
                ),
                "client_boundary": (
                    f"nats bench js native synchronous publisher from {client_image}"
                ),
                "latency_boundary": (
                    "native synchronous publisher stats measure publish acknowledgement latency "
                    "and report min/avg/p50/p90/p99/p99.9/max"
                ),
            },
        }
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
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": (
                    "synchronous JetStream PubAck after publishing to the file-backed stream"
                ),
                "durability_boundary": (
                    "file-backed stream with one replica; synchronous PubAck; exact filesystem "
                    "flush behavior is not asserted"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "nats bench js pub sync publishes one message at a time; no explicit "
                    "client batch"
                ),
                "client_boundary": (
                    f"nats bench js native synchronous publisher from {client_image}"
                ),
                "latency_boundary": (
                    "native synchronous publisher stats measure publish acknowledgement latency "
                    "and report min/avg/p50/p90/p99/p99.9/max"
                ),
            },
            "consume-with-ack": {
                "acknowledgement_boundary": (
                    "explicit consumer acknowledgement with synchronous double acknowledgement"
                ),
                "durability_boundary": (
                    "file-backed consumer on a one-replica stream; the double acknowledgement "
                    "confirms the consumer acknowledgement"
                ),
                "replication_topology": replication,
                "delivery_boundary": (
                    "pull consumer with deliver=all and replay=instant; one explicit "
                    "acknowledgement "
                    "per message"
                ),
                "batching_boundary": (
                    "nats bench js consume uses --batch=1 with explicit acknowledgement and "
                    "double acknowledgement"
                ),
                "client_boundary": f"nats bench js native consumer from {client_image}",
                "latency_boundary": (
                    "per-message consumer latency is unavailable; output reports acknowledged "
                    "consume throughput"
                ),
            },
        }

    return {
        "runtime": "container",
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
            "scenario_boundaries": scenario_boundaries,
            "comparison": {
                "classification": NATIVE_COMPARISON_CLASSIFICATION,
                "apples_to_apples": False,
                "ranking_eligible": False,
                "experimental": True,
                "mismatch_dimensions": list(COMPARISON_MISMATCH_DIMENSIONS),
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
    comparison_mode = (
        "three-node replicated durable publish; native broker tools; publish-only first slice"
        if args.nodes == THREE_NODE_COUNT
        else "native broker tools; first-pass, not a final apples-to-apples claim"
    )
    metadata = result_metadata(
        run_id,
        timestamp,
        benchmark_suite=benchmark_suite(args.nodes, args.backends),
        comparison_mode=comparison_mode,
        docker=True,
    )
    output = args.output or ROOT / "benchmark-results" / f"compare-{run_id}.json"
    resource_prefix = f"runnel-compare-{os.getpid()}-{time.time_ns()}"
    network = resource_prefix
    create_network(network)
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
        "schema_version": 2,
        **metadata,
        "started_at": timestamp.isoformat(),
        "comparison_mode": comparison_mode,
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
    write_json_result(output, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
