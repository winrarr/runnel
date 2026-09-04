"""Backend-specific execution orchestration for native comparisons."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from common import ROOT
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
    redpanda_command,
    require_redpanda_broker_count,
)
from compare_lifecycle import (
    READINESS_COMMAND_TIMEOUT,
    Service,
    ensure_image,
    run_measured_tool,
    run_tool,
    start_services,
)
from compare_results import record_tool_scenario, scenario_operation


def start_kafka_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    """Start a single-node or KRaft Kafka broker set and wait for each broker."""
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
    """Start Redpanda brokers and wait until the requested membership is visible."""
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
            [
                "cluster",
                "info",
                "-X",
                f"brokers={','.join(f'{name}:9092' for name in names)}",
            ],
            cpus=cpus,
            memory=memory,
            timeout=READINESS_COMMAND_TIMEOUT,
        )
        require_redpanda_broker_count(output, nodes)

    return start_services(services, ready, "Redpanda")


def start_nats_services(
    network: str, cpus: str, memory: str, nodes: int, resource_prefix: str
) -> list[Service]:
    """Start NATS JetStream servers and wait for each stream endpoint."""
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
    """Run Kafka's native producer and, for single-node mode, consumer clients."""
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
    """Run NATS JetStream's native synchronous publish/consume clients."""
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
    """Run Runnel's native protocol benchmark and project its result into this schema."""
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
