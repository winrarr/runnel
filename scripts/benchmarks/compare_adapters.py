"""Backend-specific command and output adapters for native comparisons."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


KAFKA_IMAGE = "apache/kafka:4.3.1"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.2.1"
NATS_IMAGE = "nats:2.14.5-alpine"
NATS_BOX_IMAGE = "natsio/nats-box:0.19.7"


class ComparisonError(RuntimeError):
    """A benchmark setup or native-tool failure."""


def parse_number(value: str) -> float:
    return float(value.replace(",", ""))


def parse_kafka_publish(output: str, size: int, messages: int) -> dict[str, Any]:
    matches = list(
        re.finditer(
            r"(?P<count>[\d,]+) records sent, (?P<throughput>[\d.]+) records/sec .*?"
            r"(?P<avg>[\d.]+) ms avg latency, (?P<max>[\d.]+) ms max latency, "
            r"(?P<p50>[\d.]+) ms 50th, (?P<p95>[\d.]+) ms 95th, "
            r"(?P<p99>[\d.]+) ms 99th, (?P<p999>[\d.]+) ms 99.9th",
            output,
        )
    )
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
