#!/usr/bin/env python3
"""Run a first-pass native-tool comparison of Runnel, Kafka, Redpanda, and JetStream.

The comparison intentionally preserves each broker's native benchmark client. It
is useful for an initial baseline, but results must not be treated as a final
apples-to-apples claim until a common client workload is available.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from resources import StatsSampler


ROOT = Path(__file__).resolve().parents[2]
KAFKA_IMAGE = "apache/kafka:4.3.1"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.2.1"
NATS_IMAGE = "nats:2.14.5-alpine"
NATS_BOX_IMAGE = "natsio/nats-box:0.19.7"
DEFAULT_MESSAGES = 10_000
DEFAULT_CPUS = "2"
# Redpanda's development container reserves approximately 1 GiB before
# application overhead, so a 1 GiB cgroup is not a viable shared default.
DEFAULT_MEMORY = "2g"
COMMAND_TIMEOUT = 180


class ComparisonError(RuntimeError):
    """A benchmark setup or native-tool failure."""


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
        self.data_dir = Path(tempfile.mkdtemp(prefix=f"runnel-{name}-"))
        self.data_dir.chmod(0o777)
        self.data_target = data_target
        self.command = command or []
        self.environment = environment or {}
        self.entrypoint = entrypoint
        self.image_id: str | None = None
        self.startup_ns: int | None = None
        self.stats = StatsSampler(name)

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
    service: Service,
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> tuple[str, dict[str, Any]]:
    token = service.stats.begin()
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
        service.stats.end(token)
        raise
    return output, service.stats.end(token)


def wait_for(check: Callable[[], None], description: str) -> None:
    deadline = time.monotonic() + 45
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


def kafka_environment(name: str) -> dict[str, str]:
    return {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{name}:9092",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{name}:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_NUM_PARTITIONS": "1",
    }


def start_kafka_service(network: str, cpus: str, memory: str) -> Service:
    service = Service(
        name="bench-kafka",
        image=KAFKA_IMAGE,
        network=network,
        cpus=cpus,
        memory=memory,
        data_target="/var/lib/kafka/data",
        environment=kafka_environment("bench-kafka"),
    )
    try:
        service.start()

        def ready() -> None:
            run_tool(
                KAFKA_IMAGE,
                network,
                [
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "bench-kafka:9092",
                    "--list",
                ],
                cpus=cpus,
                memory=memory,
            )

        wait_for(ready, "Kafka")
    except BaseException:
        service.close()
        raise
    return service


def start_redpanda_service(network: str, cpus: str, memory: str) -> Service:
    service = Service(
        name="bench-redpanda",
        image=REDPANDA_IMAGE,
        network=network,
        cpus=cpus,
        memory=memory,
        data_target="/var/lib/redpanda/data",
        command=[
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--node-id",
            "0",
            "--kafka-addr",
            "internal://0.0.0.0:9092",
            "--advertise-kafka-addr",
            "internal://bench-redpanda:9092",
        ],
        environment={"REDPANDA_DATA_DIRECTORY": "/var/lib/redpanda/data"},
    )
    try:
        service.start()

        def ready() -> None:
            run_tool(
                REDPANDA_IMAGE,
                network,
                ["cluster", "info", "-X", "brokers=bench-redpanda:9092"],
                cpus=cpus,
                memory=memory,
            )

        wait_for(ready, "Redpanda")
    except BaseException:
        service.close()
        raise
    return service


def start_nats_service(network: str, cpus: str, memory: str) -> Service:
    service = Service(
        name="bench-nats",
        image=NATS_IMAGE,
        network=network,
        cpus=cpus,
        memory=memory,
        data_target="/data",
        command=["-js", "-sd", "/data"],
    )
    try:
        service.start()

        def ready() -> None:
            run_tool(
                NATS_BOX_IMAGE,
                network,
                ["nats", "rtt", "1", "--server", "nats://bench-nats:4222"],
                cpus=cpus,
                memory=memory,
            )

        wait_for(ready, "NATS JetStream")
    except BaseException:
        service.close()
        raise
    return service


def run_kafka_family(
    *,
    backend: str,
    service: Service,
    network: str,
    client_cpus: str,
    client_memory: str,
    messages: int,
    sizes: list[int],
) -> dict[str, Any]:
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
                "1",
            ],
            cpus=client_cpus,
            memory=client_memory,
        )
        producer_output, producer_resources = run_measured_tool(
            service,
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
                "linger.ms=0",
                "compression.type=none",
                "--print-metrics",
            ],
            cpus=client_cpus,
            memory=client_memory,
        )
        consumer_output, consumer_resources = run_measured_tool(
            service,
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
        publish_scenario = parse_kafka_publish(producer_output, size, messages)
        publish_scenario["resource_samples"] = producer_resources
        consume_scenario = parse_kafka_consume(consumer_output, size, messages)
        consume_scenario["resource_samples"] = consumer_resources
        scenarios.append(publish_scenario)
        scenarios.append(consume_scenario)
        raw[f"publish_{size}"] = producer_output[-12_000:]
        raw[f"consume_{size}"] = consumer_output[-12_000:]
    return {"scenarios": scenarios, "raw_tool_output": raw}


def run_nats(
    *,
    service: Service,
    network: str,
    client_cpus: str,
    client_memory: str,
    messages: int,
    sizes: list[int],
) -> dict[str, Any]:
    scenarios: list[dict[str, Any]] = []
    raw: dict[str, str] = {}
    for size in sizes:
        stream = f"bench{size}"
        subject = f"bench.subject.{size}"
        producer_output, producer_resources = run_measured_tool(
            service,
            NATS_BOX_IMAGE,
            network,
            [
                "nats",
                "bench",
                "js",
                "pub",
                "sync",
                "--server",
                "nats://bench-nats:4222",
                "--create",
                "--storage=file",
                "--replicas=1",
                f"--stream={stream}",
                f"--msgs={messages}",
                f"--size={size}B",
                "--no-progress",
                subject,
            ],
            cpus=client_cpus,
            memory=client_memory,
        )
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
                "nats://bench-nats:4222",
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
            service,
            NATS_BOX_IMAGE,
            network,
            [
                "nats",
                "bench",
                "js",
                "consume",
                "--server",
                "nats://bench-nats:4222",
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
        publish_scenario = parse_nats_publish(producer_output, size, messages)
        publish_scenario["resource_samples"] = producer_resources
        consume_scenario = parse_nats_consume(consumer_output, size, messages)
        consume_scenario["resource_samples"] = consumer_resources
        scenarios.append(publish_scenario)
        scenarios.append(consume_scenario)
        raw[f"publish_{size}"] = producer_output[-12_000:]
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


def backend_metadata(name: str) -> dict[str, Any]:
    if name == "runnel":
        return {
            "acknowledgement": "request response after the current local durable append; consume acknowledgement persists a consumer checkpoint",
            "replication": "single local broker engine",
            "measurement_boundary": "Runnel's current line-delimited JSON protocol",
        }
    if name in {"kafka", "redpanda"}:
        return {
            "acknowledgement": "Kafka producer performance client with acks=all; consumer perf client measures fetch throughput without per-record application acknowledgement",
            "replication": "single broker, one partition, replication factor one",
            "measurement_boundary": "Kafka producer/consumer performance clients over the Kafka protocol",
        }
    return {
        "acknowledgement": "JetStream synchronous publish PubAck; durable consumer explicit acknowledgement with synchronous double acknowledgement",
        "replication": "single NATS server, file storage, one replica",
        "measurement_boundary": "nats bench js native benchmark client",
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
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    args.backends = [backend.strip() for backend in args.backends.split(",") if backend.strip()]
    valid = {"runnel", "kafka", "redpanda", "nats"}
    if not args.backends or any(backend not in valid for backend in args.backends):
        parser.error(f"backends must be selected from: {', '.join(sorted(valid))}")
    if args.messages <= 0:
        parser.error("messages must be positive")
    return args


def main() -> int:
    args = parse_args()
    if args.build_runnel:
        subprocess.run(["docker", "build", "--tag", args.runnel_image, str(ROOT)], check=True)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"compare-{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    network = f"runnel-compare-{os.getpid()}-{time.time_ns()}"
    subprocess.run(["docker", "network", "create", network], check=True, capture_output=True)
    backends: dict[str, Any] = {}
    try:
        for backend in args.backends:
            service: Service | None = None
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
                        service = start_kafka_service(network, args.cpus, args.memory)
                    elif backend == "redpanda":
                        service = start_redpanda_service(network, args.cpus, args.memory)
                    else:
                        service = start_nats_service(network, args.cpus, args.memory)
                    if backend == "nats":
                        benchmark = run_nats(
                            service=service,
                            network=network,
                            client_cpus=args.client_cpus,
                            client_memory=args.client_memory,
                            messages=args.messages,
                            sizes=args.payload_sizes,
                        )
                    else:
                        benchmark = run_kafka_family(
                            backend=backend,
                            service=service,
                            network=network,
                            client_cpus=args.client_cpus,
                            client_memory=args.client_memory,
                            messages=args.messages,
                            sizes=args.payload_sizes,
                        )
                    result = {
                        **service.close(),
                        **benchmark,
                        "measurement_client": "broker-native client container",
                    }
                except BaseException:
                    if service is not None:
                        service.close()
                    raise
            backends[backend] = {
                **backend_metadata(backend),
                **result,
            }
    finally:
        subprocess.run(["docker", "network", "rm", network], check=False, capture_output=True)

    summary = {
        "schema_version": 1,
        "generated_at": timestamp.isoformat(),
        "comparison_mode": "native broker tools; first-pass, not a final apples-to-apples claim",
        "resource_limits": {
            "broker_cpu": args.cpus,
            "broker_memory": args.memory,
            "client_cpu": args.client_cpus,
            "client_memory": args.client_memory,
        },
        "workload": {
            "messages": args.messages,
            "payload_sizes_bytes": args.payload_sizes,
            "single_node": True,
            "replication_factor": 1,
            "compression": "disabled where the native client exposes the setting",
        },
        "backends": backends,
    }
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"results written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
