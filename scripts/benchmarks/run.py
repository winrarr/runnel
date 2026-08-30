#!/usr/bin/env python3
"""Run repeatable, resource-limited end-to-end benchmarks against a Runnel container.

The current protocol is deliberately used as-is. Results therefore describe the
current development protocol and broker semantics; they are not a claim about a
future binary protocol or a different durability mode.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import (
    BenchmarkError,
    LineClient,
    ROOT,
    acknowledge,
    create_stream,
    git_revision,
    metric,
    measure_scenario,
    parse_sizes,
    poll,
    publish,
    wait_for_ready,
)
from resources import StatsSampler, summarize_stats

DEFAULT_IMAGE = "runnel:bench"
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_TIMEOUT_SECONDS = 15.0
SCENARIO_NAMES = (
    "durable_publish",
    "concurrent_publish",
    "consume_ack",
    "publish_consume_ack_roundtrip",
    "restart_recovery",
)


class DockerBroker:
    """Own a short-lived broker container and its temporary durable volume."""

    def __init__(self, image: str, cpus: str, memory: str) -> None:
        self.image = image
        self.cpus = cpus
        self.memory = memory
        isolation_id = os.environ.get("RUNNEL_ISOLATION_ID")
        suffix = isolation_id or f"{os.getpid()}-{time.time_ns()}"
        self.name = f"runnel-bench-{suffix}"
        self.network = f"runnel-bench-net-{suffix}"
        self.network_created = False
        self.data_dir = Path(tempfile.mkdtemp(prefix="runnel-bench-"))
        self.data_dir.chmod(0o777)
        self.image_id: str | None = None
        self.client_port: int | None = None
        self.http_port: int | None = None
        self.startup_ns: int | None = None
        self.stats = StatsSampler(self.name)

    def start(self) -> None:
        image = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", self.image],
            check=True,
            capture_output=True,
            text=True,
        )
        self.image_id = image.stdout.strip()
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--label",
            "runnel.benchmark=true",
            "--network",
            self.network,
            "--cpus",
            self.cpus,
            "--memory",
            self.memory,
            "--publish",
            "127.0.0.1::4222",
            "--publish",
            "127.0.0.1::8080",
            "--volume",
            f"{self.data_dir}:/var/lib/runnel",
            self.image,
        ]
        started = time.perf_counter_ns()
        try:
            subprocess.run(
                [
                    "docker",
                    "network",
                    "create",
                    "--label",
                    "runnel.benchmark=true",
                    self.network,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.network_created = True
            subprocess.run(command, check=True, capture_output=True, text=True)
            self.client_port = self._published_port(4222)
            self.http_port = self._published_port(8080)
            wait_for_ready(self.http_port, timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
        except (subprocess.CalledProcessError, BenchmarkError) as error:
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True, check=False
            )
            raise BenchmarkError(
                f"failed to start broker container: {error}\n{logs.stdout}{logs.stderr}"
            ) from error
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def client(self) -> LineClient:
        if self.client_port is None:
            raise BenchmarkError("broker client port was not discovered")
        return LineClient("127.0.0.1", self.client_port, DEFAULT_TIMEOUT_SECONDS)

    def restart(self) -> int:
        started = time.perf_counter_ns()
        subprocess.run(["docker", "restart", self.name], check=True, capture_output=True)
        self.client_port = self._published_port(4222)
        self.http_port = self._published_port(8080)
        try:
            wait_for_ready(self.http_port, timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
        except BenchmarkError as error:
            state = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} error={{.State.Error}}",
                    self.name,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            logs = subprocess.run(
                ["docker", "logs", self.name], capture_output=True, text=True, check=False
            )
            raise BenchmarkError(
                f"broker was not ready after restart: {error}\n"
                f"{state.stdout.strip()}\n{logs.stdout}{logs.stderr}"
            ) from error
        return time.perf_counter_ns() - started

    def _published_port(self, container_port: int) -> int:
        result = subprocess.run(
            ["docker", "port", self.name, f"{container_port}/tcp"],
            check=True,
            capture_output=True,
            text=True,
        )
        match = re.search(r":(\d+)\s*$", result.stdout.strip())
        if match is None:
            raise BenchmarkError(f"could not parse published port: {result.stdout!r}")
        return int(match.group(1))

    def close(self) -> None:
        self.stats.close()
        subprocess.run(["docker", "rm", "--force", self.name], check=False, capture_output=True)
        if self.network_created:
            subprocess.run(
                ["docker", "network", "rm", self.network],
                check=False,
                capture_output=True,
            )
        shutil.rmtree(self.data_dir, ignore_errors=True)


def new_stream(run_id: str, name: str, size: int) -> str:
    return f"bench_{run_id}_{name}_{size}"


def run_durable_publish(
    broker: DockerBroker, stream: str, payload: str, messages: int, warmup: int
) -> dict[str, Any]:
    client = broker.client()
    try:
        create_stream(client, stream)
        for _ in range(warmup):
            publish(client, stream, payload)
        def measured() -> dict[str, Any]:
            latencies: list[int] = []
            started = time.perf_counter_ns()
            for _ in range(messages):
                latencies.append(publish(client, stream, payload)[1])
            elapsed = time.perf_counter_ns() - started
            return metric("durable_publish", latencies, elapsed, message_size=len(payload))

        return measure_scenario(broker.stats, measured)
    finally:
        client.close()


def run_concurrent_publish(
    broker: DockerBroker,
    stream: str,
    payload: str,
    messages: int,
    concurrency: int,
) -> dict[str, Any]:
    setup = broker.client()
    create_stream(setup, stream)
    setup.close()
    per_worker = [(messages // concurrency) + (index < messages % concurrency) for index in range(concurrency)]

    def worker(worker_messages: int) -> list[int]:
        client = broker.client()
        try:
            return [
                publish(client, stream, payload)[1] for _ in range(worker_messages)
            ]
        finally:
            client.close()

    def measured() -> dict[str, Any]:
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, count) for count in per_worker if count]
            latencies = [latency for future in futures for latency in future.result()]
        elapsed = time.perf_counter_ns() - started
        return metric(
            "concurrent_publish",
            latencies,
            elapsed,
            message_size=len(payload),
            metadata={"concurrency": concurrency},
        )

    return measure_scenario(broker.stats, measured)


def run_consume_ack(
    broker: DockerBroker, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    producer = broker.client()
    create_stream(producer, stream)
    for _ in range(messages):
        publish(producer, stream, payload)
    producer.close()

    consumer = broker.client()

    def measured() -> dict[str, Any]:
        latencies: list[int] = []
        started = time.perf_counter_ns()
        try:
            for offset in range(messages):
                poll_started = time.perf_counter_ns()
                poll(consumer, stream, "benchmark-consumer", offset)
                acknowledge(consumer, stream, "benchmark-consumer", offset)
                latencies.append(time.perf_counter_ns() - poll_started)
        finally:
            consumer.close()
        elapsed = time.perf_counter_ns() - started
        return metric(
            "consume_ack",
            latencies,
            elapsed,
            message_size=len(payload),
            metadata={"publish_setup_excluded": True},
        )

    return measure_scenario(broker.stats, measured)


def run_roundtrip(
    broker: DockerBroker, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    producer = broker.client()
    consumer = broker.client()
    create_stream(producer, stream)

    def measured() -> dict[str, Any]:
        latencies: list[int] = []
        started = time.perf_counter_ns()
        try:
            for offset in range(messages):
                roundtrip_started = time.perf_counter_ns()
                publish(producer, stream, payload)
                poll(consumer, stream, "roundtrip-consumer", offset)
                acknowledge(consumer, stream, "roundtrip-consumer", offset)
                latencies.append(time.perf_counter_ns() - roundtrip_started)
        finally:
            producer.close()
            consumer.close()
        elapsed = time.perf_counter_ns() - started
        return metric("publish_consume_ack_roundtrip", latencies, elapsed, message_size=len(payload))

    return measure_scenario(broker.stats, measured)


def run_restart_recovery(
    broker: DockerBroker, stream: str, payload: str
) -> dict[str, Any]:
    client = broker.client()
    create_stream(client, stream)
    publish(client, stream, payload)
    poll(client, stream, "restart-consumer", 0)
    client.close()
    def measured() -> dict[str, Any]:
        restart_ns = broker.restart()
        recovered = broker.client()
        try:
            poll(recovered, stream, "restart-consumer", 0)
            acknowledge(recovered, stream, "restart-consumer", 0)
        finally:
            recovered.close()
        return {
            "operation": "restart_recovery",
            "messages": 1,
            "message_size_bytes": len(payload),
            "restart_ready_seconds": restart_ns / 1_000_000_000,
            "metadata": {"unacknowledged_message_redelivered": True},
        }

    return measure_scenario(broker.stats, measured)


def build_image(image: str) -> None:
    subprocess.run(["docker", "build", "--tag", image, str(ROOT)], check=True)


def parse_scenarios(value: str) -> list[str]:
    scenarios = [part.strip() for part in value.split(",") if part.strip()]
    if not scenarios:
        raise argparse.ArgumentTypeError("scenarios must not be empty")
    if len(set(scenarios)) != len(scenarios):
        raise argparse.ArgumentTypeError("scenarios must not contain duplicates")
    unknown = [scenario for scenario in scenarios if scenario not in SCENARIO_NAMES]
    if unknown:
        available = ", ".join(SCENARIO_NAMES)
        raise argparse.ArgumentTypeError(
            f"unknown scenario(s): {', '.join(unknown)}; choose from: {available}"
        )
    return scenarios


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--build", action="store_true", help="build the image before running")
    parser.add_argument("--cpus", default="2", help="Docker CPU limit, for example 2 or 1.5")
    parser.add_argument("--memory", default="1g", help="Docker memory limit, for example 1g")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument(
        "--scenarios",
        type=parse_scenarios,
        default=list(SCENARIO_NAMES),
        metavar="NAME,...",
        help="comma-separated scenarios to run (default: all scenarios)",
    )
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--output", type=Path, help="result JSON path; defaults to benchmark-results/<timestamp>.json")
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.concurrency <= 0:
        parser.error("messages and concurrency must be positive; warmup cannot be negative")
    return args


def main() -> int:
    args = parse_args()
    if args.build:
        build_image(args.image)

    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"{run_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    broker = DockerBroker(args.image, args.cpus, args.memory)
    scenarios: list[dict[str, Any]] = []
    selected_scenarios = set(args.scenarios)
    try:
        broker.start()
        for size in args.payload_sizes:
            payload = "x" * size
            if "durable_publish" in selected_scenarios:
                scenarios.append(
                    run_durable_publish(
                        broker,
                        new_stream(run_id, "publish", size),
                        payload,
                        args.messages,
                        args.warmup,
                    )
                )
            if "concurrent_publish" in selected_scenarios:
                scenarios.append(
                    run_concurrent_publish(
                        broker,
                        new_stream(run_id, "concurrent", size),
                        payload,
                        args.messages,
                        args.concurrency,
                    )
                )
            if "consume_ack" in selected_scenarios:
                scenarios.append(
                    run_consume_ack(
                        broker,
                        new_stream(run_id, "consume", size),
                        payload,
                        args.messages,
                    )
                )
            if "publish_consume_ack_roundtrip" in selected_scenarios:
                scenarios.append(
                    run_roundtrip(
                        broker,
                        new_stream(run_id, "roundtrip", size),
                        payload,
                        args.messages,
                    )
                )
            if (
                "restart_recovery" in selected_scenarios
                and not args.skip_restart
                and size == args.payload_sizes[0]
            ):
                scenarios.append(
                    run_restart_recovery(
                        broker,
                        new_stream(run_id, "restart", size),
                        payload,
                    )
                )
    finally:
        broker.close()

    summary = {
        "schema_version": 1,
        "generated_at": timestamp.isoformat(),
        "backend": "runnel",
        "engine": "local",
        "git_revision": git_revision(short=True),
        "host": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
        },
        "container": {
            "image": args.image,
            "image_id": broker.image_id,
            "cpu_limit": args.cpus,
            "memory_limit": args.memory,
            "startup_seconds": (broker.startup_ns or 0) / 1_000_000_000,
            "resource_samples": summarize_stats(broker.stats.samples),
        },
        "workload": {
            "messages": args.messages,
            "warmup": args.warmup,
            "concurrency": args.concurrency,
            "payload_sizes_bytes": args.payload_sizes,
            "scenarios": args.scenarios,
            "protocol": "line-delimited JSON with UTF-8 string payloads",
            "durability": "current broker default; see engine and implementation configuration",
        },
        "scenarios": scenarios,
    }
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"results written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
