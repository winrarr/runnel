"""Run a first-pass native-tool comparison of Runnel, Kafka, Redpanda, and JetStream.

The default comparison preserves each broker's native benchmark client and
single-node topology. ``--nodes 3`` adds a competitor-only, durable-publish
comparison with three broker nodes and replication factor three. It deliberately
does not include Runnel or a consumer result because those paths do not yet have
matching distributed semantics in this harness.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import ROOT, parse_sizes, result_metadata, write_json_result
from compare_backends import (
    run_kafka_family,
    run_nats,
    run_runnel,
    start_kafka_services,
    start_nats_services,
    start_redpanda_services,
)
from compare_lifecycle import close_services
from compare_results import (
    DEFAULT_NODES,
    THREE_NODE_COUNT,
    annotate_scenario_metadata,
    backend_metadata,
    benchmark_suite,
    comparison_guardrail_metadata,
    validate_comparison_summary,
)
from runtime import create_network, remove_network


DEFAULT_MESSAGES = 10_000
DEFAULT_CPUS = "2"
# Redpanda's development container reserves approximately 1 GiB before
# application overhead, so a 1 GiB cgroup is not a viable shared default.
DEFAULT_MEMORY = "2g"


def parse_args() -> argparse.Namespace:
    """Parse and validate the comparison command's stable public options."""
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
        choices=(DEFAULT_NODES, THREE_NODE_COUNT),
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
    """Run the selected comparison and write its versioned machine-readable result."""
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
            services = []
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
