#!/usr/bin/env python3
"""Machine-readable result shaping for the clustered benchmark."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import TYPE_CHECKING, Any

from cluster_resources import resource_limits
from cluster_scenarios import MAX_HOT_ORDERING_MESSAGES
from common import result_metadata

if TYPE_CHECKING:
    from cluster_lifecycle import Cluster


def build_workload(args: argparse.Namespace) -> dict[str, Any]:
    """Build the stable workload section shared by clustered result artifacts."""
    workload: dict[str, Any] = {
        "messages": args.messages,
        "warmup": args.warmup,
        "concurrency": args.concurrency,
        "scenarios": args.scenarios,
        "nodes": args.nodes,
        "ack_timeout_ms": args.ack_timeout_ms,
        "slow_consumer_delay_ms": args.slow_consumer_delay_ms,
        "batch_size": args.batch_size,
        "hot_ordering": {
            "hot_key_messages": args.hot_key_messages,
            "cold_key_count": args.cold_key_count,
            "cold_messages_per_key": args.cold_messages_per_key,
            "concurrency": args.hot_ordering_concurrency,
            "hot_key_processing_delay_ms": args.hot_key_processing_delay_ms,
            "timeout_seconds": args.hot_ordering_timeout_seconds,
            "max_records": MAX_HOT_ORDERING_MESSAGES,
        },
        "peer_forwarding_concurrency": args.peer_forwarding_concurrency,
        "peer_response_delay_ms": args.peer_response_delay_ms,
        "peer_forwarding_timeout_seconds": args.peer_forwarding_timeout_seconds,
        "leader_failure_timeout_seconds": args.leader_failure_timeout_seconds,
        "payload_sizes_bytes": args.payload_sizes,
        "runtime": args.runtime,
        "protocol": "line-delimited JSON with UTF-8 string payloads",
        "protocol_version": "provisional-line-json-v1",
        "payload_encoding": "utf-8",
        "compression": "none",
        "durability": "committed by the current three-node Raft quorum and local durable state",
    }
    selected_scenarios = set(args.scenarios)
    if not args.skip_recovery and "cluster_retained_recovery" in selected_scenarios:
        workload["retained_recovery_messages"] = args.retained_messages
    if "retained_hot_path" in selected_scenarios:
        workload["retained_hot_path_messages"] = args.retained_messages
    return workload


def build_result(
    args: argparse.Namespace,
    *,
    run_id: str,
    started_at: datetime,
    cluster: Cluster,
    scenarios: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the complete schema-v2 result without writing the artifact."""
    return {
        **result_metadata(
            run_id,
            started_at,
            benchmark_suite="cluster",
            comparison_mode="cluster-baseline",
            docker=args.runtime == "container",
        ),
        "resource_limits": resource_limits(
            runtime=args.runtime, cpus=args.cpus, memory=args.memory
        ),
        "workload": build_workload(args),
        "backends": {
            "runnel-cluster": {
                "image": args.image if args.runtime == "container" else str(args.binary),
                "image_id": cluster.image_id,
                "runtime": args.runtime,
                "acknowledgement": "durable quorum commit",
                "replication": f"{args.nodes}-node static Multi-Raft",
                "measurement_boundary": "public line-delimited JSON protocol",
                "measurement_client": "scripts/benchmarks/cluster.py",
                "client_image": "host Python runtime",
                "peer_response_proxy": cluster.peer_proxy_summary(),
                "startup_seconds": cluster.startup_ns / 1_000_000_000,
                "resource_samples": cluster.stats.summary(),
                "scenarios": scenarios,
            }
        },
    }
