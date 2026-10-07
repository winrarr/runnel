#!/usr/bin/env python3
"""Machine-readable result shaping for the clustered benchmark."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import TYPE_CHECKING, Any

from cluster_resources import resource_limits
from cluster_scenarios import (
    MAX_HOT_ORDERING_MESSAGES,
    MAX_PEER_FORWARDING_STREAM_COUNT,
    MAX_SNAPSHOT_BUILD_LOGICAL_PAYLOAD_BYTES,
    MAX_SNAPSHOT_BUILD_MESSAGES,
    MAX_SNAPSHOT_BUILD_RETAINED_MESSAGES,
    MIN_SNAPSHOT_BUILD_MESSAGES,
    MIN_RETAINED_RECOVERY_MESSAGES,
)
from cluster_scenarios import (
    MAX_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
    MAX_RAFT_LOG_GROWTH_LOGICAL_PAYLOAD_BYTES,
    MAX_RAFT_LOG_GROWTH_MESSAGES,
    MAX_RAFT_LOG_GROWTH_BATCH_SIZE,
    MIN_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
    MIN_RAFT_LOG_GROWTH_MESSAGES,
)
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
        "slow_consumer_timeout_seconds": args.slow_consumer_timeout_seconds,
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
        "protocol": "negotiated v2 Protobuf framing with opaque binary payloads",
        "protocol_version": "runnel-protobuf-v2",
        "application_transport_security": (
            "plaintext inside an isolated development container network"
            if args.runtime == "container"
            else "plaintext on loopback"
        ),
        "payload_encoding": "utf-8",
        "compression": "none",
        "durability": "committed by the current three-node Raft quorum and local durable state",
    }
    selected_scenarios = set(args.scenarios)
    if "parallel_grouped_consume_ack" in selected_scenarios:
        workload["parallel_grouped_timeout_seconds"] = (
            args.parallel_grouped_timeout_seconds
        )
        workload["parallel_grouped_timeout_source"] = (
            args.parallel_grouped_timeout_source
        )
    if not args.skip_recovery and "cluster_retained_recovery" in selected_scenarios:
        workload["retained_recovery_messages"] = args.retained_messages
    if "peer_forwarding" in selected_scenarios:
        workload["peer_forwarding"] = {
            "stream_count": args.peer_forwarding_stream_count,
            "data_group_count": args.peer_forwarding_stream_count,
            "maximum_stream_count": MAX_PEER_FORWARDING_STREAM_COUNT,
            "measured_messages_total": args.messages,
            "measured_messages_distribution": "round_robin_across_streams",
            "warmup_messages_per_stream": args.warmup,
            "setup_streams_created": args.peer_forwarding_stream_count,
            "setup_warmup_messages_total": (
                args.warmup * args.peer_forwarding_stream_count
            ),
            "setup_excluded_from_measurement": True,
        }
    if "retained_hot_path" in selected_scenarios:
        workload["retained_hot_path_messages"] = args.retained_messages
    if "raft_log_growth" in selected_scenarios:
        workload["raft_log_growth"] = {
            "measured_messages": args.raft_log_growth_messages,
            "batch_size": args.raft_log_growth_batch_size,
            "minimum_batch_size": 1,
            "maximum_batch_size": MAX_RAFT_LOG_GROWTH_BATCH_SIZE,
            "minimum_messages": MIN_RAFT_LOG_GROWTH_MESSAGES,
            "maximum_messages": MAX_RAFT_LOG_GROWTH_MESSAGES,
            "maximum_logical_payload_bytes": MAX_RAFT_LOG_GROWTH_LOGICAL_PAYLOAD_BYTES,
            "observation_every_publishes": args.raft_log_growth_observation_every,
            "cycle_timeout_seconds": args.raft_log_growth_cycle_timeout_seconds,
            "minimum_cycle_timeout_seconds": MIN_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
            "maximum_cycle_timeout_seconds": MAX_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
            "setup_messages_excluded": 1,
            "publish_operation": "publish_batch",
            "message_history_source": "public protocol; first setup publish is offset 0",
            "consensus_history_source": "per-node data-group raft-log.json",
        }
    if "snapshot_build_hot_path" in selected_scenarios:
        workload["snapshot_build_hot_path"] = {
            "measured_messages": args.snapshot_build_messages,
            "minimum_messages": MIN_SNAPSHOT_BUILD_MESSAGES,
            "maximum_messages": MAX_SNAPSHOT_BUILD_MESSAGES,
            "maximum_logical_payload_bytes": MAX_SNAPSHOT_BUILD_LOGICAL_PAYLOAD_BYTES,
            "retained_messages": args.retained_messages,
            "minimum_retained_messages": MIN_RETAINED_RECOVERY_MESSAGES,
            "maximum_retained_messages": MAX_SNAPSHOT_BUILD_RETAINED_MESSAGES,
            "cycle_timeout_seconds": args.snapshot_build_cycle_timeout_seconds,
            "setup_messages_excluded": True,
            "retained_state_source": "public durable publishes before measured interval",
            "measurement_boundary": "public durable publish through snapshot build completion",
        }
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
                "measurement_boundary": "public negotiated v2 Protobuf protocol",
                "measurement_client": "scripts/benchmarks/cluster.py",
                "client_image": "host Python runtime",
                "peer_response_proxy": cluster.peer_proxy_summary(),
                "startup_seconds": cluster.startup_ns / 1_000_000_000,
                "resource_samples": cluster.stats.summary(),
                "scenarios": scenarios,
            }
        },
    }
