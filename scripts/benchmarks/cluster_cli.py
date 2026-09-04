#!/usr/bin/env python3
"""Benchmark a real three-node Runnel cluster through its public protocol.

This is a development baseline, not a production benchmark harness. It keeps
the workload and durability boundary explicit: every measured publish is sent
through the line-delimited JSON protocol and every delivery scenario includes
an acknowledgement. The broker uses the current static three-node Raft
backend with its normal durable storage.
"""

from __future__ import annotations

import argparse
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cluster_lifecycle import Cluster, build_binary
from cluster_results import build_result
from cluster_scenarios import (
    DEFAULT_COLD_KEY_COUNT,
    DEFAULT_COLD_MESSAGES_PER_KEY,
    DEFAULT_HOT_KEY_MESSAGES,
    DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
    DEFAULT_HOT_ORDERING_CONCURRENCY,
    DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
    DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
    DEFAULT_PEER_FORWARDING_CONCURRENCY,
    DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS,
    DEFAULT_PEER_RESPONSE_DELAY_MS,
    DEFAULT_PUBLISH_BATCH_SIZE,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_SCENARIOS,
    DEFAULT_SLOW_CONSUMER_DELAY_MS,
    MAX_HOT_KEY_PROCESSING_DELAY_MS,
    MAX_HOT_ORDERING_CONCURRENCY,
    MAX_HOT_ORDERING_MESSAGES,
    MAX_HOT_ORDERING_TIMEOUT_SECONDS,
    MAX_LEADER_FAILURE_TIMEOUT_SECONDS,
    MAX_PEER_FORWARDING_CONCURRENCY,
    MAX_PEER_FORWARDING_TIMEOUT_SECONDS,
    MAX_PEER_RESPONSE_DELAY_MS,
    MAX_PUBLISH_BATCH_SIZE,
    MIN_RETAINED_RECOVERY_MESSAGES,
    parse_retained_messages,
    parse_scenarios,
    run_consume_ack,
    run_durable_publish,
    run_follower_failure_recovery,
    run_grouped_consume_ack,
    run_hot_ordering,
    run_leader_failure_recovery,
    run_parallel_grouped,
    run_peer_forwarding,
    run_publish_batch,
    run_restart_recovery,
    run_retained_hot_path,
    run_retained_recovery,
    run_slow_consumer,
)
from common import (
    DEFAULT_TIMEOUT_SECONDS,
    ROOT,
    build_image,
    default_binary,
    parse_nonnegative_int,
    parse_sizes,
    write_json_result,
)


DEFAULT_BINARY = default_binary()
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 50
DEFAULT_NODES = 3
# Match the broker's default so constrained benchmark hosts do not turn slow
# but valid operations into redeliveries while measuring unrelated scenarios.
DEFAULT_ACK_TIMEOUT_MS = 30_000
COMMAND_TIMEOUT_SECONDS = DEFAULT_TIMEOUT_SECONDS


def parse_positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument(
        "--runtime",
        choices=("process", "container"),
        default="process",
        help="run native broker processes or bounded Docker broker containers",
    )
    parser.add_argument("--image", default="runnel:bench")
    parser.add_argument("--cpus", default="2", help="per-container CPU limit")
    parser.add_argument("--memory", default="2g", help="per-container memory limit")
    parser.add_argument("--build", action="store_true", help="build the selected broker artifact")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument(
        "--scenarios",
        type=parse_scenarios,
        default=list(DEFAULT_SCENARIOS),
        help=(
            "comma-separated scenarios to run (default: existing clustered workload; "
            "add retained_hot_path, peer_forwarding, publish_batch, hot_ordering, "
            "leader_failure_recovery, or follower_failure_recovery explicitly for "
            "focused probes)"
        ),
    )
    parser.add_argument("--ack-timeout-ms", type=int, default=DEFAULT_ACK_TIMEOUT_MS)
    parser.add_argument(
        "--slow-consumer-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_SLOW_CONSUMER_DELAY_MS,
        help="fixed processing delay before each slow-consumer acknowledgement",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_PUBLISH_BATCH_SIZE,
        help="records per publish_batch request for the opt-in publish_batch scenario",
    )
    parser.add_argument(
        "--hot-key-messages",
        type=int,
        default=DEFAULT_HOT_KEY_MESSAGES,
        help="preloaded records for the hot key in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--cold-key-count",
        type=int,
        default=DEFAULT_COLD_KEY_COUNT,
        help="number of independent cold keys in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--cold-messages-per-key",
        type=int,
        default=DEFAULT_COLD_MESSAGES_PER_KEY,
        help="preloaded records per cold key in the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--hot-ordering-concurrency",
        type=int,
        default=DEFAULT_HOT_ORDERING_CONCURRENCY,
        help="grouped workers for the opt-in hot_ordering scenario",
    )
    parser.add_argument(
        "--hot-key-processing-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
        help="processing delay before acknowledging hot-key records",
    )
    parser.add_argument(
        "--hot-ordering-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for each hot_ordering scenario",
    )
    parser.add_argument(
        "--retained-messages",
        type=parse_retained_messages,
        default=DEFAULT_RETAINED_RECOVERY_MESSAGES,
        help=(
            "retained records preloaded for the restart-recovery growth probe "
            f"(minimum: {MIN_RETAINED_RECOVERY_MESSAGES})"
        ),
    )
    parser.add_argument(
        "--peer-forwarding-concurrency",
        type=int,
        default=DEFAULT_PEER_FORWARDING_CONCURRENCY,
        help="concurrent persistent follower clients for the peer-forwarding scenario",
    )
    parser.add_argument(
        "--peer-response-delay-ms",
        type=parse_nonnegative_int,
        default=DEFAULT_PEER_RESPONSE_DELAY_MS,
        help=(
            "delay every framed peer response in the native-only focused probe; "
            "zero keeps direct peer connections"
        ),
    )
    parser.add_argument(
        "--peer-forwarding-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for each peer-forwarding scenario",
    )
    parser.add_argument(
        "--leader-failure-timeout-seconds",
        type=parse_positive_float,
        default=DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS,
        help="bounded wall-clock budget for the opt-in leader-failure scenario",
    )
    parser.add_argument("--payload-sizes", type=parse_sizes, default=[100, 1024])
    parser.add_argument(
        "--skip-recovery",
        action="store_true",
        help="skip restart and failure-recovery scenarios",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-dir", type=Path)
    args = parser.parse_args()
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3 or args.concurrency <= 0:
        parser.error(
            "messages, nodes, and concurrency must be positive; at least three nodes are required"
        )
    if args.peer_forwarding_concurrency <= 0:
        parser.error("peer forwarding concurrency must be positive")
    if args.peer_forwarding_concurrency > MAX_PEER_FORWARDING_CONCURRENCY:
        parser.error(
            "peer forwarding concurrency exceeds the bounded maximum "
            f"of {MAX_PEER_FORWARDING_CONCURRENCY}"
        )
    if args.batch_size <= 0 or args.batch_size > MAX_PUBLISH_BATCH_SIZE:
        parser.error(
            f"batch size must be between 1 and {MAX_PUBLISH_BATCH_SIZE} records"
        )
    if args.hot_key_messages <= 0:
        parser.error("hot-key messages must be positive")
    if args.cold_key_count <= 0:
        parser.error("cold-key count must be positive")
    if args.cold_messages_per_key <= 0:
        parser.error("cold messages per key must be positive")
    if args.hot_ordering_concurrency <= 0:
        parser.error("hot-ordering concurrency must be positive")
    if args.hot_ordering_concurrency > MAX_HOT_ORDERING_CONCURRENCY:
        parser.error(
            "hot-ordering concurrency exceeds the bounded maximum "
            f"of {MAX_HOT_ORDERING_CONCURRENCY}"
        )
    hot_ordering_messages = args.hot_key_messages + (
        args.cold_key_count * args.cold_messages_per_key
    )
    if hot_ordering_messages > MAX_HOT_ORDERING_MESSAGES:
        parser.error(
            "hot-ordering workload exceeds the bounded maximum of "
            f"{MAX_HOT_ORDERING_MESSAGES} records"
        )
    if args.hot_key_processing_delay_ms > MAX_HOT_KEY_PROCESSING_DELAY_MS:
        parser.error(
            "hot-key processing delay exceeds the bounded maximum "
            f"of {MAX_HOT_KEY_PROCESSING_DELAY_MS} ms"
        )
    if args.hot_ordering_timeout_seconds > MAX_HOT_ORDERING_TIMEOUT_SECONDS:
        parser.error(
            "hot-ordering timeout exceeds the bounded maximum "
            f"of {MAX_HOT_ORDERING_TIMEOUT_SECONDS:g} seconds"
        )
    if "hot_ordering" in args.scenarios and args.hot_ordering_concurrency < 2:
        parser.error("hot-ordering scenario requires at least two grouped workers")
    if (
        "hot_ordering" in args.scenarios
        and args.hot_key_processing_delay_ms >= args.ack_timeout_ms
    ):
        parser.error(
            "hot-key processing delay must be shorter than the acknowledgement timeout"
        )
    if args.peer_response_delay_ms > MAX_PEER_RESPONSE_DELAY_MS:
        parser.error(
            "peer response delay exceeds the bounded maximum "
            f"of {MAX_PEER_RESPONSE_DELAY_MS} ms"
        )
    if args.peer_forwarding_timeout_seconds > MAX_PEER_FORWARDING_TIMEOUT_SECONDS:
        parser.error(
            "peer forwarding timeout exceeds the bounded maximum "
            f"of {MAX_PEER_FORWARDING_TIMEOUT_SECONDS:g} seconds"
        )
    if args.leader_failure_timeout_seconds > MAX_LEADER_FAILURE_TIMEOUT_SECONDS:
        parser.error(
            "leader failure timeout exceeds the bounded maximum "
            f"of {MAX_LEADER_FAILURE_TIMEOUT_SECONDS:g} seconds"
        )
    if args.peer_response_delay_ms and args.runtime != "process":
        parser.error("peer response delay requires the native process runtime")
    if args.ack_timeout_ms <= 0:
        parser.error("ack timeout must be positive")
    if args.slow_consumer_delay_ms >= args.ack_timeout_ms:
        parser.error("slow consumer delay must be shorter than the acknowledgement timeout")
    return args


def run_scenarios(
    args: argparse.Namespace, cluster: Cluster, run_id: str
) -> list[dict[str, Any]]:
    """Dispatch selected scenarios while preserving the established run order."""
    scenarios: list[dict[str, Any]] = []
    selected_scenarios = set(args.scenarios)
    for size in args.payload_sizes:
        payload = "x" * size
        if "durable_publish" in selected_scenarios:
            scenarios.append(
                run_durable_publish(
                    cluster,
                    f"cluster_{run_id}_publish_{size}",
                    payload,
                    args.messages,
                    args.warmup,
                )
            )
        if "publish_batch" in selected_scenarios:
            scenarios.append(
                run_publish_batch(
                    cluster,
                    f"cluster_{run_id}_publish_batch_{size}",
                    payload,
                    args.messages,
                    args.warmup,
                    args.batch_size,
                )
            )
        if "consume_ack" in selected_scenarios:
            scenarios.append(
                run_consume_ack(
                    cluster,
                    f"cluster_{run_id}_consume_{size}",
                    payload,
                    args.messages,
                )
            )
        if "retained_hot_path" in selected_scenarios:
            scenarios.append(
                run_retained_hot_path(
                    cluster,
                    f"cluster_{run_id}_retained_hot_path_{size}",
                    payload,
                    args.messages,
                    args.retained_messages,
                )
            )
        if "slow_consumer" in selected_scenarios:
            scenarios.append(
                run_slow_consumer(
                    cluster,
                    f"cluster_{run_id}_slow_consumer_{size}",
                    payload,
                    args.messages,
                    args.slow_consumer_delay_ms,
                )
            )
        if "grouped_consume_ack" in selected_scenarios:
            scenarios.append(
                run_grouped_consume_ack(
                    cluster,
                    f"cluster_{run_id}_grouped_{size}",
                    payload,
                    args.messages,
                )
            )
        if "parallel_grouped_consume_ack" in selected_scenarios:
            scenarios.append(
                run_parallel_grouped(
                    cluster,
                    f"cluster_{run_id}_parallel_{size}",
                    payload,
                    args.messages,
                    args.concurrency,
                )
            )
        if "hot_ordering" in selected_scenarios:
            scenarios.append(
                run_hot_ordering(
                    cluster,
                    f"cluster_{run_id}_hot_ordering_{size}",
                    payload,
                    hot_key_messages=args.hot_key_messages,
                    cold_key_count=args.cold_key_count,
                    cold_messages_per_key=args.cold_messages_per_key,
                    concurrency=args.hot_ordering_concurrency,
                    processing_delay_ms=args.hot_key_processing_delay_ms,
                    timeout_seconds=args.hot_ordering_timeout_seconds,
                )
            )
        if "peer_forwarding" in selected_scenarios:
            scenarios.append(
                run_peer_forwarding(
                    cluster,
                    f"cluster_{run_id}_peer_forwarding_{size}",
                    payload,
                    args.messages,
                    args.warmup,
                    args.peer_forwarding_concurrency,
                    args.peer_forwarding_timeout_seconds,
                )
            )
        if not args.skip_recovery and size == args.payload_sizes[0]:
            if "restart_recovery" in selected_scenarios:
                scenarios.append(
                    run_restart_recovery(
                        cluster, f"cluster_{run_id}_recovery_{size}", payload
                    )
                )
            if "cluster_retained_recovery" in selected_scenarios:
                scenarios.append(
                    run_retained_recovery(
                        cluster,
                        f"cluster_{run_id}_retained_recovery_{size}",
                        payload,
                        args.retained_messages,
                    )
                )
            if "leader_failure_recovery" in selected_scenarios:
                scenarios.append(
                    run_leader_failure_recovery(
                        cluster,
                        f"cluster_{run_id}_leader_failure_{size}",
                        payload,
                        args.leader_failure_timeout_seconds,
                    )
                )
            if "follower_failure_recovery" in selected_scenarios:
                scenarios.append(
                    run_follower_failure_recovery(
                        cluster,
                        f"cluster_{run_id}_follower_failure_{size}",
                        payload,
                        args.leader_failure_timeout_seconds,
                    )
                )
    return scenarios


def main() -> int:
    args = parse_args()
    if args.build:
        if args.runtime == "container":
            build_image(args.image)
        else:
            build_binary(args.binary)
    timestamp = datetime.now(UTC)
    run_id = timestamp.strftime("%Y%m%d%H%M%S%f")
    output = args.output or ROOT / "benchmark-results" / f"cluster-{run_id}.json"
    cluster = Cluster(
        args.binary,
        node_count=args.nodes,
        ack_timeout_ms=args.ack_timeout_ms,
        log_dir=args.log_dir,
        runtime=args.runtime,
        image=args.image,
        cpus=args.cpus,
        memory=args.memory,
        peer_response_delay_ms=args.peer_response_delay_ms,
    )
    try:
        cluster.start()
        scenarios = run_scenarios(args, cluster, run_id)
    finally:
        cluster.close()

    result = build_result(
        args,
        run_id=run_id,
        started_at=timestamp,
        cluster=cluster,
        scenarios=scenarios,
    )
    write_json_result(output, result)
    return 0
