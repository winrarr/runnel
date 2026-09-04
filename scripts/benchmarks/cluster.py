#!/usr/bin/env python3
"""Benchmark a real three-node Runnel cluster through its public protocol."""

from cluster_cli import (
    COMMAND_TIMEOUT_SECONDS,
    DEFAULT_ACK_TIMEOUT_MS,
    DEFAULT_BINARY,
    DEFAULT_MESSAGES,
    DEFAULT_NODES,
    DEFAULT_WARMUP,
    main,
    parse_args,
    parse_positive_float,
)
from cluster_lifecycle import Cluster, Node, build_binary, free_port
from cluster_resources import resource_limits
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
)
from common import parse_nonnegative_int, parse_sizes


if __name__ == "__main__":
    raise SystemExit(main())
