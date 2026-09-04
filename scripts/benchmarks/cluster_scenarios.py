#!/usr/bin/env python3
"""Workload scenarios and failure/recovery probes for the clustered benchmark."""

from __future__ import annotations

import argparse
import base64
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from common import (
    BenchmarkError,
    LineClient,
    acknowledge,
    consume_ack_messages,
    create_stream,
    measure_message_batch,
    measure_scenario,
    metric,
    percentile,
    poll,
    publish,
    publish_messages,
    publish_stream,
    request_ok,
    DEFAULT_TIMEOUT_SECONDS,
)

if TYPE_CHECKING:
    from cluster import Cluster


DEFAULT_SLOW_CONSUMER_DELAY_MS = 10
DEFAULT_PUBLISH_BATCH_SIZE = 32
MAX_PUBLISH_BATCH_SIZE = 1_024
HOT_ORDERING_HOT_KEY = "hot-key"
DEFAULT_HOT_KEY_MESSAGES = 64
DEFAULT_COLD_KEY_COUNT = 4
DEFAULT_COLD_MESSAGES_PER_KEY = 8
DEFAULT_HOT_ORDERING_CONCURRENCY = 4
DEFAULT_HOT_KEY_PROCESSING_DELAY_MS = 5
DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS = 60.0
MAX_HOT_ORDERING_CONCURRENCY = 128
MAX_HOT_KEY_PROCESSING_DELAY_MS = 5_000
MAX_HOT_ORDERING_TIMEOUT_SECONDS = 300.0
MAX_HOT_ORDERING_MESSAGES = 4_096
# The topology-free forwarding pool currently reserves one control lane from
# five total connections, leaving four shared forwarding lanes. Keep the
# focused workload above that boundary by default so queueing is observable.
DEFAULT_PEER_FORWARDING_CONCURRENCY = 8
DEFAULT_PEER_RESPONSE_DELAY_MS = 0
DEFAULT_PEER_FORWARDING_TIMEOUT_SECONDS = 60.0
MAX_PEER_FORWARDING_CONCURRENCY = 128
MAX_PEER_RESPONSE_DELAY_MS = 2_000
MAX_PEER_FORWARDING_TIMEOUT_SECONDS = 300.0
PEER_FORWARDING_INGRESS_NODE_INDEX = 1
DEFAULT_LEADER_FAILURE_TIMEOUT_SECONDS = 60.0
MAX_LEADER_FAILURE_TIMEOUT_SECONDS = 300.0
DEFAULT_SCENARIOS = (
    "durable_publish",
    "consume_ack",
    "slow_consumer",
    "grouped_consume_ack",
    "parallel_grouped_consume_ack",
    "restart_recovery",
    "cluster_retained_recovery",
)
SCENARIO_NAMES = (
    *DEFAULT_SCENARIOS,
    "retained_hot_path",
    "peer_forwarding",
    "publish_batch",
    "hot_ordering",
    "leader_failure_recovery",
    "follower_failure_recovery",
)
# Keep the retained-data probe beyond the local engine's bounded tail index so
# recovery measurements exercise a non-trivial retained history.
MIN_RETAINED_RECOVERY_MESSAGES = 1_025
DEFAULT_RETAINED_RECOVERY_MESSAGES = 2_048


@dataclass
class HotOrderingObservation:
    """Collect bounded delivery observations for the hot-ordering probe."""

    expected_offsets_by_key: dict[str, list[int]]
    hot_key: str
    hot_key_messages: int
    delivered_offsets_by_key: dict[str, list[int]]
    completed_offsets_by_key: dict[str, list[int]]
    delivery_wait_ns_by_key: dict[str, list[int]]
    request_latency_ns_by_key: dict[str, list[int]]
    completion_elapsed_ns_by_key: dict[str, list[int]]
    delivered_attempts: dict[int, int]
    completed_offsets: set[int]
    active_offsets: set[int]
    processing_offsets: set[int]
    ack_started_offsets: set[int]
    active_by_key: dict[str, int]
    max_in_flight_by_key: dict[str, int]
    request_latencies_ns: list[int]
    hot_backlog_at_delivery: list[int]
    hot_backlog_at_cold_completion: list[int]
    cold_keys_with_progress_while_hot_backlog: set[str]
    cold_keys_completed_while_hot_backlog: set[str]
    hot_completed_messages: int = 0
    hot_drained_elapsed_ns: int | None = None
    max_in_flight_messages: int = 0

    @classmethod
    def for_records(
        cls, records: list[tuple[int, str]], hot_key: str
    ) -> "HotOrderingObservation":
        expected: dict[str, list[int]] = {}
        for offset, key in records:
            expected.setdefault(key, []).append(offset)
        return cls(
            expected_offsets_by_key=expected,
            hot_key=hot_key,
            hot_key_messages=len(expected.get(hot_key, [])),
            delivered_offsets_by_key={key: [] for key in expected},
            completed_offsets_by_key={key: [] for key in expected},
            delivery_wait_ns_by_key={key: [] for key in expected},
            request_latency_ns_by_key={key: [] for key in expected},
            completion_elapsed_ns_by_key={key: [] for key in expected},
            delivered_attempts={},
            completed_offsets=set(),
            active_offsets=set(),
            processing_offsets=set(),
            ack_started_offsets=set(),
            active_by_key={key: 0 for key in expected},
            max_in_flight_by_key={key: 0 for key in expected},
            request_latencies_ns=[],
            hot_backlog_at_delivery=[],
            hot_backlog_at_cold_completion=[],
            cold_keys_with_progress_while_hot_backlog=set(),
            cold_keys_completed_while_hot_backlog=set(),
        )

    def record_delivery(
        self,
        *,
        offset: int,
        key: str,
        delivery_attempt: int,
        delivery_wait_ns: int,
    ) -> None:
        expected_offsets = self.expected_offsets_by_key.get(key)
        if expected_offsets is None or offset not in expected_offsets:
            raise BenchmarkError(
                f"hot-ordering delivery returned unexpected key/offset: {key!r}/{offset}"
            )
        if delivery_attempt != 1:
            raise BenchmarkError(
                "hot-ordering workload observed an unexpected redelivery at "
                f"offset {offset} (attempt {delivery_attempt})"
            )
        if offset in self.delivered_attempts:
            raise BenchmarkError(
                f"hot-ordering workload delivered offset {offset} more than once"
            )
        observed_offsets = self.delivered_offsets_by_key[key]
        if observed_offsets and offset <= observed_offsets[-1]:
            raise BenchmarkError(
                f"hot-ordering key {key!r} was delivered out of order: "
                f"{observed_offsets[-1]} then {offset}"
            )
        observed_offsets.append(offset)
        self.delivered_attempts[offset] = delivery_attempt
        self.delivery_wait_ns_by_key[key].append(delivery_wait_ns)
        self.active_offsets.add(offset)
        self.processing_offsets.add(offset)
        self.active_by_key[key] += 1
        self.max_in_flight_by_key[key] = max(
            self.max_in_flight_by_key[key], self.active_by_key[key]
        )
        self.max_in_flight_messages = max(
            self.max_in_flight_messages, len(self.processing_offsets)
        )
        if key == self.hot_key:
            self.hot_backlog_at_delivery.append(
                self.hot_key_messages - self.hot_completed_messages
            )

    def record_ack_start(self, *, offset: int, key: str) -> None:
        if offset not in self.processing_offsets:
            raise BenchmarkError(
                f"hot-ordering workload acknowledged offset {offset} without processing it"
            )
        self.processing_offsets.remove(offset)
        self.active_by_key[key] -= 1
        self.ack_started_offsets.add(offset)

    def record_completion(
        self,
        *,
        offset: int,
        key: str,
        request_latency_ns: int,
        completion_elapsed_ns: int,
    ) -> None:
        if offset not in self.active_offsets or offset not in self.ack_started_offsets:
            raise BenchmarkError(
                f"hot-ordering workload completed offset {offset} without an acknowledgement"
            )
        self.active_offsets.remove(offset)
        self.ack_started_offsets.remove(offset)
        self.completed_offsets.add(offset)
        self.completed_offsets_by_key[key].append(offset)
        self.request_latencies_ns.append(request_latency_ns)
        self.request_latency_ns_by_key[key].append(request_latency_ns)
        self.completion_elapsed_ns_by_key[key].append(completion_elapsed_ns)
        if key == self.hot_key:
            self.hot_completed_messages += 1
            if self.hot_completed_messages == self.hot_key_messages:
                self.hot_drained_elapsed_ns = completion_elapsed_ns
            return
        hot_backlog = self.hot_key_messages - self.hot_completed_messages
        if hot_backlog > 0:
            self.hot_backlog_at_cold_completion.append(hot_backlog)
            self.cold_keys_with_progress_while_hot_backlog.add(key)
            if len(self.completed_offsets_by_key[key]) == len(
                self.expected_offsets_by_key[key]
            ):
                self.cold_keys_completed_while_hot_backlog.add(key)


def poll_until_redelivered(
    client: LineClient, stream: str, consumer: str, expected_offset: int
) -> tuple[dict[str, Any], int]:
    """Wait for an expired unacknowledged message without assuming a margin."""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SECONDS
    attempts = 0
    last_response: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        attempts += 1
        response, elapsed = client.request(
            {"op": "poll", "stream": stream, "consumer": consumer}
        )
        last_response = response
        if response.get("type") == "empty":
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.05, remaining))
            continue
        if response.get("type") != "message":
            raise BenchmarkError(f"unexpected recovery poll response: {response}")
        if response.get("offset") != expected_offset:
            raise BenchmarkError(f"expected recovery offset {expected_offset}, got {response}")
        if response.get("delivery_attempt") != 2:
            raise BenchmarkError(f"expected recovery delivery attempt 2, got {response}")
        return response, attempts
    raise BenchmarkError(
        f"message at offset {expected_offset} was not redelivered before the deadline; "
        f"last response: {last_response}"
    )


def poll_group(
    client: LineClient, stream: str, consumer: str, member: str
) -> tuple[dict[str, Any], int]:
    response, elapsed = request_ok(
        client,
        {"op": "poll_group", "stream": stream, "consumer": consumer, "member": member},
        "message",
    )
    return response, elapsed


def acknowledge_group(
    client: LineClient,
    stream: str,
    consumer: str,
    member: str,
    offset: int,
    token: str,
) -> int:
    _, elapsed = request_ok(
        client,
        {
            "op": "ack_group",
            "stream": stream,
            "consumer": consumer,
            "member": member,
            "offset": offset,
            "delivery_token": token,
        },
        "acknowledged",
    )
    return elapsed


def run_durable_publish(
    cluster: Cluster, stream: str, payload: str, messages: int, warmup: int
) -> dict[str, Any]:
    setup = cluster.client(0)
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()
    with cluster.connected_clients() as clients:
        return measure_message_batch(
            cluster.stats,
            "cluster_durable_publish",
            len(payload),
            lambda: publish_messages(
                lambda offset: clients[offset % len(clients)],
                stream,
                payload,
                messages,
                expected_offset=warmup,
            ),
            metadata={"nodes": cluster.node_count, "any_node_routing": True},
            metrics=cluster.metrics,
        )


def preload(cluster: Cluster, stream: str, payload: str, messages: int) -> None:
    client = cluster.client(0)
    try:
        publish_stream(client, stream, payload, messages)
    finally:
        client.close()


def run_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
        return measure_message_batch(
            cluster.stats,
            "cluster_consume_ack",
            len(payload),
            lambda: consume_ack_messages(
                lambda offset: clients[offset % len(clients)],
                stream,
                "cluster-consumer",
                messages,
                ack_client_for=lambda offset: clients[(offset + 1) % len(clients)],
            ),
            metadata={"nodes": cluster.node_count, "publish_setup_excluded": True},
            metrics=cluster.metrics,
        )


def run_retained_hot_path(
    cluster: Cluster, stream: str, payload: str, messages: int, retained_messages: int
) -> dict[str, Any]:
    """Measure durable publishes after preloading a retained history.

    The retained-history preload is deliberately excluded from measurement so
    the result isolates the append hot path after the stream already contains
    a controlled amount of retained state.
    """
    preload(cluster, stream, payload, retained_messages)
    with cluster.connected_clients() as clients:
        return measure_message_batch(
            cluster.stats,
            "cluster_retained_hot_path",
            len(payload),
            lambda: publish_messages(
                lambda offset: clients[offset % len(clients)],
                stream,
                payload,
                messages,
                expected_offset=retained_messages,
            ),
            metadata={
                "nodes": cluster.node_count,
                "retained_messages": retained_messages,
                "retained_logical_payload_bytes": retained_messages * len(payload),
                "publish_setup_excluded": True,
                "preload_scope": "same stream and payload before measured publishes",
                "latency_scope": "one public durable publish roundtrip per sample",
                "throughput_scope": "measured publishes after retained-history preload",
            },
            metrics=cluster.metrics,
        )


def publish_batch_request(
    client: LineClient,
    stream: str,
    payload: str,
    batch_size: int,
    expected_offset: int,
) -> tuple[int, int]:
    """Publish one public batch and validate every per-record outcome."""
    encoded_payload = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    response, elapsed = client.request(
        {
            "op": "publish_batch",
            "stream": stream,
            "records": [
                {"key": None, "payload_base64": encoded_payload}
                for _ in range(batch_size)
            ],
        }
    )
    if response.get("type") != "publish_batch":
        raise BenchmarkError(f"unexpected publish batch response: {response}")
    outcomes = response.get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != batch_size:
        raise BenchmarkError(
            f"publish batch returned {len(outcomes) if isinstance(outcomes, list) else 'no'} "
            f"outcomes for {batch_size} records"
        )
    offsets: list[int] = []
    for index, outcome in enumerate(outcomes):
        if not isinstance(outcome, dict) or outcome.get("type") != "published":
            raise BenchmarkError(
                f"publish batch record {index} did not publish: {outcome}"
            )
        offset = outcome.get("offset")
        if not isinstance(offset, int) or offset != expected_offset + index:
            raise BenchmarkError(
                f"publish batch returned unexpected offset at record {index}: {outcome}"
            )
        offsets.append(offset)
    return len(offsets), elapsed


def batch_metric(
    operation: str,
    batch_latencies_ns: list[int],
    elapsed_ns: int,
    *,
    messages: int,
    message_size: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """Report record throughput while retaining one latency sample per batch."""
    result = metric(
        operation,
        batch_latencies_ns,
        elapsed_ns,
        message_size=message_size,
        metadata=metadata,
    )
    elapsed_seconds = elapsed_ns / 1_000_000_000
    result["messages"] = messages
    result["throughput_messages_per_second"] = messages / elapsed_seconds
    result["throughput_megabytes_per_second"] = (
        messages * message_size / elapsed_seconds / 1_000_000
    )
    return result


def run_publish_batch(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    warmup: int,
    batch_size: int,
) -> dict[str, Any]:
    """Measure clustered public publish_batch round trips and outcomes."""
    setup = cluster.client(0)
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()

    with cluster.connected_clients() as clients:
        def operation() -> dict[str, Any]:
            batch_latencies: list[int] = []
            published_messages = 0
            started = time.perf_counter_ns()
            while published_messages < messages:
                current_batch_size = min(batch_size, messages - published_messages)
                client = clients[len(batch_latencies) % len(clients)]
                published, elapsed = publish_batch_request(
                    client,
                    stream,
                    payload,
                    current_batch_size,
                    warmup + published_messages,
                )
                published_messages += published
                batch_latencies.append(elapsed)
            return batch_metric(
                "cluster_publish_batch",
                batch_latencies,
                time.perf_counter_ns() - started,
                messages=published_messages,
                message_size=len(payload),
                metadata={
                    "nodes": cluster.node_count,
                    "batch_size": batch_size,
                    "batches": len(batch_latencies),
                    "any_node_routing": True,
                    "setup_excluded": True,
                    "outcome_scope": "every input record returned a contiguous published outcome",
                    "latency_scope": "one public publish_batch roundtrip per sample",
                    "latency_sample_scope": "batch_requests",
                },
            )

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_slow_consumer(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    processing_delay_ms: int,
) -> dict[str, Any]:
    """Drain a bounded preloaded backlog with a fixed delay before each ack.

    The intentional processing delay is excluded from request latency samples
    but included in drain throughput. This keeps broker request latency
    comparable with the normal consume/ack scenario while making the slow
    consumer condition explicit in the result metadata.
    """
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
        def operation() -> dict[str, Any]:
            request_latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                client = clients[offset % len(clients)]
                response, poll_elapsed = poll(client, stream, "slow-consumer", offset)
                time.sleep(processing_delay_ms / 1_000)
                ack_elapsed = acknowledge(client, stream, "slow-consumer", offset)
                request_latencies.append(poll_elapsed + ack_elapsed)
                if response.get("payload") != payload:
                    raise BenchmarkError(f"slow consumer received unexpected payload at offset {offset}")
            result = metric(
                "cluster_slow_consumer",
                request_latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={
                    "nodes": cluster.node_count,
                    "processing_delay_ms": processing_delay_ms,
                    "preloaded_messages": messages,
                    "publish_setup_excluded": True,
                    "redelivery_expected": False,
                    "latency_scope": "poll_and_ack_request_time_excludes_processing_delay",
                    "throughput_scope": "preloaded_backlog_drain_includes_processing_delay",
                },
            )
            return result

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_grouped_consume_ack(
    cluster: Cluster, stream: str, payload: str, messages: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    with cluster.connected_clients() as clients:
        def operation() -> dict[str, Any]:
            latencies: list[int] = []
            started = time.perf_counter_ns()
            for offset in range(messages):
                member = "member-a" if offset % 2 == 0 else "member-b"
                poll_client = clients[offset % len(clients)]
                ack_client = clients[(offset + 1) % len(clients)]
                roundtrip_started = time.perf_counter_ns()
                response, _ = poll_group(poll_client, stream, "cluster-workers", member)
                if response.get("offset") != offset:
                    raise BenchmarkError(f"expected grouped offset {offset}, got {response}")
                acknowledge_group(
                    ack_client,
                    stream,
                    "cluster-workers",
                    member,
                    offset,
                    str(response["delivery_token"]),
                )
                latencies.append(time.perf_counter_ns() - roundtrip_started)
            return metric(
                "cluster_grouped_consume_ack",
                latencies,
                time.perf_counter_ns() - started,
                message_size=len(payload),
                metadata={"nodes": cluster.node_count, "members": 2, "parallel": False},
            )

        return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_parallel_grouped(
    cluster: Cluster, stream: str, payload: str, messages: int, concurrency: int
) -> dict[str, Any]:
    preload(cluster, stream, payload, messages)
    lock = threading.Lock()
    latencies: list[int] = []
    processed = 0
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SECONDS

    def worker(worker_index: int) -> None:
        nonlocal processed
        client = cluster.client(worker_index)
        member = f"parallel-member-{worker_index}"
        try:
            while True:
                with lock:
                    if processed >= messages:
                        return
                if time.monotonic() >= deadline:
                    raise BenchmarkError("parallel grouped benchmark did not drain its messages")
                started = time.perf_counter_ns()
                response, _ = client.request(
                    {
                        "op": "poll_group",
                        "stream": stream,
                        "consumer": "parallel-workers",
                        "member": member,
                    }
                )
                if response.get("type") == "empty":
                    time.sleep(0.001)
                    continue
                if response.get("type") != "message":
                    raise BenchmarkError(f"unexpected parallel poll response: {response}")
                acknowledge_group(
                    client,
                    stream,
                    "parallel-workers",
                    member,
                    int(response["offset"]),
                    str(response["delivery_token"]),
                )
                with lock:
                    latencies.append(time.perf_counter_ns() - started)
                    processed += 1
        finally:
            client.close()

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, index) for index in range(concurrency)]
            for future in futures:
                future.result()
        if len(latencies) != messages:
            raise BenchmarkError(f"parallel grouped benchmark processed {len(latencies)} of {messages}")
        return metric(
            "cluster_parallel_grouped_consume_ack",
            latencies,
            time.perf_counter_ns() - started,
            message_size=len(payload),
            metadata={"nodes": cluster.node_count, "members": concurrency, "parallel": True},
        )

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def hot_ordering_records(
    hot_key_messages: int,
    cold_key_count: int,
    cold_messages_per_key: int,
    *,
    hot_key: str = HOT_ORDERING_HOT_KEY,
) -> list[tuple[int, str]]:
    """Build the deterministic interleaved record schedule for the probe."""
    if hot_key_messages <= 0 or cold_key_count <= 0 or cold_messages_per_key <= 0:
        raise BenchmarkError("hot-ordering workload dimensions must be positive")
    total_messages = hot_key_messages + cold_key_count * cold_messages_per_key
    if total_messages > MAX_HOT_ORDERING_MESSAGES:
        raise BenchmarkError(
            "hot-ordering workload exceeds the bounded maximum of "
            f"{MAX_HOT_ORDERING_MESSAGES} records"
        )
    records: list[tuple[int, str]] = []
    offset = 0
    rounds = max(hot_key_messages, cold_messages_per_key)
    for round_index in range(rounds):
        if round_index < hot_key_messages:
            records.append((offset, hot_key))
            offset += 1
        if round_index < cold_messages_per_key:
            for cold_key_index in range(cold_key_count):
                records.append((offset, f"cold-key-{cold_key_index}"))
                offset += 1
    return records


def publish_keyed(
    client: LineClient,
    stream: str,
    key: str,
    payload: str,
    expected_offset: int,
) -> tuple[int, int]:
    """Publish one keyed record through the public protocol and check its offset."""
    response, elapsed = request_ok(
        client,
        {"op": "publish", "stream": stream, "key": key, "payload": payload},
        "published",
    )
    offset = response.get("offset")
    if offset != expected_offset:
        raise BenchmarkError(
            f"hot-ordering setup returned offset {offset}, expected {expected_offset}"
        )
    return int(offset), elapsed


def _duration_summary_milliseconds(values_ns: list[int]) -> dict[str, Any]:
    if not values_ns:
        return {"samples": 0}
    return {
        "samples": len(values_ns),
        "p50_milliseconds": percentile(values_ns, 50) / 1_000_000,
        "p99_milliseconds": percentile(values_ns, 99) / 1_000_000,
        "max_milliseconds": max(values_ns) / 1_000_000,
    }


def _hot_ordering_metadata(
    observation: HotOrderingObservation,
    *,
    records: list[tuple[int, str]],
    cluster: Cluster,
    concurrency: int,
    processing_delay_ms: int,
    timeout_seconds: float,
    operation_elapsed_ns: int,
) -> dict[str, Any]:
    expected_offsets = observation.expected_offsets_by_key
    key_metrics: dict[str, Any] = {}
    strict_delivery_order_verified = True
    strict_completion_order_verified = True
    same_key_exclusion_verified = True
    for key, expected in expected_offsets.items():
        observed = observation.delivered_offsets_by_key[key]
        completed = observation.completed_offsets_by_key[key]
        ordering_verified = observed == expected
        strict_delivery_order_verified &= ordering_verified
        completion_order_verified = completed == expected
        strict_completion_order_verified &= completion_order_verified
        same_key_exclusion_verified &= observation.max_in_flight_by_key[key] <= 1
        completions = observation.completion_elapsed_ns_by_key[key]
        key_metrics[key] = {
            "role": "hot" if key == observation.hot_key else "cold",
            "messages": len(expected),
            "delivered_messages": len(observed),
            "completed_messages": len(completions),
            "expected_offsets": expected,
            "observed_delivery_offsets": observed,
            "delivery_order_verified": ordering_verified,
            "observed_completion_offsets": completed,
            "completion_order_verified": completion_order_verified,
            "same_key_processing_overlap_verified": (
                observation.max_in_flight_by_key[key] <= 1
            ),
            "delivery_wait": _duration_summary_milliseconds(
                observation.delivery_wait_ns_by_key[key]
            ),
            "request_latency": _duration_summary_milliseconds(
                observation.request_latency_ns_by_key[key]
            ),
            "completion_elapsed": _duration_summary_milliseconds(completions),
            "first_completion_elapsed_milliseconds": (
                min(completions) / 1_000_000 if completions else None
            ),
            "last_completion_elapsed_milliseconds": (
                max(completions) / 1_000_000 if completions else None
            ),
            "max_in_flight": observation.max_in_flight_by_key[key],
        }

    cold_keys = [key for key in expected_offsets if key != observation.hot_key]
    cold_first_completions = [
        min(observation.completion_elapsed_ns_by_key[key])
        for key in cold_keys
        if observation.completion_elapsed_ns_by_key[key]
    ]
    cold_last_completions = [
        max(observation.completion_elapsed_ns_by_key[key])
        for key in cold_keys
        if observation.completion_elapsed_ns_by_key[key]
    ]
    cold_first_completion_spread_ms = (
        (max(cold_first_completions) - min(cold_first_completions)) / 1_000_000
        if cold_first_completions
        else None
    )
    cold_last_completion_spread_ms = (
        (max(cold_last_completions) - min(cold_last_completions)) / 1_000_000
        if cold_last_completions
        else None
    )
    hot_backlog_at_cold_completion = observation.hot_backlog_at_cold_completion
    return {
        "nodes": cluster.node_count,
        "records": len(records),
        "hot_key": observation.hot_key,
        "hot_key_messages": observation.hot_key_messages,
        "cold_key_count": len(cold_keys),
        "cold_messages": sum(len(expected_offsets[key]) for key in cold_keys),
        "configured_workers": concurrency,
        "hot_key_processing_delay_ms": processing_delay_ms,
        "bounded_runtime_seconds": timeout_seconds,
        "mixed_workload_schedule": (
            "each round publishes one hot-key record followed by one record for "
            "each cold key; all records are preloaded before measurement"
        ),
        "setup_excluded": True,
        "redelivery_expected": False,
        "latency_scope": (
            "poll_and_ack_request_time_excludes_the_configured_processing_delay"
        ),
        "throughput_scope": (
            "concurrent_preloaded_grouped_backlog_drain_includes_processing_delay"
        ),
        "per_key_ordering": {
            "verified": (
                strict_delivery_order_verified
                and strict_completion_order_verified
                and same_key_exclusion_verified
            ),
            "verification": (
                "observed delivery and acknowledgement-completion offsets exactly "
                "match published offsets per key"
            ),
            "expected_offsets_by_key": expected_offsets,
            "observed_delivery_offsets_by_key": observation.delivered_offsets_by_key,
            "observed_completion_offsets_by_key": observation.completed_offsets_by_key,
            "delivery_order_verified": strict_delivery_order_verified,
            "completion_order_verified": strict_completion_order_verified,
            "same_key_processing_overlap_verified": same_key_exclusion_verified,
        },
        "key_metrics": key_metrics,
        "hot_key_backlog": {
            "definition": "preloaded hot-key records not yet durably acknowledged",
            "initial_messages": observation.hot_key_messages,
            "peak_messages": max(
                observation.hot_backlog_at_delivery,
                default=observation.hot_key_messages,
            ),
            "samples_at_hot_delivery": observation.hot_backlog_at_delivery,
            "at_first_cold_completion": (
                hot_backlog_at_cold_completion[0]
                if hot_backlog_at_cold_completion
                else None
            ),
            "at_last_cold_completion": (
                hot_backlog_at_cold_completion[-1]
                if hot_backlog_at_cold_completion
                else None
            ),
            "samples_at_cold_completion": hot_backlog_at_cold_completion,
            "drained": observation.hot_completed_messages
            == observation.hot_key_messages,
            "drained_elapsed_milliseconds": (
                observation.hot_drained_elapsed_ns / 1_000_000
                if observation.hot_drained_elapsed_ns is not None
                else None
            ),
        },
        "unrelated_key_progress": {
            "definition": "cold-key acknowledgements completed while any hot-key record remained unacknowledged",
            "cold_messages_completed_before_hot_drained": len(
                hot_backlog_at_cold_completion
            ),
            "cold_keys_with_progress_before_hot_drained": len(
                observation.cold_keys_with_progress_while_hot_backlog
            ),
            "cold_keys_with_progress_before_hot_drained_names": sorted(
                observation.cold_keys_with_progress_while_hot_backlog
            ),
            "cold_keys_completed_before_hot_drained": len(
                observation.cold_keys_completed_while_hot_backlog
            ),
            "cold_keys_completed_before_hot_drained_names": sorted(
                observation.cold_keys_completed_while_hot_backlog
            ),
            "cold_key_first_completion_spread_milliseconds": cold_first_completion_spread_ms,
            "cold_key_last_completion_spread_milliseconds": cold_last_completion_spread_ms,
            "fairness": {
                "definition": (
                    "descriptive spread of first and last completion times across "
                    "cold keys; lower spread indicates closer timing"
                ),
                "first_completion_spread_milliseconds": cold_first_completion_spread_ms,
                "last_completion_spread_milliseconds": cold_last_completion_spread_ms,
            },
        },
        "delivery_concurrency": {
            "max_processing_in_flight_messages": observation.max_in_flight_messages,
            "max_processing_in_flight_by_key": observation.max_in_flight_by_key,
            "interpretation": (
                "observed client-side processing slots from delivery through the "
                "start of the acknowledgement request; it does not claim a broker "
                "scheduling policy"
            ),
        },
        "resource_measurement": {
            "scope": (
                "scenario resource_samples cover the grouped poll/ack drain, "
                "including configured hot-key processing delay"
            ),
            "dimensions": [
                "resource_samples.cpu_seconds",
                "resource_samples.memory_bytes_avg",
                "resource_samples.memory_bytes_max",
                "resource_samples.storage_bytes_avg",
                "resource_samples.storage_bytes_max",
                "resource_samples.per_node.*",
            ],
            "server_metrics": "scenario-scoped GET /metrics delta when all node endpoints are available",
        },
        "scheduling_observation": (
            "worker scheduling and timing are runtime-dependent; this result does "
            "not imply adaptive scheduling or prove a performance improvement"
        ),
        "operation_elapsed_milliseconds": operation_elapsed_ns / 1_000_000,
    }


def run_hot_ordering(
    cluster: Cluster,
    stream: str,
    payload: str,
    *,
    hot_key_messages: int,
    cold_key_count: int,
    cold_messages_per_key: int,
    concurrency: int,
    processing_delay_ms: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Measure a bounded mixed-key grouped-consumer drain through the public protocol."""
    records = hot_ordering_records(
        hot_key_messages,
        cold_key_count,
        cold_messages_per_key,
    )
    setup = cluster.client(0)
    try:
        create_stream(setup, stream)
        for offset, key in records:
            publish_keyed(setup, stream, key, payload, offset)
    finally:
        setup.close()

    total_messages = len(records)
    client_timeout = min(DEFAULT_TIMEOUT_SECONDS, timeout_seconds)
    stop_event = threading.Event()
    observation_lock = threading.Lock()

    def operation() -> dict[str, Any]:
        observation = HotOrderingObservation.for_records(
            records, HOT_ORDERING_HOT_KEY
        )
        operation_started_ns = time.perf_counter_ns()
        deadline = time.monotonic() + timeout_seconds

        def worker(worker_index: int) -> None:
            client = cluster.client(
                worker_index % cluster.node_count,
                timeout_seconds=client_timeout,
            )
            member = f"hot-ordering-member-{worker_index}"
            try:
                while not stop_event.is_set():
                    with observation_lock:
                        if len(observation.completed_offsets) >= total_messages:
                            return
                    if time.monotonic() >= deadline:
                        raise BenchmarkError(
                            "hot-ordering benchmark exceeded its bounded runtime"
                        )
                    response, poll_elapsed = client.request(
                        {
                            "op": "poll_group",
                            "stream": stream,
                            "consumer": "hot-ordering-workers",
                            "member": member,
                        }
                    )
                    if response.get("type") == "empty":
                        remaining = deadline - time.monotonic()
                        if remaining > 0:
                            time.sleep(min(0.001, remaining))
                        continue
                    if response.get("type") != "message":
                        raise BenchmarkError(
                            f"unexpected hot-ordering poll response: {response}"
                        )
                    offset = response.get("offset")
                    key = response.get("key")
                    token = response.get("delivery_token")
                    delivery_attempt = response.get("delivery_attempt")
                    if not isinstance(offset, int) or not isinstance(key, str):
                        raise BenchmarkError(
                            f"hot-ordering poll omitted offset or key: {response}"
                        )
                    if not isinstance(token, str) or not isinstance(delivery_attempt, int):
                        raise BenchmarkError(
                            f"hot-ordering poll omitted delivery fencing fields: {response}"
                        )
                    if response.get("payload") != payload:
                        raise BenchmarkError(
                            f"hot-ordering poll returned an unexpected payload at offset {offset}"
                        )
                    delivered_ns = time.perf_counter_ns()
                    with observation_lock:
                        observation.record_delivery(
                            offset=offset,
                            key=key,
                            delivery_attempt=delivery_attempt,
                            delivery_wait_ns=delivered_ns - operation_started_ns,
                        )
                    if key == HOT_ORDERING_HOT_KEY and processing_delay_ms:
                        time.sleep(processing_delay_ms / 1_000)
                    with observation_lock:
                        observation.record_ack_start(offset=offset, key=key)
                    ack_elapsed = acknowledge_group(
                        client,
                        stream,
                        "hot-ordering-workers",
                        member,
                        offset,
                        token,
                    )
                    completed_ns = time.perf_counter_ns()
                    with observation_lock:
                        observation.record_completion(
                            offset=offset,
                            key=key,
                            request_latency_ns=poll_elapsed + ack_elapsed,
                            completion_elapsed_ns=completed_ns - operation_started_ns,
                        )
            except BaseException:
                stop_event.set()
                raise
            finally:
                client.close()

        try:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(worker, worker_index)
                    for worker_index in range(concurrency)
                ]
                for future in futures:
                    future.result()
        finally:
            stop_event.set()

        if len(observation.completed_offsets) != total_messages:
            raise BenchmarkError(
                "hot-ordering benchmark processed "
                f"{len(observation.completed_offsets)} of {total_messages} messages"
            )
        if observation.active_offsets:
            raise BenchmarkError(
                "hot-ordering benchmark ended with active offsets: "
                f"{sorted(observation.active_offsets)}"
            )
        if observation.processing_offsets or observation.ack_started_offsets:
            raise BenchmarkError(
                "hot-ordering benchmark ended with incomplete client acknowledgement state"
            )
        metadata = _hot_ordering_metadata(
            observation,
            records=records,
            cluster=cluster,
            concurrency=concurrency,
            processing_delay_ms=processing_delay_ms,
            timeout_seconds=timeout_seconds,
            operation_elapsed_ns=time.perf_counter_ns() - operation_started_ns,
        )
        return metric(
            "cluster_hot_ordering",
            observation.request_latencies_ns,
            time.perf_counter_ns() - operation_started_ns,
            message_size=len(payload),
            metadata=metadata,
        )

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_peer_forwarding(
    cluster: Cluster,
    stream: str,
    payload: str,
    messages: int,
    warmup: int,
    concurrency: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Publish through a follower to exercise the topology-free peer pool.

    The setup is sent through the bootstrap node and excluded from the
    measured interval. Measured publishes use persistent public clients on a
    different node, so the Raft engine must forward each operation over its
    shared topology-free peer lane. Concurrency above the current four shared
    permits queues behind that lane; optional peer-response delay is applied by
    the run-scoped native proxy configured on ``Cluster``.
    """
    setup = cluster.client(0)
    try:
        publish_stream(setup, stream, payload, warmup)
    finally:
        setup.close()

    latencies: list[int] = []
    offsets: list[int] = []
    lock = threading.Lock()
    deadline = time.monotonic() + timeout_seconds
    ingress_index = PEER_FORWARDING_INGRESS_NODE_INDEX
    client_timeout = min(DEFAULT_TIMEOUT_SECONDS, timeout_seconds)

    def worker(worker_index: int) -> None:
        client = cluster.client(ingress_index, timeout_seconds=client_timeout)
        try:
            for message_index in range(worker_index, messages, concurrency):
                if time.monotonic() >= deadline:
                    raise BenchmarkError(
                        "peer forwarding benchmark exceeded its bounded runtime"
                    )
                published, elapsed = publish(client, stream, payload)
                with lock:
                    offsets.append(published)
                    latencies.append(elapsed)
        finally:
            client.close()

    proxy_summary = cluster.peer_proxy_summary()

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker, index) for index in range(concurrency)]
            for future in futures:
                future.result()
        ordered_offsets = sorted(offsets)
        if len(ordered_offsets) != messages or any(
            offset != warmup + index for index, offset in enumerate(ordered_offsets)
        ):
            raise BenchmarkError(
                "peer forwarding returned non-contiguous offsets: "
                f"received {len(ordered_offsets)} of {messages}"
            )
        result = metric(
            "cluster_peer_forwarding",
            latencies,
            time.perf_counter_ns() - started,
            message_size=len(payload),
            metadata={
                "nodes": cluster.node_count,
                "forwarded_operation": "publish",
                "forwarding_ingress_node": ingress_index + 1,
                "forwarding_target": "data-group leader selected by the cluster",
                "concurrency": concurrency,
                "warmup": warmup,
                "peer_response_delay_ms": cluster.peer_response_delay_ms,
                "peer_response_proxy_enabled": proxy_summary["enabled"],
                "latency_scope": (
                    "follower_public_publish_roundtrip_includes_peer_forwarding_and_pool_wait"
                ),
                "setup_excluded": True,
                "saturation_scope": (
                    "shared_forwarding_lane_queues_when_concurrency_exceeds_current_four_per_peer"
                ),
                "bounded_runtime_seconds": timeout_seconds,
            },
        )
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_restart_recovery(cluster: Cluster, stream: str, payload: str) -> dict[str, Any]:
    client = cluster.client(0)
    create_stream(client, stream)
    publish(client, stream, payload)
    poll(client, stream, "recovery-consumer", 0)
    client.close()
    # Let the configured lease expire. The measured operation below polls for
    # actual eligibility instead of relying on a fixed scheduling margin.
    time.sleep(cluster.ack_timeout_ms / 1_000)

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        restart_ns = cluster.restart_node(0)
        recovered = cluster.client(0)
        try:
            response, poll_attempts = poll_until_redelivered(
                recovered, stream, "recovery-consumer", 0
            )
            acknowledge(recovered, stream, "recovery-consumer", 0)
        finally:
            recovered.close()
        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            "cluster_restart_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "unacknowledged_message_redelivered": True,
                "delivery_attempt": response.get("delivery_attempt"),
                "latency_scope": "restart_ready_to_redelivered_acknowledgement",
                "redelivery_poll_attempts": poll_attempts,
                "restarted_node": cluster.nodes[0].node_id,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_retained_recovery(
    cluster: Cluster, stream: str, payload: str, retained_messages: int
) -> dict[str, Any]:
    """Measure restart recovery after preloading a bounded retained history.

    The preload is deliberately excluded from the measured interval. The
    measured probe restarts one node, waits for readiness, replays the earliest
    record, and acknowledges it. This exercises recovery and cold replay with a
    known retained-data size without inventing retention or batch semantics.
    """
    preload(cluster, stream, payload, retained_messages)

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        restart_ns = cluster.restart_node(0)
        recovered = cluster.client(0)
        try:
            response, _ = poll(recovered, stream, "retained-recovery", 0)
            if response.get("payload") != payload:
                raise BenchmarkError(
                    "retained recovery returned an unexpected payload at offset 0"
                )
            acknowledge(recovered, stream, "retained-recovery", 0)
        finally:
            recovered.close()
        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            "cluster_retained_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "retained_messages": retained_messages,
                "retained_logical_payload_bytes": retained_messages * len(payload),
                "recovery_probe_offset": 0,
                "publish_setup_excluded": True,
                "latency_scope": "restart_ready_to_earliest_replay_acknowledgement",
                "redelivery_expected": False,
                "restarted_node": cluster.nodes[0].node_id,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def request_until_response(
    cluster: Cluster,
    node_index: int,
    request: dict[str, Any],
    response_type: str,
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], int, int]:
    """Retry a public request while a bounded leader transition is in progress."""
    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        attempts += 1
        client: LineClient | None = None
        try:
            client = cluster.client(
                node_index, timeout_seconds=min(DEFAULT_TIMEOUT_SECONDS, remaining)
            )
            response, elapsed = client.request(request)
            if response.get("type") == response_type:
                return response, elapsed, attempts
            last_error = BenchmarkError(
                f"unexpected response to {request.get('op')}: {response}"
            )
        except (BenchmarkError, OSError, TimeoutError) as error:
            last_error = error
        finally:
            if client is not None:
                try:
                    client.close()
                except OSError:
                    pass
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.05, remaining))
    node_id = cluster.nodes[node_index].node_id
    raise BenchmarkError(
        f"node {node_id} did not return {response_type} for {request.get('op')} "
        f"before the {timeout_seconds:g}-second deadline after {attempts} attempts; "
        f"last error: {last_error}"
    )


def validate_leader_failure_message(
    response: dict[str, Any], expected_offset: int, payload: str, phase: str
) -> None:
    if response.get("offset") != expected_offset:
        raise BenchmarkError(
            f"{phase} returned offset {response.get('offset')}, expected {expected_offset}"
        )
    if response.get("payload") != payload:
        raise BenchmarkError(f"{phase} returned an unexpected payload")


def run_node_failure_recovery(
    cluster: Cluster,
    stream: str,
    payload: str,
    *,
    failed_index: int,
    failure_kind: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Exercise one bounded process-stop and same-node restart.

    The public protocol does not expose leader identity, so the leader probe
    keeps its bootstrap assumption. The follower probe uses the same public
    sequence with a non-bootstrap node, separating process recovery from
    leader-election observation without adding broker-specific controls.
    """
    if failure_kind not in {"leader", "follower"}:
        raise BenchmarkError(f"unsupported failure kind: {failure_kind}")
    if not 0 <= failed_index < cluster.node_count:
        raise BenchmarkError(f"failure node index is outside the cluster: {failed_index}")
    survivor_indices = [index for index in range(cluster.node_count) if index != failed_index]
    if len(survivor_indices) < 2:
        raise BenchmarkError("node failure recovery requires at least two surviving nodes")

    setup_index = failed_index if failure_kind == "leader" else 0
    setup = cluster.client(setup_index)
    try:
        create_stream(setup, stream)
        pre_failure_offset, _ = publish(setup, stream, payload)
    finally:
        setup.close()
    if pre_failure_offset != 0:
        raise BenchmarkError(
            f"{failure_kind} failure recovery setup returned offset {pre_failure_offset}, "
            "expected 0"
        )

    def operation() -> dict[str, Any]:
        started = time.perf_counter_ns()
        cluster.stop_node(failed_index)

        attempts: dict[str, int] = {}
        consumer = f"{failure_kind}-failure-consumer"
        request_prefix = (
            stream if failure_kind == "leader" else f"follower-failure-{stream}"
        )
        failure_phase = f"{failure_kind}-failure"
        response, _, attempts[f"publish_after_{failure_kind}_failure"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "publish",
                "stream": stream,
                "payload": payload,
                "request_id": f"{request_prefix}-after-{failure_phase}",
            },
            "published",
            timeout_seconds=timeout_seconds,
        )
        if response.get("offset") != 1:
            raise BenchmarkError(
                f"publish after {failure_kind} failure returned {response}, expected offset 1"
            )

        response, _, attempts["poll_before_restart"] = request_until_response(
            cluster,
            survivor_indices[0],
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 0, payload, "survivor poll")
        _, _, attempts["ack_before_restart"] = request_until_response(
            cluster,
            survivor_indices[1],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 0,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        response, _, attempts["poll_second_survivor"] = request_until_response(
            cluster,
            survivor_indices[1],
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 1, payload, "second survivor poll")
        _, _, attempts["ack_second_survivor"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 1,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        restart_ns = cluster.restart_node(failed_index)
        response, _, attempts["publish_after_restart"] = request_until_response(
            cluster,
            failed_index,
            {
                "op": "publish",
                "stream": stream,
                "payload": payload,
                "request_id": f"{request_prefix}-after-node-restart",
            },
            "published",
            timeout_seconds=timeout_seconds,
        )
        if response.get("offset") != 2:
            raise BenchmarkError(
                f"publish after node restart returned {response}, expected offset 2"
            )
        response, _, attempts["poll_after_restart"] = request_until_response(
            cluster,
            failed_index,
            {"op": "poll", "stream": stream, "consumer": consumer},
            "message",
            timeout_seconds=timeout_seconds,
        )
        validate_leader_failure_message(response, 2, payload, "restarted node poll")
        _, _, attempts["ack_after_restart"] = request_until_response(
            cluster,
            survivor_indices[0],
            {
                "op": "ack",
                "stream": stream,
                "consumer": consumer,
                "offset": 2,
            },
            "acknowledged",
            timeout_seconds=timeout_seconds,
        )

        elapsed_ns = time.perf_counter_ns() - started
        result = metric(
            f"cluster_{failure_kind}_failure_recovery",
            [elapsed_ns],
            elapsed_ns,
            message_size=len(payload),
            metadata={
                "nodes": cluster.node_count,
                "failed_node": cluster.nodes[failed_index].node_id,
                "surviving_nodes": [cluster.nodes[index].node_id for index in survivor_indices],
                "failure_state": f"{failure_kind}_process_stop",
                "failed_node_role": failure_kind,
                "initial_leader_selection": (
                    "bootstrap_assumption"
                    if failure_kind == "leader"
                    else "not_required_for_follower_probe"
                ),
                "initial_leader_node": (
                    cluster.nodes[failed_index].node_id
                    if failure_kind == "leader"
                    else None
                ),
                "initial_leader_basis": (
                    "node 1 is the only process started with --bootstrap; the current "
                    "PersistentEngine uses that process to initialize the static metadata "
                    "group before data-stream creation"
                    if failure_kind == "leader"
                    else "follower probe does not require identifying the current leader"
                ),
                "replacement_leader_observed": failure_kind == "leader",
                "replacement_leader_identity": (
                    "not exposed by the provisional public protocol"
                    if failure_kind == "leader"
                    else "not applicable"
                ),
                "replacement_observation": (
                    "both surviving public endpoints committed, consumed, and acknowledged "
                    "records after the failed node stopped"
                ),
                "public_protocol_survivor_nodes": [
                    cluster.nodes[index].node_id for index in survivor_indices[:2]
                ],
                "post_failure_publish_offset": 1,
                "post_failure_consumed_offsets": [0, 1],
                "post_restart_publish_offset": 2,
                "restart_recovered_message_offset": 2,
                "fault_sequence_messages": 3,
                "verified_message_count": 3,
                "metrics_counter_reset_on_restart_expected": True,
                "verified": {
                    "surviving_nodes_elected_and_served": failure_kind == "leader",
                    "surviving_nodes_served": True,
                    "publish_after_failure": True,
                    "consume_after_failure": True,
                    "ack_after_failure": True,
                    "publish_after_leader_failure": True,
                    "consume_after_leader_failure": True,
                    "ack_after_leader_failure": True,
                    "stopped_node_restarted": True,
                    "restarted_node_served_and_recovered": True,
                },
                "setup_excluded": True,
                "request_identity_for_retried_publishes": "stable request_id",
                "bounded_timeout_seconds": timeout_seconds,
                "latency_scope": (
                    "stopped-bootstrap-leader-through-survivor-failover-and-restarted-node-ack"
                    if failure_kind == "leader"
                    else "stopped-follower-through-survivor-service-and-restarted-node-ack"
                ),
                "failure_scope": (
                    f"one {failure_kind} process stop in a static quorum followed by same-process restart; "
                    "network partitions, storage loss, and membership changes are excluded"
                ),
                "request_attempts": attempts,
            },
        )
        result["restart_ready_seconds"] = restart_ns / 1_000_000_000
        return result

    return measure_scenario(cluster.stats, operation, metrics=cluster.metrics)


def run_leader_failure_recovery(
    cluster: Cluster, stream: str, payload: str, timeout_seconds: float
) -> dict[str, Any]:
    """Exercise one bounded bootstrap-leader failure through the public protocol."""
    return run_node_failure_recovery(
        cluster,
        stream,
        payload,
        failed_index=0,
        failure_kind="leader",
        timeout_seconds=timeout_seconds,
    )


def run_follower_failure_recovery(
    cluster: Cluster, stream: str, payload: str, timeout_seconds: float
) -> dict[str, Any]:
    """Exercise one bounded non-bootstrap follower failure through the public protocol."""
    return run_node_failure_recovery(
        cluster,
        stream,
        payload,
        failed_index=1,
        failure_kind="follower",
        timeout_seconds=timeout_seconds,
    )


def parse_retained_messages(value: str) -> int:
    try:
        messages = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("retained messages must be an integer") from error
    if messages < MIN_RETAINED_RECOVERY_MESSAGES:
        raise argparse.ArgumentTypeError(
            "retained messages must exceed the current 1,024-record tail index "
            f"(minimum: {MIN_RETAINED_RECOVERY_MESSAGES})"
        )
    return messages


def parse_scenarios(value: str) -> list[str]:
    scenarios = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(scenarios) - set(SCENARIO_NAMES))
    if not scenarios:
        raise argparse.ArgumentTypeError("scenarios cannot be empty")
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown scenario(s): {', '.join(unknown)}; choose from {', '.join(SCENARIO_NAMES)}"
        )
    if len(scenarios) != len(set(scenarios)):
        raise argparse.ArgumentTypeError("scenarios must not contain duplicates")
    return scenarios
