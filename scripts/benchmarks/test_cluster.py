import argparse
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from cluster import (  # noqa: E402
    BenchmarkError,
    DEFAULT_SCENARIOS,
    DEFAULT_PUBLISH_BATCH_SIZE,
    DEFAULT_HOT_KEY_MESSAGES,
    DEFAULT_COLD_KEY_COUNT,
    DEFAULT_COLD_MESSAGES_PER_KEY,
    DEFAULT_HOT_ORDERING_CONCURRENCY,
    DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
    DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
    MAX_HOT_ORDERING_CONCURRENCY,
    MAX_HOT_KEY_PROCESSING_DELAY_MS,
    MAX_HOT_ORDERING_TIMEOUT_SECONDS,
    MAX_HOT_ORDERING_MESSAGES,
    MAX_PUBLISH_BATCH_SIZE,
    MAX_LEADER_FAILURE_TIMEOUT_SECONDS,
    batch_metric,
    hot_ordering_records,
    HotOrderingObservation,
    _hot_ordering_metadata,
    metric,
    MIN_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    parse_args,
    parse_nonnegative_int,
    parse_retained_messages,
    parse_scenarios,
    parse_positive_float,
    percentile,
    publish_batch_request,
    PeerResponseDelayProxy,
    ProcessStats,
    poll_until_redelivered,
    process_stats,
    resource_limits,
    run_publish_batch,
    run_peer_forwarding,
    run_follower_failure_recovery,
    run_leader_failure_recovery,
    run_retained_recovery,
)
from profile import summarize_timing_logs  # noqa: E402


class _ClientsContext:
    def __init__(self, clients: list[object]) -> None:
        self.clients = clients

    def __enter__(self) -> list[object]:
        return self.clients

    def __exit__(self, *_: object) -> None:
        return None


class ClusterBenchmarkTests(unittest.TestCase):
    def test_default_scenarios_preserve_the_existing_entrypoint_workload(self) -> None:
        with patch.object(sys, "argv", ["cluster.py"]):
            args = parse_args()

        self.assertEqual(args.scenarios, list(DEFAULT_SCENARIOS))
        self.assertNotIn("peer_forwarding", args.scenarios)
        self.assertNotIn("publish_batch", args.scenarios)
        self.assertNotIn("leader_failure_recovery", args.scenarios)
        self.assertNotIn("hot_ordering", args.scenarios)

    def test_hot_ordering_options_are_opt_in_and_bounded(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "hot_ordering",
                "--hot-key-messages",
                "12",
                "--cold-key-count",
                "3",
                "--cold-messages-per-key",
                "4",
                "--hot-ordering-concurrency",
                "6",
                "--hot-key-processing-delay-ms",
                "7",
                "--hot-ordering-timeout-seconds",
                "12.5",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["hot_ordering"])
        self.assertEqual(args.hot_key_messages, 12)
        self.assertEqual(args.cold_key_count, 3)
        self.assertEqual(args.cold_messages_per_key, 4)
        self.assertEqual(args.hot_ordering_concurrency, 6)
        self.assertEqual(args.hot_key_processing_delay_ms, 7)
        self.assertEqual(args.hot_ordering_timeout_seconds, 12.5)
        self.assertEqual(DEFAULT_HOT_KEY_MESSAGES, 64)
        self.assertEqual(DEFAULT_COLD_KEY_COUNT, 4)
        self.assertEqual(DEFAULT_COLD_MESSAGES_PER_KEY, 8)
        self.assertEqual(DEFAULT_HOT_ORDERING_CONCURRENCY, 4)
        self.assertEqual(DEFAULT_HOT_KEY_PROCESSING_DELAY_MS, 5)
        self.assertEqual(DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS, 60.0)

        invalid_options = (
            ("--hot-ordering-concurrency", str(MAX_HOT_ORDERING_CONCURRENCY + 1)),
            ("--hot-key-processing-delay-ms", str(MAX_HOT_KEY_PROCESSING_DELAY_MS + 1)),
            (
                "--hot-ordering-timeout-seconds",
                str(MAX_HOT_ORDERING_TIMEOUT_SECONDS + 1),
            ),
            ("--hot-key-messages", str(MAX_HOT_ORDERING_MESSAGES)),
        )
        for option, value in invalid_options:
            with self.subTest(option=option):
                with patch.object(
                    sys,
                    "argv",
                    ["cluster.py", "--scenarios", "hot_ordering", option, value],
                ):
                    with self.assertRaises(SystemExit):
                        parse_args()

        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "hot_ordering", "--hot-ordering-concurrency", "1"],
        ):
            with self.assertRaises(SystemExit):
                parse_args()
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "hot_ordering",
                "--ack-timeout-ms",
                "5",
                "--hot-key-processing-delay-ms",
                "5",
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_hot_ordering_schedule_is_deterministic_and_mixed(self) -> None:
        self.assertEqual(
            hot_ordering_records(3, 2, 2),
            [
                (0, "hot-key"),
                (1, "cold-key-0"),
                (2, "cold-key-1"),
                (3, "hot-key"),
                (4, "cold-key-0"),
                (5, "cold-key-1"),
                (6, "hot-key"),
            ],
        )
        with self.assertRaises(BenchmarkError):
            hot_ordering_records(0, 2, 2)

    def test_hot_ordering_metadata_reports_backlog_order_and_cold_fairness(self) -> None:
        records = hot_ordering_records(2, 1, 2)
        observation = HotOrderingObservation.for_records(records, "hot-key")
        observation.record_delivery(
            offset=0, key="hot-key", delivery_attempt=1, delivery_wait_ns=1_000_000
        )
        observation.record_delivery(
            offset=1, key="cold-key-0", delivery_attempt=1, delivery_wait_ns=2_000_000
        )
        observation.record_ack_start(offset=1, key="cold-key-0")
        observation.record_completion(
            offset=1,
            key="cold-key-0",
            request_latency_ns=100,
            completion_elapsed_ns=3_000_000,
        )
        observation.record_ack_start(offset=0, key="hot-key")
        observation.record_completion(
            offset=0,
            key="hot-key",
            request_latency_ns=100,
            completion_elapsed_ns=4_000_000,
        )
        observation.record_delivery(
            offset=2, key="hot-key", delivery_attempt=1, delivery_wait_ns=5_000_000
        )
        observation.record_delivery(
            offset=3, key="cold-key-0", delivery_attempt=1, delivery_wait_ns=6_000_000
        )
        observation.record_ack_start(offset=3, key="cold-key-0")
        observation.record_completion(
            offset=3,
            key="cold-key-0",
            request_latency_ns=100,
            completion_elapsed_ns=7_000_000,
        )
        observation.record_ack_start(offset=2, key="hot-key")
        observation.record_completion(
            offset=2,
            key="hot-key",
            request_latency_ns=100,
            completion_elapsed_ns=8_000_000,
        )

        metadata = _hot_ordering_metadata(
            observation,
            records=records,
            cluster=SimpleNamespace(node_count=3),
            concurrency=2,
            processing_delay_ms=5,
            timeout_seconds=60.0,
            operation_elapsed_ns=8_000_000,
        )

        self.assertTrue(metadata["per_key_ordering"]["verified"])
        self.assertEqual(metadata["hot_key_backlog"]["at_first_cold_completion"], 2)
        self.assertEqual(
            metadata["unrelated_key_progress"]["cold_messages_completed_before_hot_drained"],
            2,
        )
        self.assertEqual(
            metadata["unrelated_key_progress"]["cold_keys_completed_before_hot_drained_names"],
            ["cold-key-0"],
        )
        self.assertEqual(
            metadata["unrelated_key_progress"][
                "cold_keys_with_progress_before_hot_drained_names"
            ],
            ["cold-key-0"],
        )
        self.assertIn("fairness", metadata["unrelated_key_progress"])
        self.assertEqual(
            metadata["delivery_concurrency"]["max_processing_in_flight_by_key"]["hot-key"],
            1,
        )

    def test_leader_failure_scenario_is_opt_in_and_has_a_bounded_timeout(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "leader_failure_recovery",
                "--leader-failure-timeout-seconds",
                "12.5",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["leader_failure_recovery"])
        self.assertEqual(args.leader_failure_timeout_seconds, 12.5)
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--leader-failure-timeout-seconds",
                str(MAX_LEADER_FAILURE_TIMEOUT_SECONDS + 1),
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_follower_failure_scenario_is_opt_in(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "follower_failure_recovery"],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["follower_failure_recovery"])

    def test_publish_batch_options_are_explicit_and_bounded(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "publish_batch", "--batch-size", "16"],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["publish_batch"])
        self.assertEqual(args.batch_size, 16)
        self.assertEqual(DEFAULT_PUBLISH_BATCH_SIZE, 32)
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--batch-size", str(MAX_PUBLISH_BATCH_SIZE + 1)],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_peer_forwarding_options_are_explicit_and_parseable(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "peer_forwarding",
                "--messages",
                "3",
                "--peer-forwarding-concurrency",
                "8",
                "--peer-response-delay-ms",
                "5",
                "--peer-forwarding-timeout-seconds",
                "12.5",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["peer_forwarding"])
        self.assertEqual(args.peer_forwarding_concurrency, 8)
        self.assertEqual(args.peer_response_delay_ms, 5)
        self.assertEqual(args.peer_forwarding_timeout_seconds, 12.5)
        self.assertEqual(parse_positive_float("0.5"), 0.5)

    def test_publish_batch_request_validates_each_published_outcome(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.request_body: dict[str, object] | None = None

            def request(self, request: dict[str, object]) -> tuple[dict[str, object], int]:
                self.request_body = request
                return {
                    "type": "publish_batch",
                    "outcomes": [
                        {"type": "published", "offset": 4},
                        {"type": "published", "offset": 5},
                    ],
                }, 2_500

        client = FakeClient()
        published, elapsed = publish_batch_request(
            client, "events", "payload", batch_size=2, expected_offset=4
        )

        self.assertEqual(published, 2)
        self.assertEqual(elapsed, 2_500)
        self.assertEqual(client.request_body["op"], "publish_batch")
        self.assertEqual(
            client.request_body["records"],
            [
                {"key": None, "payload_base64": "cGF5bG9hZA=="},
                {"key": None, "payload_base64": "cGF5bG9hZA=="},
            ],
        )

    def test_publish_batch_request_rejects_a_per_record_error(self) -> None:
        client = SimpleNamespace(
            request=lambda _request: (
                {
                    "type": "publish_batch",
                    "outcomes": [
                        {"type": "error", "code": "invalid_record"},
                    ],
                },
                1_000,
            )
        )

        with self.assertRaisesRegex(BenchmarkError, "did not publish"):
            publish_batch_request(client, "events", "payload", 1, 0)

    def test_publish_batch_reports_record_count_and_batch_latency_samples(self) -> None:
        result = batch_metric(
            "cluster_publish_batch",
            [1_000, 2_000, 3_000],
            3_000_000,
            messages=5,
            message_size=100,
            metadata={"batch_size": 2},
        )

        self.assertEqual(result["messages"], 5)
        self.assertEqual(result["latency_sample_count"], 3)
        self.assertEqual(result["throughput_messages_per_second"], 5 / 0.003)

    def test_publish_batch_excludes_setup_and_checks_batch_offsets(self) -> None:
        clients = [SimpleNamespace(close=lambda: None) for _ in range(3)]
        setup = SimpleNamespace(close=lambda: None)
        cluster = SimpleNamespace(
            node_count=3,
            stats=object(),
            metrics=lambda: None,
            client=lambda _index: setup,
            connected_clients=lambda: _ClientsContext(clients),
        )
        next_offset = 2

        def publish_batch_message(
            _client: object,
            _stream: str,
            _payload: str,
            batch_size: int,
            expected_offset: int,
        ) -> tuple[int, int]:
            nonlocal next_offset
            self.assertEqual(expected_offset, next_offset)
            next_offset += batch_size
            return batch_size, 1_000

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster.publish_stream") as publish_stream,
            patch("cluster.publish_batch_request", side_effect=publish_batch_message),
            patch("cluster.measure_scenario", side_effect=run_measurement),
        ):
            result = run_publish_batch(
                cluster,
                "events",
                "payload",
                messages=5,
                warmup=2,
                batch_size=2,
            )

        publish_stream.assert_called_once_with(setup, "events", "payload", 2)
        self.assertEqual(result["operation"], "cluster_publish_batch")
        self.assertEqual(result["messages"], 5)
        self.assertEqual(result["latency_sample_count"], 3)
        self.assertEqual(result["metadata"]["batches"], 3)

    def test_scenarios_reject_unknown_and_duplicate_names(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_scenarios("peer_forwarding,unknown")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_scenarios("peer_forwarding,peer_forwarding")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_scenarios("  ")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_positive_float("nan")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_positive_float("inf")

    def test_peer_response_proxy_can_close_before_start(self) -> None:
        proxy = PeerResponseDelayProxy(0, 1)
        proxy.close()

    def test_peer_response_delay_requires_native_runtime(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--runtime",
                "container",
                "--peer-response-delay-ms",
                "1",
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_peer_forwarding_rejects_a_non_contiguous_response_batch(self) -> None:
        cluster = SimpleNamespace(
            node_count=3,
            peer_response_delay_ms=0,
            stats=object(),
            metrics=lambda: None,
            peer_proxy_summary=lambda: {"enabled": False},
            client=lambda _index, **_: SimpleNamespace(close=lambda: None),
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster.publish_stream"),
            patch("cluster.measure_scenario", side_effect=run_measurement),
            patch("cluster.publish", return_value=(0, 100)),
        ):
            with self.assertRaisesRegex(BenchmarkError, "non-contiguous offsets"):
                run_peer_forwarding(
                    cluster,
                    "events",
                    "payload",
                    messages=2,
                    warmup=0,
                    concurrency=2,
                    timeout_seconds=1,
                )

    def test_peer_forwarding_records_follower_roundtrip_metadata(self) -> None:
        cluster = SimpleNamespace(
            node_count=3,
            peer_response_delay_ms=5,
            stats=object(),
            metrics=lambda: None,
            peer_proxy_summary=lambda: {"enabled": True},
            client=lambda _index, **_: SimpleNamespace(close=lambda: None),
        )
        next_offset = 2

        def publish_message(*_: object) -> tuple[int, int]:
            nonlocal next_offset
            offset = next_offset
            next_offset += 1
            return offset, 100

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster.publish_stream"),
            patch("cluster.measure_scenario", side_effect=run_measurement),
            patch("cluster.publish", side_effect=publish_message),
        ):
            result = run_peer_forwarding(
                cluster,
                "events",
                "payload",
                messages=4,
                warmup=2,
                concurrency=2,
                timeout_seconds=1,
            )

        self.assertEqual(result["operation"], "cluster_peer_forwarding")
        self.assertEqual(result["messages"], 4)
        self.assertEqual(result["metadata"]["forwarding_ingress_node"], 2)
        self.assertEqual(result["metadata"]["peer_response_delay_ms"], 5)
        self.assertTrue(result["metadata"]["peer_response_proxy_enabled"])

    def test_leader_failure_recovery_checks_both_survivors_and_restarted_node(self) -> None:
        requests: list[tuple[int, dict[str, object]]] = []
        state = {"failed_publish": False, "polls": 0}

        class FakeClient:
            def __init__(self, node_index: int) -> None:
                self.node_index = node_index

            def request(self, request: dict[str, object]) -> tuple[dict[str, object], int]:
                requests.append((self.node_index, request))
                if request["op"] == "create_stream":
                    return {"type": "stream_created"}, 100
                if request["op"] == "publish":
                    request_id = request.get("request_id")
                    if request_id == "leader-failure-events-after-leader-failure":
                        if not state["failed_publish"]:
                            state["failed_publish"] = True
                            raise BenchmarkError("leader transition")
                        return {"type": "published", "offset": 1}, 100
                    if request_id == "leader-failure-events-after-node-restart":
                        return {"type": "published", "offset": 2}, 100
                    return {"type": "published", "offset": 0}, 100
                if request["op"] == "poll":
                    offset = state["polls"]
                    state["polls"] += 1
                    return {"type": "message", "offset": offset, "payload": "payload"}, 100
                if request["op"] == "ack":
                    return {"type": "acknowledged"}, 100
                raise AssertionError(f"unexpected request: {request}")

            def close(self) -> None:
                return None

        events: list[tuple[str, int]] = []
        cluster = SimpleNamespace(
            node_count=3,
            nodes=[SimpleNamespace(node_id=index) for index in (1, 2, 3)],
            stats=object(),
            metrics=lambda: None,
            client=lambda index, **_: FakeClient(index),
            stop_node=lambda index: events.append(("stop", index)),
            restart_node=lambda index: events.append(("restart", index)) or 2_000_000,
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with patch("cluster.measure_scenario", side_effect=run_measurement), patch(
            "cluster.time.sleep"
        ):
            result = run_leader_failure_recovery(
                cluster, "leader-failure-events", "payload", timeout_seconds=1
            )

        self.assertEqual(events, [("stop", 0), ("restart", 0)])
        self.assertEqual(result["operation"], "cluster_leader_failure_recovery")
        self.assertEqual(result["restart_ready_seconds"], 0.002)
        self.assertEqual(result["metadata"]["failed_node"], 1)
        self.assertEqual(result["metadata"]["surviving_nodes"], [2, 3])
        self.assertEqual(result["metadata"]["initial_leader_selection"], "bootstrap_assumption")
        self.assertTrue(result["metadata"]["replacement_leader_observed"])
        self.assertEqual(result["metadata"]["request_attempts"]["publish_after_leader_failure"], 2)
        publish_request_ids = [
            request.get("request_id")
            for _, request in requests
            if request["op"] == "publish" and request.get("request_id")
        ]
        self.assertEqual(
            publish_request_ids,
            [
                "leader-failure-events-after-leader-failure",
                "leader-failure-events-after-leader-failure",
                "leader-failure-events-after-node-restart",
            ],
        )
        self.assertEqual(
            {node_index for node_index, request in requests if request["op"] == "poll"},
            {0, 1, 2},
        )

    def test_follower_failure_recovery_records_process_failure_state(self) -> None:
        events: list[tuple[str, int]] = []
        polls = 0

        class FakeClient:
            def __init__(self, _node_index: int) -> None:
                pass

            def request(self, request: dict[str, object]) -> tuple[dict[str, object], int]:
                nonlocal polls
                if request["op"] == "create_stream":
                    return {"type": "stream_created"}, 100
                if request["op"] == "publish":
                    request_id = request.get("request_id", "")
                    if "after-node-restart" in request_id:
                        return {"type": "published", "offset": 2}, 100
                    if "after-follower-failure" in request_id:
                        return {"type": "published", "offset": 1}, 100
                    return {"type": "published", "offset": 0}, 100
                if request["op"] == "poll":
                    response = {"type": "message", "offset": polls, "payload": "payload"}
                    polls += 1
                    return response, 100
                if request["op"] == "ack":
                    return {"type": "acknowledged"}, 100
                raise AssertionError(f"unexpected request: {request}")

            def close(self) -> None:
                return None

        cluster = SimpleNamespace(
            node_count=3,
            nodes=[SimpleNamespace(node_id=index) for index in (1, 2, 3)],
            stats=object(),
            metrics=lambda: None,
            client=lambda index, **_: FakeClient(index),
            stop_node=lambda index: events.append(("stop", index)),
            restart_node=lambda index: events.append(("restart", index)) or 2_000_000,
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with patch("cluster.measure_scenario", side_effect=run_measurement), patch(
            "cluster.time.sleep"
        ):
            result = run_follower_failure_recovery(
                cluster, "follower-failure-events", "payload", timeout_seconds=1
            )

        self.assertEqual(events, [("stop", 1), ("restart", 1)])
        self.assertEqual(result["operation"], "cluster_follower_failure_recovery")
        self.assertEqual(result["metadata"]["failed_node"], 2)
        self.assertEqual(result["metadata"]["failed_node_role"], "follower")
        self.assertEqual(
            result["metadata"]["initial_leader_selection"],
            "not_required_for_follower_probe",
        )

    def test_recovery_poll_retries_empty_responses_until_second_attempt(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.responses = [
                    {"type": "empty"},
                    {"type": "message", "offset": 0, "delivery_attempt": 2},
                ]

            def request(self, request: dict[str, object]) -> tuple[dict[str, object], int]:
                self.assertEqual(request["op"], "poll")
                return self.responses.pop(0), 1_000

            def assertEqual(self, first: object, second: object) -> None:
                if first != second:
                    raise AssertionError(f"expected {second!r}, got {first!r}")

        response, attempts = poll_until_redelivered(FakeClient(), "events", "worker", 0)

        self.assertEqual(response["delivery_attempt"], 2)
        self.assertEqual(attempts, 2)

    def test_percentile_interpolates_sorted_values(self) -> None:
        values = [30, 10, 20]
        self.assertEqual(percentile(values, 0), 10)
        self.assertEqual(percentile(values, 50), 20)
        self.assertEqual(percentile(values, 100), 30)

    def test_metric_uses_cluster_operation_shape(self) -> None:
        result = metric(
            "cluster_consume_ack",
            [1_000, 2_000, 3_000],
            3_000_000,
            message_size=100,
            metadata={"nodes": 3},
        )
        self.assertEqual(result["operation"], "cluster_consume_ack")
        self.assertEqual(result["messages"], 3)
        self.assertEqual(result["latency_microseconds"]["p50"], 2.0)
        self.assertEqual(result["metadata"]["nodes"], 3)

    def test_process_stats_reports_current_process(self) -> None:
        sample = process_stats(__import__("os").getpid())
        self.assertIsNotNone(sample)
        self.assertGreaterEqual(sample[0], 0)
        self.assertGreaterEqual(sample[1], 0)

    def test_process_stats_preserves_per_node_storage_samples(self) -> None:
        summary = ProcessStats._summarize_nodes(
            [
                {"1": {"storage_bytes": 8.0}},
                {"1": {"memory_bytes": 200.0, "storage_bytes": 12.0}},
            ]
        )

        self.assertEqual(summary["1"]["samples"], 2)
        self.assertEqual(summary["1"]["memory_bytes_avg"], 200.0)
        self.assertEqual(summary["1"]["storage_bytes_avg"], 10.0)
        self.assertEqual(summary["1"]["storage_bytes_max"], 12.0)

    def test_slow_consumer_delay_is_configurable_and_recorded(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--messages",
                "3",
                "--slow-consumer-delay-ms",
                "25",
                "--ack-timeout-ms",
                "100",
            ],
        ):
            args = parse_args()
        self.assertEqual(args.slow_consumer_delay_ms, 25)
        self.assertEqual(args.ack_timeout_ms, 100)
        self.assertEqual(parse_nonnegative_int("0"), 0)

    def test_slow_consumer_delay_cannot_reach_ack_timeout(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--slow-consumer-delay-ms", "100", "--ack-timeout-ms", "100"],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_retained_recovery_messages_default_and_boundary_are_above_tail_index(self) -> None:
        with patch.object(sys, "argv", ["cluster.py"]):
            args = parse_args()

        self.assertEqual(args.retained_messages, DEFAULT_RETAINED_RECOVERY_MESSAGES)
        self.assertEqual(
            parse_retained_messages(str(MIN_RETAINED_RECOVERY_MESSAGES)),
            MIN_RETAINED_RECOVERY_MESSAGES,
        )

    def test_retained_recovery_messages_reject_invalid_bounds(self) -> None:
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_retained_messages(str(MIN_RETAINED_RECOVERY_MESSAGES - 1))
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_retained_messages("not-an-integer")

        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--retained-messages", str(MIN_RETAINED_RECOVERY_MESSAGES - 1)],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_retained_recovery_restarts_and_probes_earliest_record(self) -> None:
        cluster = SimpleNamespace(
            node_count=3,
            nodes=[SimpleNamespace(node_id=1)],
            stats=object(),
            metrics=lambda: None,
            restart_node=lambda _: 2_000_000,
            client=lambda _: SimpleNamespace(close=lambda: None),
        )
        retained_messages = MIN_RETAINED_RECOVERY_MESSAGES
        payload = "payload"

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster.preload") as preload,
            patch("cluster.measure_scenario", side_effect=run_measurement),
            patch("cluster.poll", return_value=({"offset": 0, "payload": payload}, 100)),
            patch("cluster.acknowledge", return_value=200) as acknowledge,
        ):
            result = run_retained_recovery(
                cluster, "retained-events", payload, retained_messages
            )

        preload.assert_called_once_with(
            cluster, "retained-events", payload, retained_messages
        )
        acknowledge.assert_called_once()
        self.assertEqual(result["operation"], "cluster_retained_recovery")
        self.assertEqual(result["metadata"]["retained_messages"], retained_messages)
        self.assertEqual(
            result["metadata"]["retained_logical_payload_bytes"],
            retained_messages * len(payload),
        )
        self.assertEqual(result["restart_ready_seconds"], 0.002)

    def test_retained_recovery_rejects_wrong_replayed_payload(self) -> None:
        cluster = SimpleNamespace(
            nodes=[SimpleNamespace(node_id=1)],
            stats=object(),
            metrics=lambda: None,
            restart_node=lambda _: 0,
            client=lambda _: SimpleNamespace(close=lambda: None),
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster.preload"),
            patch("cluster.measure_scenario", side_effect=run_measurement),
            patch(
                "cluster.poll",
                return_value=({"offset": 0, "payload": "corrupt"}, 100),
            ),
            patch("cluster.acknowledge") as acknowledge,
        ):
            with self.assertRaisesRegex(
                BenchmarkError,
                "unexpected payload",
            ):
                run_retained_recovery(
                    cluster,
                    "retained-events",
                    "payload",
                    MIN_RETAINED_RECOVERY_MESSAGES,
                )

        acknowledge.assert_not_called()

    def test_container_runtime_records_per_broker_limits(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--runtime", "container", "--cpus", "1.5", "--memory", "1g"],
        ):
            args = parse_args()

        self.assertEqual(args.runtime, "container")
        self.assertEqual(
            resource_limits(runtime=args.runtime, cpus=args.cpus, memory=args.memory),
            {
                "processes": "Docker containers; benchmark client remains host-side",
                "cpu_per_broker": "1.5",
                "memory_per_broker": "1g",
            },
        )

    def test_timing_summary_normalizes_tracing_stage_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "node-1.log"
            log.write_text(
                'TRACE runnel::timing: stage complete stage="raft.publish_quorum" elapsed_us=10\n'
                'TRACE runnel::timing: stage complete stage="raft.peer_rpc" elapsed_us=30\n',
                encoding="utf-8",
            )
            summary = summarize_timing_logs(Path(directory))
        self.assertEqual(summary["stages"]["raft.publish_quorum"]["samples"], 1)
        self.assertEqual(summary["stages"]["raft.publish_quorum"]["p50_us"], 10.0)
        self.assertEqual(summary["stages"]["raft.peer_rpc"]["p50_us"], 30.0)


if __name__ == "__main__":
    unittest.main()
