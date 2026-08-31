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
    metric,
    MIN_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    parse_args,
    parse_nonnegative_int,
    parse_retained_messages,
    percentile,
    poll_until_redelivered,
    process_stats,
    resource_limits,
    run_retained_recovery,
)
from profile import summarize_timing_logs  # noqa: E402


class ClusterBenchmarkTests(unittest.TestCase):
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
