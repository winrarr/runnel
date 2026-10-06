import argparse
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import cluster_cli  # noqa: E402
import cluster_lifecycle  # noqa: E402
import cluster_results  # noqa: E402
from resources import DEFAULT_PROBE_TIMEOUT_SECONDS  # noqa: E402
from cluster import parse_args, parse_positive_float, resource_limits  # noqa: E402
from cluster_faults import PeerResponseDelayProxy  # noqa: E402
from cluster_lifecycle import Cluster  # noqa: E402
from cluster_resources import (  # noqa: E402
    container_peer_tcp_endpoint_count,
    ProcessStats,
    _container_host_pids,
    parse_owned_peer_tcp_endpoints,
    peer_connection_census,
    process_peer_tcp_endpoint_count,
    process_stats,
)
from cluster_scenarios import (  # noqa: E402
    DEFAULT_COLD_KEY_COUNT,
    DEFAULT_COLD_MESSAGES_PER_KEY,
    DEFAULT_HOT_KEY_MESSAGES,
    DEFAULT_HOT_KEY_PROCESSING_DELAY_MS,
    DEFAULT_HOT_ORDERING_CONCURRENCY,
    DEFAULT_HOT_ORDERING_TIMEOUT_SECONDS,
    DEFAULT_PUBLISH_BATCH_SIZE,
    DEFAULT_RAFT_LOG_GROWTH_BATCH_SIZE,
    DEFAULT_RAFT_LOG_GROWTH_MESSAGES,
    DEFAULT_RETAINED_RECOVERY_MESSAGES,
    DEFAULT_SNAPSHOT_BUILD_MESSAGES,
    DEFAULT_SNAPSHOT_BUILD_TIMEOUT_SECONDS,
    DEFAULT_SLOW_CONSUMER_BACKPRESSURE_TIMEOUT_SECONDS,
    DEFAULT_SCENARIOS,
    HotOrderingObservation,
    MAX_HOT_KEY_PROCESSING_DELAY_MS,
    MAX_HOT_ORDERING_CONCURRENCY,
    MAX_HOT_ORDERING_MESSAGES,
    MAX_HOT_ORDERING_TIMEOUT_SECONDS,
    MAX_LEADER_FAILURE_TIMEOUT_SECONDS,
    MAX_PEER_FORWARDING_STREAM_COUNT,
    MAX_PUBLISH_BATCH_SIZE,
    MAX_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
    MAX_RAFT_LOG_GROWTH_BATCH_SIZE,
    MAX_RAFT_LOG_GROWTH_LOGICAL_PAYLOAD_BYTES,
    MAX_RAFT_LOG_GROWTH_MESSAGES,
    MAX_SNAPSHOT_BUILD_LOGICAL_PAYLOAD_BYTES,
    MAX_SNAPSHOT_BUILD_MESSAGES,
    MAX_SNAPSHOT_BUILD_RETAINED_MESSAGES,
    MAX_SNAPSHOT_BUILD_TIMEOUT_SECONDS,
    MAX_SLOW_CONSUMER_BACKPRESSURE_TIMEOUT_SECONDS,
    MIN_RETAINED_RECOVERY_MESSAGES,
    MIN_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
    MIN_RAFT_LOG_GROWTH_MESSAGES,
    MIN_SNAPSHOT_BUILD_MESSAGES,
    _hot_ordering_metadata,
    _observed_purge_advanced,
    _raft_data_group_state,
    batch_metric,
    hot_ordering_records,
    parse_retained_messages,
    parse_raft_log_growth_messages,
    parse_raft_log_growth_batch_size,
    parse_raft_log_growth_observation_every,
    parse_snapshot_build_messages,
    parse_scenarios,
    poll_until_redelivered,
    publish_batch_request,
    run_follower_failure_recovery,
    run_leader_failure_recovery,
    run_peer_forwarding,
    run_publish_batch,
    run_retained_hot_path,
    run_retained_recovery,
    run_raft_log_growth,
    run_snapshot_build_hot_path,
    _snapshot_build_metric_deltas,
    _snapshot_build_metrics,
    run_slow_consumer_backpressure,
)
from common import BenchmarkError, metric, parse_nonnegative_int, percentile  # noqa: E402
from profile import summarize_timing_logs  # noqa: E402


class _ClientsContext:
    def __init__(self, clients: list[object]) -> None:
        self.clients = clients

    def __enter__(self) -> list[object]:
        return self.clients

    def __exit__(self, *_: object) -> None:
        return None


class ClusterBenchmarkTests(unittest.TestCase):
    def test_cli_dispatch_preserves_payload_and_recovery_order(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "durable_publish,consume_ack,restart_recovery",
                "--payload-sizes",
                "100,200",
            ],
        ):
            args = parse_args()

        calls: list[str] = []

        def record(name: str):
            def scenario(*_: object) -> dict[str, str]:
                calls.append(name)
                return {"operation": name}

            return scenario

        cluster = SimpleNamespace()
        with (
            patch.object(cluster_cli, "run_durable_publish", side_effect=record("publish")),
            patch.object(cluster_cli, "run_consume_ack", side_effect=record("consume")),
            patch.object(cluster_cli, "run_restart_recovery", side_effect=record("restart")),
        ):
            results = cluster_cli.run_scenarios(args, cluster, "run-id")

        self.assertEqual(calls, ["publish", "consume", "restart", "publish", "consume"])
        self.assertEqual([result["operation"] for result in results], calls)

    def test_result_builder_keeps_schema_envelope_and_optional_workload_fields(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "retained_hot_path",
                "--payload-sizes",
                "100",
                "--retained-messages",
                "2048",
                "--skip-recovery",
            ],
        ):
            args = parse_args()
        cluster = SimpleNamespace(
            image_id="sha256:test",
            startup_ns=2_000_000_000,
            peer_proxy_summary=lambda: {"enabled": False, "response_delay_ms": 0},
            stats=SimpleNamespace(summary=lambda: {"samples": 2}),
        )
        with (
            patch.object(
                cluster_results,
                "result_metadata",
                return_value={"schema_version": 2, "run_id": "run-id"},
            ),
            patch.object(
                cluster_results,
                "resource_limits",
                return_value={"processes": "host-scheduled; no cgroup limit"},
            ),
        ):
            result = cluster_results.build_result(
                args,
                run_id="run-id",
                started_at=datetime(2026, 1, 1, tzinfo=UTC),
                cluster=cluster,
                scenarios=[{"operation": "cluster_retained_hot_path"}],
            )

        self.assertEqual(result["schema_version"], 2)
        self.assertEqual(result["run_id"], "run-id")
        self.assertEqual(result["workload"]["retained_hot_path_messages"], 2048)
        self.assertEqual(result["workload"]["slow_consumer_timeout_seconds"], 60.0)
        self.assertNotIn("retained_recovery_messages", result["workload"])
        self.assertEqual(result["backends"]["runnel-cluster"]["startup_seconds"], 2.0)
        self.assertEqual(
            result["backends"]["runnel-cluster"]["scenarios"],
            [{"operation": "cluster_retained_hot_path"}],
        )

    def test_result_builder_records_raft_log_growth_controls(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "raft_log_growth", "--payload-sizes", "1024"],
        ):
            args = parse_args()
        cluster = SimpleNamespace(
            image_id="sha256:test",
            startup_ns=1_000_000,
            peer_proxy_summary=lambda: {"enabled": False, "response_delay_ms": 0},
            stats=SimpleNamespace(summary=lambda: {}),
        )
        with (
            patch.object(cluster_results, "result_metadata", return_value={}),
            patch.object(cluster_results, "resource_limits", return_value={}),
        ):
            result = cluster_results.build_result(
                args,
                run_id="run-id",
                started_at=datetime(2026, 1, 1, tzinfo=UTC),
                cluster=cluster,
                scenarios=[],
            )

        self.assertEqual(
            result["workload"]["raft_log_growth"],
            {
                "measured_messages": DEFAULT_RAFT_LOG_GROWTH_MESSAGES,
                "batch_size": DEFAULT_RAFT_LOG_GROWTH_BATCH_SIZE,
                "minimum_batch_size": 1,
                "maximum_batch_size": MAX_RAFT_LOG_GROWTH_BATCH_SIZE,
                "minimum_messages": MIN_RAFT_LOG_GROWTH_MESSAGES,
                "maximum_messages": MAX_RAFT_LOG_GROWTH_MESSAGES,
                "maximum_logical_payload_bytes": MAX_RAFT_LOG_GROWTH_LOGICAL_PAYLOAD_BYTES,
                "observation_every_publishes": 8,
                "cycle_timeout_seconds": 30.0,
                "minimum_cycle_timeout_seconds": MIN_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
                "maximum_cycle_timeout_seconds": MAX_RAFT_LOG_GROWTH_CYCLE_TIMEOUT_SECONDS,
                "setup_messages_excluded": 1,
                "publish_operation": "publish_batch",
                "message_history_source": "public protocol; first setup publish is offset 0",
                "consensus_history_source": "per-node data-group raft-log.json",
            },
        )

    def test_result_builder_records_snapshot_build_boundary_and_limits(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "snapshot_build_hot_path"],
        ):
            args = parse_args()
        cluster = SimpleNamespace(
            image_id="sha256:test",
            startup_ns=1_000_000,
            peer_proxy_summary=lambda: {"enabled": False, "response_delay_ms": 0},
            stats=SimpleNamespace(summary=lambda: {}),
        )
        with (
            patch.object(cluster_results, "result_metadata", return_value={}),
            patch.object(cluster_results, "resource_limits", return_value={}),
        ):
            result = cluster_results.build_result(
                args,
                run_id="run-id",
                started_at=datetime(2026, 1, 1, tzinfo=UTC),
                cluster=cluster,
                scenarios=[],
            )

        self.assertEqual(
            result["workload"]["snapshot_build_hot_path"],
            {
                "measured_messages": DEFAULT_SNAPSHOT_BUILD_MESSAGES,
                "minimum_messages": MIN_SNAPSHOT_BUILD_MESSAGES,
                "maximum_messages": MAX_SNAPSHOT_BUILD_MESSAGES,
                "maximum_logical_payload_bytes": MAX_SNAPSHOT_BUILD_LOGICAL_PAYLOAD_BYTES,
                "retained_messages": DEFAULT_RETAINED_RECOVERY_MESSAGES,
                "minimum_retained_messages": MIN_RETAINED_RECOVERY_MESSAGES,
                "maximum_retained_messages": MAX_SNAPSHOT_BUILD_RETAINED_MESSAGES,
                "cycle_timeout_seconds": DEFAULT_SNAPSHOT_BUILD_TIMEOUT_SECONDS,
                "setup_messages_excluded": True,
                "retained_state_source": "public durable publishes before measured interval",
                "measurement_boundary": "public durable publish through snapshot build completion",
            },
        )

    def test_result_builder_records_peer_forwarding_stream_setup_semantics(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "peer_forwarding",
                "--messages",
                "10",
                "--warmup",
                "3",
                "--peer-forwarding-stream-count",
                "2",
            ],
        ):
            args = parse_args()
        cluster = SimpleNamespace(
            image_id="sha256:test",
            startup_ns=1_000_000,
            peer_proxy_summary=lambda: {"enabled": False, "response_delay_ms": 0},
            stats=SimpleNamespace(summary=lambda: {}),
        )
        with (
            patch.object(cluster_results, "result_metadata", return_value={}),
            patch.object(cluster_results, "resource_limits", return_value={}),
        ):
            result = cluster_results.build_result(
                args,
                run_id="run-id",
                started_at=datetime(2026, 1, 1, tzinfo=UTC),
                cluster=cluster,
                scenarios=[{"operation": "cluster_peer_forwarding"}],
            )

        forwarding = result["workload"]["peer_forwarding"]
        self.assertEqual(forwarding["stream_count"], 2)
        self.assertEqual(forwarding["data_group_count"], 2)
        self.assertEqual(forwarding["measured_messages_total"], 10)
        self.assertEqual(forwarding["warmup_messages_per_stream"], 3)
        self.assertEqual(forwarding["setup_warmup_messages_total"], 6)
        self.assertTrue(forwarding["setup_excluded_from_measurement"])

    def test_native_log_handle_closes_when_a_node_stops(self) -> None:
        class FakeProcess:
            pid = 123

            def poll(self) -> int:
                return 0

            def wait(self, **_: object) -> int:
                return 0

        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(
                Path("/tmp/runnel"),
                node_count=3,
                ack_timeout_ms=30_000,
                log_dir=Path(directory),
            )
            try:
                with patch.object(
                    cluster_lifecycle.subprocess,
                    "Popen",
                    return_value=FakeProcess(),
                ):
                    cluster._start_node(0, bootstrap=True)
                log_handle = cluster.nodes[0].log_handle
                self.assertIsNotNone(log_handle)
                self.assertFalse(log_handle.closed)

                cluster.stop_node(0)

                self.assertTrue(log_handle.closed)
                self.assertIsNone(cluster.nodes[0].log_handle)
            finally:
                cluster.close()

    def test_default_scenarios_preserve_the_existing_entrypoint_workload(self) -> None:
        with patch.object(sys, "argv", ["cluster.py"]):
            args = parse_args()

        self.assertEqual(args.scenarios, list(DEFAULT_SCENARIOS))
        self.assertNotIn("peer_forwarding", args.scenarios)
        self.assertNotIn("publish_batch", args.scenarios)
        self.assertNotIn("leader_failure_recovery", args.scenarios)
        self.assertNotIn("hot_ordering", args.scenarios)
        self.assertNotIn("retained_hot_path", args.scenarios)
        self.assertNotIn("slow_consumer_backpressure", args.scenarios)

    def test_retained_hot_path_is_opt_in_and_accepts_retained_history(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "retained_hot_path",
                "--retained-messages",
                "2048",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["retained_hot_path"])
        self.assertEqual(args.retained_messages, 2048)

    def test_snapshot_build_hot_path_options_are_bounded_and_opt_in(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "snapshot_build_hot_path",
                "--snapshot-build-messages",
                "128",
                "--snapshot-build-cycle-timeout-seconds",
                "45",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["snapshot_build_hot_path"])
        self.assertEqual(args.snapshot_build_messages, 128)
        self.assertEqual(args.snapshot_build_cycle_timeout_seconds, 45)
        self.assertEqual(DEFAULT_SNAPSHOT_BUILD_MESSAGES, 256)
        self.assertEqual(DEFAULT_SNAPSHOT_BUILD_TIMEOUT_SECONDS, 30)
        self.assertEqual(
            parse_snapshot_build_messages(str(MIN_SNAPSHOT_BUILD_MESSAGES)),
            MIN_SNAPSHOT_BUILD_MESSAGES,
        )
        self.assertEqual(
            parse_snapshot_build_messages(str(MAX_SNAPSHOT_BUILD_MESSAGES)),
            MAX_SNAPSHOT_BUILD_MESSAGES,
        )
        for invalid in (
            str(MIN_SNAPSHOT_BUILD_MESSAGES - 1),
            str(MAX_SNAPSHOT_BUILD_MESSAGES + 1),
            "not-an-integer",
        ):
            with self.subTest(messages=invalid), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                parse_snapshot_build_messages(invalid)

    def test_snapshot_build_hot_path_rejects_unbounded_retention(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "snapshot_build_hot_path",
                "--retained-messages",
                str(MAX_SNAPSHOT_BUILD_RETAINED_MESSAGES + 1),
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "snapshot_build_hot_path",
                "--payload-sizes",
                "8192",
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_snapshot_build_metric_parser_extracts_per_process_deltas(self) -> None:
        metric_names = {
            "builds_started": "runnel_snapshot_builds_started_total",
            "builds_completed": "runnel_snapshot_builds_completed_total",
            "build_failures": "runnel_snapshot_build_failures_total",
            "builds_in_progress": "runnel_snapshot_builds_in_progress",
            "duration_sum_seconds": "runnel_snapshot_build_duration_seconds_sum",
            "duration_count": "runnel_snapshot_build_duration_seconds_count",
            "duration_max_seconds": "runnel_snapshot_build_duration_seconds_max",
        }
        before: dict[str, float] = {}
        after: dict[str, float] = {}
        initial = {
            "builds_started": 1,
            "builds_completed": 1,
            "build_failures": 0,
            "builds_in_progress": 0,
            "duration_sum_seconds": 2.0,
            "duration_count": 1,
            "duration_max_seconds": 2.0,
        }
        final = {
            "builds_started": 3,
            "builds_completed": 3,
            "build_failures": 0,
            "builds_in_progress": 0,
            "duration_sum_seconds": 3.25,
            "duration_count": 3,
            "duration_max_seconds": 2.0,
        }
        for node_name in ("node_1", "node_2"):
            for field, metric_name in metric_names.items():
                before[f"{node_name}.{metric_name}"] = float(initial[field])
                after[f"{node_name}.{metric_name}"] = float(final[field])

        parsed = _snapshot_build_metrics(after, [1, 2])
        deltas = _snapshot_build_metric_deltas(before, after, [1, 2])

        self.assertEqual(parsed["node_1"]["builds_completed"], 3)
        self.assertTrue(deltas["available"])
        self.assertEqual(
            deltas["per_node"]["node_1"]["build_duration_count_delta"], 2
        )
        self.assertEqual(
            deltas["per_node"]["node_1"]["build_duration_seconds_sum_delta"], 1.25
        )
        self.assertEqual(
            deltas["per_node"]["node_1"]["build_duration_seconds_mean"], 0.625
        )
        self.assertIsNone(
            deltas["per_node"]["node_1"]["build_duration_seconds_max_in_this_interval"]
        )
        self.assertEqual(
            deltas["per_node"]["node_2"]["builds_completed_delta"], 2
        )
        self.assertFalse(
            _snapshot_build_metric_deltas(before, None, [1, 2])["available"]
        )

    def test_snapshot_build_memory_summary_is_an_active_sample_lower_bound(self) -> None:
        samples = [
            {
                "1": {
                    "memory_bytes": 100.0,
                    "snapshot_builds_in_progress": 0.0,
                    "snapshot_build_publishes_active": 1.0,
                },
                "2": {
                    "memory_bytes": 200.0,
                    "snapshot_builds_in_progress": 0.0,
                    "snapshot_build_publishes_active": 1.0,
                },
            },
            {
                "1": {
                    "memory_bytes": 150.0,
                    "snapshot_builds_in_progress": 1.0,
                    "snapshot_build_publishes_active": 1.0,
                },
                "2": {
                    "memory_bytes": 220.0,
                    "snapshot_builds_in_progress": 0.0,
                    "snapshot_build_publishes_active": 0.0,
                },
            },
            {
                "1": {
                    "memory_bytes": 180.0,
                    "snapshot_builds_in_progress": 1.0,
                    "snapshot_build_publishes_active": 1.0,
                },
                "2": {
                    "memory_bytes": 240.0,
                    "snapshot_builds_in_progress": 0.0,
                    "snapshot_build_publishes_active": 0.0,
                },
            },
        ]

        summary = ProcessStats._summarize_snapshot_build_memory(samples)

        self.assertEqual(summary["1"]["samples_with_build_in_progress"], 2)
        self.assertEqual(
            summary["1"]["samples_with_build_in_progress_during_measured_publishes"],
            2,
        )
        self.assertEqual(summary["1"]["rss_bytes_max_during_observed_build"], 180.0)
        self.assertEqual(
            summary["1"]["rss_bytes_max_during_observed_build_and_publishes"], 180.0
        )
        self.assertEqual(summary["2"]["samples_with_build_in_progress"], 0)
        self.assertEqual(
            summary["2"]["samples_with_build_in_progress_during_measured_publishes"],
            0,
        )
        self.assertIsNone(summary["2"]["rss_bytes_max_during_observed_build"])
        self.assertIn("lower bound", summary["1"]["rss_scope"])

    def test_process_stats_end_emits_nested_snapshot_build_observations(self) -> None:
        stats = ProcessStats(SimpleNamespace(nodes=[], runtime="process"))
        stats.samples.append(
            {"cpu_seconds": 0.0, "memory_bytes": 100.0, "storage_bytes": 10.0}
        )
        stats.node_samples.append(
            {
                "1": {
                    "cpu_seconds": 0.0,
                    "memory_bytes": 100.0,
                    "storage_bytes": 10.0,
                    "snapshot_builds_in_progress": 1.0,
                    "snapshot_build_publishes_active": 1.0,
                }
            }
        )

        result = stats.end((0, 0, 0.0, 1, 0))

        observations = result["snapshot_build_memory_observations"]
        self.assertEqual(
            observations["per_node"]["1"][
                "samples_with_build_in_progress_during_measured_publishes"
            ],
            1,
        )

    def test_snapshot_build_hot_path_dispatches_selected_payload_size(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "snapshot_build_hot_path",
                "--snapshot-build-messages",
                "128",
                "--retained-messages",
                "1025",
                "--snapshot-build-cycle-timeout-seconds",
                "12",
                "--payload-sizes",
                "100",
            ],
        ):
            args = parse_args()
        cluster = SimpleNamespace()
        expected = {"operation": "cluster_snapshot_build_hot_path"}
        with patch.object(
            cluster_cli, "run_snapshot_build_hot_path", return_value=expected
        ) as run_snapshot_build:
            results = cluster_cli.run_scenarios(args, cluster, "run-id")

        self.assertEqual(results, [expected])
        run_snapshot_build.assert_called_once_with(
            cluster,
            "cluster_run-id_snapshot_build_hot_path_100",
            "x" * 100,
            128,
            1025,
            12,
        )

    def test_raft_log_growth_is_opt_in_and_uses_bounded_workload_options(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "raft_log_growth",
                "--raft-log-growth-messages",
                "128",
                "--raft-log-growth-batch-size",
                "8",
                "--raft-log-growth-observation-every",
                "2",
                "--raft-log-growth-cycle-timeout-seconds",
                "45",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["raft_log_growth"])
        self.assertEqual(args.raft_log_growth_messages, 128)
        self.assertEqual(args.raft_log_growth_batch_size, 8)
        self.assertEqual(args.raft_log_growth_observation_every, 2)
        self.assertEqual(args.raft_log_growth_cycle_timeout_seconds, 45)
        self.assertEqual(DEFAULT_RAFT_LOG_GROWTH_MESSAGES, 256)
        self.assertEqual(DEFAULT_RAFT_LOG_GROWTH_BATCH_SIZE, 1)
        self.assertEqual(parse_raft_log_growth_batch_size("1"), 1)
        self.assertEqual(
            parse_raft_log_growth_batch_size(str(MAX_RAFT_LOG_GROWTH_BATCH_SIZE)),
            MAX_RAFT_LOG_GROWTH_BATCH_SIZE,
        )
        self.assertEqual(
            parse_raft_log_growth_messages(str(MIN_RAFT_LOG_GROWTH_MESSAGES)),
            MIN_RAFT_LOG_GROWTH_MESSAGES,
        )
        self.assertEqual(
            parse_raft_log_growth_messages(str(MAX_RAFT_LOG_GROWTH_MESSAGES)),
            MAX_RAFT_LOG_GROWTH_MESSAGES,
        )
        self.assertEqual(parse_raft_log_growth_observation_every("1"), 1)

    def test_raft_log_growth_rejects_unbounded_options_and_skip_recovery(self) -> None:
        for invalid in (
            str(MIN_RAFT_LOG_GROWTH_MESSAGES - 1),
            str(MAX_RAFT_LOG_GROWTH_MESSAGES + 1),
            "not-an-integer",
        ):
            with self.subTest(messages=invalid), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                parse_raft_log_growth_messages(invalid)
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_raft_log_growth_observation_every("0")
        for invalid in ("0", str(MAX_RAFT_LOG_GROWTH_BATCH_SIZE + 1), "bad"):
            with self.subTest(batch_size=invalid), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                parse_raft_log_growth_batch_size(invalid)
        with patch.object(
            sys,
            "argv",
            ["cluster.py", "--scenarios", "raft_log_growth", "--skip-recovery"],
        ):
            with self.assertRaises(SystemExit):
                parse_args()
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "raft_log_growth",
                "--raft-log-growth-messages",
                str(MAX_RAFT_LOG_GROWTH_MESSAGES),
                "--payload-sizes",
                str(MAX_RAFT_LOG_GROWTH_LOGICAL_PAYLOAD_BYTES),
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "raft_log_growth",
                "--raft-log-growth-cycle-timeout-seconds",
                "0.5",
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_raft_log_growth_dispatches_selected_payload_size(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "raft_log_growth",
                "--raft-log-growth-batch-size",
                "7",
                "--payload-sizes",
                "100",
            ],
        ):
            args = parse_args()
        cluster = SimpleNamespace()
        expected = {"operation": "cluster_raft_log_growth"}
        with patch.object(
            cluster_cli, "run_raft_log_growth", return_value=expected
        ) as run_growth:
            results = cluster_cli.run_scenarios(args, cluster, "run-id")

        run_growth.assert_called_once_with(
            cluster,
            "cluster_run-id_raft_log_growth_100",
            "x" * 100,
            args.raft_log_growth_messages,
            args.raft_log_growth_batch_size,
            args.raft_log_growth_observation_every,
            args.raft_log_growth_cycle_timeout_seconds,
        )
        self.assertEqual(results, [expected])

    def test_raft_data_group_observation_separates_consensus_and_state_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            group = Path(temporary) / "groups" / "data" / "stream-id"
            state = group / "state-machine"
            state.mkdir(parents=True)
            (group / "raft-log.json").write_text(
                '{"version":1,"last_purged_log_id":{"index":10},'
                '"log":{"11":{"log_id":{"index":11}},'
                '"12":{"log_id":{"index":12}}},'
                '"committed":{"index":12},"vote":null}',
                encoding="utf-8",
            )
            (state / "state-machine.log").write_bytes(b"journal")
            (state / "state-machine.json").write_bytes(b"checkpoint")
            (state / "snapshot.json").write_bytes(b"snapshot")

            observed = _raft_data_group_state(3, group)

        self.assertEqual(observed["node_id"], 3)
        self.assertEqual(observed["paths"]["raft_log"]["retained_log_entries"], 2)
        self.assertEqual(observed["paths"]["raft_log"]["first_log_index"], 11)
        self.assertEqual(observed["paths"]["raft_log"]["last_purged_log_index"], 10)
        self.assertEqual(observed["paths"]["state_machine_journal"]["file_bytes"], 7)
        self.assertEqual(
            observed["paths"]["state_machine_checkpoint"]["file_bytes"], 10
        )
        self.assertEqual(observed["paths"]["snapshot"]["file_bytes"], 8)

    def test_raft_log_growth_cycle_requires_new_snapshot_and_purged_index(self) -> None:
        before = {
            "node_1": {
                "paths": {
                    "raft_log": {"last_purged_log_index": None},
                    "snapshot": {"file_bytes": 0},
                }
            }
        }
        snapshot_without_purge = {
            "node_1": {
                "paths": {
                    "raft_log": {"last_purged_log_index": None},
                    "snapshot": {"file_bytes": 12},
                }
            }
        }
        purged_without_snapshot = {
            "node_1": {
                "paths": {
                    "raft_log": {"last_purged_log_index": 32},
                    "snapshot": {"file_bytes": 0},
                }
            }
        }
        snapshot_and_purge = {
            "node_1": {
                "paths": {
                    "raft_log": {"last_purged_log_index": 32},
                    "snapshot": {"file_bytes": 12},
                }
            }
        }

        self.assertFalse(_observed_purge_advanced(before, snapshot_without_purge))
        self.assertFalse(_observed_purge_advanced(before, purged_without_snapshot))
        self.assertTrue(_observed_purge_advanced(before, snapshot_and_purge))

    def test_raft_log_growth_batches_records_and_preserves_cycle_and_recovery(self) -> None:
        def state(*, purged: bool) -> dict[str, dict[str, object]]:
            per_node: dict[str, dict[str, object]] = {}
            for node_id in range(1, 4):
                per_node[f"node_{node_id}"] = {
                    "node_id": node_id,
                    "paths": {
                        "raft_log": {
                            "file_bytes": 100 if purged else 500,
                            "retained_log_entries": 4 if purged else 12,
                            "first_log_index": 33 if purged else 1,
                            "last_log_index": 36 if purged else 12,
                            "last_purged_log_index": 32 if purged else None,
                        },
                        "state_machine_journal": {"file_bytes": 10},
                        "state_machine_checkpoint": {"file_bytes": 20},
                        "snapshot": {"file_bytes": 50 if purged else 0},
                    },
                }
            return per_node

        clients = [SimpleNamespace(close=lambda: None) for _ in range(3)]
        nodes = [
            SimpleNamespace(node_id=node_id, data_dir=f"node-{node_id}")
            for node_id in range(1, 4)
        ]
        cluster = SimpleNamespace(
            node_count=3,
            nodes=nodes,
            stats=object(),
            metrics=lambda: None,
            client=lambda index: clients[index],
            restart_node=lambda _index: 10_000,
        )
        batch_calls: list[tuple[int, int]] = []

        def publish_batch_message(
            _client: object,
            _stream: str,
            _payload: str,
            batch_size: int,
            expected_offset: int,
        ) -> tuple[int, int]:
            batch_calls.append((batch_size, expected_offset))
            return batch_size, 1_000

        observed_states = [state(purged=False)] + [
            state(purged=True) for _ in range(4)
        ]
        with (
            patch("cluster_scenarios.create_stream"),
            patch("cluster_scenarios.publish", return_value=(0, 1_000)),
            patch(
                "cluster_scenarios._data_group_directories",
                return_value={
                    node_id: Path(f"node-{node_id}") for node_id in range(1, 4)
                },
            ),
            patch(
                "cluster_scenarios._cluster_raft_data_group_state",
                side_effect=observed_states,
            ),
            patch(
                "cluster_scenarios.publish_batch_request",
                side_effect=publish_batch_message,
            ),
            patch(
                "cluster_scenarios.measure_scenario",
                side_effect=lambda _stats, operation, **_kwargs: operation(),
            ),
            patch(
                "cluster_scenarios.poll",
                return_value=({"payload": "payload"}, 2_000),
            ),
            patch("cluster_scenarios.acknowledge", return_value=3_000),
        ):
            result = run_raft_log_growth(
                cluster,
                "events",
                "payload",
                messages=5,
                batch_size=2,
                observation_every=2,
                cycle_timeout_seconds=1,
            )

        self.assertEqual(batch_calls, [(2, 1), (2, 3), (1, 5)])
        self.assertEqual(result["messages"], 5)
        self.assertEqual(result["latency_sample_count"], 3)
        self.assertEqual(result["metadata"]["batch_size_counts"], {"2": 2, "1": 1})
        self.assertEqual(result["metadata"]["final_batch_size"], 1)
        self.assertEqual(
            [sample["message_index"] for sample in result["metadata"]["observations"]],
            [2, 4, 5, 5],
        )
        self.assertIn(
            "offset 0 is setup", result["metadata"]["message_history_boundary"]
        )
        self.assertIn(
            "not inferred from batch or message count",
            result["metadata"]["consensus_history_boundary"],
        )
        self.assertTrue(result["metadata"]["snapshot_purge_cycle_observed"])
        self.assertEqual(result["recovery"]["metadata"]["replayed_offset"], 0)
        self.assertTrue(result["recovery"]["metadata"]["earliest_payload_verified"])

    def test_raft_log_growth_does_not_retry_an_ambiguous_batch(self) -> None:
        clients = [SimpleNamespace(close=lambda: None) for _ in range(3)]
        nodes = [
            SimpleNamespace(node_id=node_id, data_dir=f"node-{node_id}")
            for node_id in range(1, 4)
        ]
        cluster = SimpleNamespace(
            node_count=3,
            nodes=nodes,
            stats=object(),
            metrics=lambda: None,
            client=lambda index: clients[index],
        )
        initial_state = {f"node_{node_id}": {} for node_id in range(1, 4)}
        with (
            patch("cluster_scenarios.create_stream"),
            patch("cluster_scenarios.publish", return_value=(0, 1_000)),
            patch(
                "cluster_scenarios._data_group_directories",
                return_value={
                    node_id: Path(f"node-{node_id}") for node_id in range(1, 4)
                },
            ),
            patch(
                "cluster_scenarios._cluster_raft_data_group_state",
                return_value=initial_state,
            ),
            patch(
                "cluster_scenarios.measure_scenario",
                side_effect=lambda _stats, operation, **_kwargs: operation(),
            ),
            patch(
                "cluster_scenarios.publish_batch_request",
                side_effect=BenchmarkError("broker closed after request write"),
            ) as publish_batch,
        ):
            with self.assertRaisesRegex(
                BenchmarkError,
                r"offsets 1-8; the request was not retried and may have committed some records",
            ):
                run_raft_log_growth(
                    cluster,
                    "events",
                    "payload",
                    messages=64,
                    batch_size=8,
                    observation_every=8,
                    cycle_timeout_seconds=1,
                )

        publish_batch.assert_called_once()

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
                "--peer-forwarding-stream-count",
                "2",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["peer_forwarding"])
        self.assertEqual(args.peer_forwarding_concurrency, 8)
        self.assertEqual(args.peer_forwarding_stream_count, 2)
        self.assertEqual(args.peer_response_delay_ms, 5)
        self.assertEqual(args.peer_forwarding_timeout_seconds, 12.5)
        self.assertEqual(parse_positive_float("0.5"), 0.5)

    def test_peer_forwarding_stream_count_is_bounded_and_fits_total_messages(self) -> None:
        with patch.object(sys, "argv", ["cluster.py"]):
            args = parse_args()
        self.assertEqual(args.peer_forwarding_stream_count, 1)

        for arguments in (
            ["cluster.py", "--peer-forwarding-stream-count", "0"],
            [
                "cluster.py",
                "--peer-forwarding-stream-count",
                str(MAX_PEER_FORWARDING_STREAM_COUNT + 1),
            ],
            [
                "cluster.py",
                "--scenarios",
                "peer_forwarding",
                "--messages",
                "2",
                "--peer-forwarding-stream-count",
                "3",
            ],
        ):
            with patch.object(sys, "argv", arguments), self.subTest(arguments=arguments):
                with self.assertRaises(SystemExit):
                    parse_args()

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
            patch("cluster_scenarios.publish_stream") as publish_stream,
            patch("cluster_scenarios.publish_batch_request", side_effect=publish_batch_message),
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
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

    def test_peer_response_proxy_forwards_and_summarizes_delayed_response(self) -> None:
        def receive_exact(sock: socket.socket, size: int) -> bytes:
            received = bytearray()
            while len(received) < size:
                chunk = sock.recv(size - len(received))
                if not chunk:
                    raise AssertionError("target closed a partial test frame")
                received.extend(chunk)
            return bytes(received)

        class TargetHandler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                header = receive_exact(self.request, 4)
                payload = receive_exact(self.request, int.from_bytes(header, "big"))
                self.request.sendall(header + payload)

        target = socketserver.ThreadingTCPServer(("127.0.0.1", 0), TargetHandler)
        target.daemon_threads = True
        target_thread = threading.Thread(target=target.serve_forever, daemon=True)
        target_thread.start()
        proxy = PeerResponseDelayProxy(target.server_address[1], 1)
        try:
            proxy.start()
            payload = b'{"Forward":true}'
            frame = len(payload).to_bytes(4, "big") + payload
            with socket.create_connection(("127.0.0.1", proxy.port), timeout=2) as client:
                client.sendall(frame)
                self.assertEqual(receive_exact(client, len(frame)), frame)
        finally:
            proxy.close()
            target.shutdown()
            target.server_close()
            target_thread.join(timeout=5)

        self.assertEqual(
            proxy.summary(),
            {
                "target_port": target.server_address[1],
                "listen_port": proxy.port,
                "response_delay_ms": 1,
                "connections": 1,
                "max_active_connections": 1,
                "requests": 1,
                "responses": 1,
                "delayed_responses": 1,
            },
        )

    def test_peer_response_proxy_closes_client_after_oversized_frame(self) -> None:
        proxy = PeerResponseDelayProxy(0, 0)
        proxy.start()
        try:
            with socket.create_connection(("127.0.0.1", proxy.port), timeout=2) as client:
                client.sendall((64 * 1024 * 1024 + 1).to_bytes(4, "big"))
                self.assertEqual(client.recv(1), b"")
        finally:
            proxy.close()

        self.assertFalse(proxy.thread.is_alive())
        self.assertEqual(proxy.server.active_connections, 0)
        self.assertEqual(proxy.summary()["connections"], 1)
        self.assertEqual(proxy.summary()["requests"], 0)

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
            peer_connection_census=lambda: {"available": True, "per_node": {}},
            client=lambda _index, **_: SimpleNamespace(close=lambda: None),
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster_scenarios.publish_stream"),
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch("cluster_scenarios.publish", return_value=(0, 100)),
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
        census_samples = iter(
            [
                {
                    "available": True,
                    "per_node": {"1": {"established_socket_endpoint_count": 4}},
                },
                {
                    "available": True,
                    "per_node": {"1": {"established_socket_endpoint_count": 6}},
                },
            ]
        )
        cluster = SimpleNamespace(
            node_count=3,
            peer_response_delay_ms=5,
            stats=object(),
            metrics=lambda: None,
            peer_proxy_summary=lambda: {"enabled": True},
            peer_connection_census=lambda: next(census_samples),
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
            patch("cluster_scenarios.publish_stream"),
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch("cluster_scenarios.publish", side_effect=publish_message),
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
        self.assertEqual(
            result["metadata"]["peer_connection_census"]["sample_boundaries"],
            ["after_setup_warmup", "after_measured_forwarding"],
        )
        self.assertEqual(
            result["metadata"]["peer_connection_census"]["after_setup_warmup"][
                "per_node"
            ]["1"]["established_socket_endpoint_count"],
            4,
        )
        self.assertEqual(
            result["metadata"]["peer_connection_census"]["after_measured_forwarding"][
                "per_node"
            ]["1"]["established_socket_endpoint_count"],
            6,
        )

    def test_peer_forwarding_distributes_total_messages_and_checks_offsets_per_stream(self) -> None:
        created_streams: list[tuple[str, int]] = []
        published_streams: list[str] = []
        offsets = {"events-1": 2, "events-2": 2}
        lock = threading.Lock()
        cluster = SimpleNamespace(
            node_count=3,
            peer_response_delay_ms=0,
            stats=object(),
            metrics=lambda: None,
            peer_proxy_summary=lambda: {"enabled": False},
            peer_connection_census=lambda: {"available": True, "per_node": {}},
            client=lambda _index, **_: SimpleNamespace(close=lambda: None),
        )

        def publish_setup(_client: object, stream: str, _payload: str, count: int) -> None:
            created_streams.append((stream, count))

        def publish_message(
            _client: object, stream: str, _payload: str
        ) -> tuple[int, int]:
            with lock:
                published_streams.append(stream)
                offset = offsets[stream]
                offsets[stream] += 1
            return offset, 100

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster_scenarios.publish_stream", side_effect=publish_setup),
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch("cluster_scenarios.publish", side_effect=publish_message),
        ):
            result = run_peer_forwarding(
                cluster,
                "events",
                "payload",
                messages=5,
                warmup=2,
                concurrency=3,
                timeout_seconds=1,
                stream_count=2,
            )

        self.assertEqual(created_streams, [("events-1", 2), ("events-2", 2)])
        self.assertEqual(published_streams.count("events-1"), 3)
        self.assertEqual(published_streams.count("events-2"), 2)
        self.assertEqual(result["messages"], 5)
        self.assertEqual(result["metadata"]["stream_count"], 2)
        self.assertEqual(result["metadata"]["data_group_count"], 2)
        self.assertEqual(result["metadata"]["setup_warmup_messages_total"], 4)
        self.assertEqual(result["metadata"]["measurement_messages_total"], 5)
        self.assertEqual(result["metadata"]["measurement_messages_per_stream_min"], 2)
        self.assertEqual(result["metadata"]["measurement_messages_per_stream_max"], 3)

    def test_leader_failure_recovery_reports_only_observed_public_endpoint_service(self) -> None:
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

        with patch("cluster_scenarios.measure_scenario", side_effect=run_measurement), patch(
            "cluster_scenarios.time.sleep"
        ):
            result = run_leader_failure_recovery(
                cluster, "leader-failure-events", "payload", timeout_seconds=1
            )

        self.assertEqual(events, [("stop", 0), ("restart", 0)])
        self.assertEqual(result["operation"], "cluster_leader_failure_recovery")
        self.assertEqual(result["restart_ready_seconds"], 0.002)
        self.assertEqual(result["metadata"]["failed_node"], 1)
        self.assertEqual(result["metadata"]["failed_node_role"], "bootstrap_assumed_leader")
        self.assertEqual(result["metadata"]["surviving_nodes"], [2, 3])
        self.assertEqual(result["metadata"]["bootstrap_assumed_initial_leader_node"], 1)
        self.assertEqual(result["metadata"]["initial_leader_selection"], "bootstrap_assumption")
        self.assertEqual(
            result["metadata"]["replacement_leader_identity"],
            "not exposed by the provisional public protocol",
        )
        self.assertEqual(
            result["metadata"]["public_request_endpoints"],
            {
                "publish_after_failure": [2],
                "poll_after_failure": [2, 3],
                "ack_after_failure": [3, 2],
                "publish_after_restart": [1],
                "poll_after_restart": [1],
                "ack_after_restart": [2],
            },
        )
        self.assertTrue(result["metadata"]["verified"]["survivor_endpoint_requests_succeeded"])
        self.assertTrue(
            result["metadata"]["verified"]["restarted_endpoint_publish_and_poll_succeeded"]
        )
        self.assertEqual(result["metadata"]["restarted_endpoint_consumed_offset"], 2)
        self.assertEqual(result["metadata"]["request_attempts"]["publish_after_failure"], 2)
        self.assertNotIn("replacement_leader_observed", result["metadata"])
        self.assertNotIn("initial_leader_node", result["metadata"])
        self.assertNotIn("restart_recovered_message_offset", result["metadata"])
        self.assertNotIn(
            "surviving_nodes_elected_and_served", result["metadata"]["verified"]
        )
        self.assertNotIn("surviving_nodes_served", result["metadata"]["verified"])
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

        with patch("cluster_scenarios.measure_scenario", side_effect=run_measurement), patch(
            "cluster_scenarios.time.sleep"
        ):
            result = run_follower_failure_recovery(
                cluster, "follower-failure-events", "payload", timeout_seconds=1
            )

        self.assertEqual(events, [("stop", 1), ("restart", 1)])
        self.assertEqual(result["operation"], "cluster_follower_failure_recovery")
        self.assertEqual(result["metadata"]["failed_node"], 2)
        self.assertEqual(result["metadata"]["failed_node_role"], "follower")
        self.assertEqual(result["metadata"]["failure_state"], "follower_process_stop")
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

    def test_peer_tcp_census_matches_owned_established_socket_endpoints(self) -> None:
        tcp = """\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:1B58 0100007F:9C40 01 00000000:00000000 00:00000000 00000000 1000 0 101
   1: 0100007F:9C40 0100007F:1F40 01 00000000:00000000 00:00000000 00000000 1000 0 102
   2: 0100007F:9C40 0100007F:1F40 0A 00000000:00000000 00:00000000 00000000 1000 0 103
   3: 0100007F:9C40 0100007F:9C41 01 00000000:00000000 00:00000000 00000000 1000 0 104
   4: 0100007F:1B58 0100007F:9C40 01 00000000:00000000 00:00000000 00000000 1000 0 105
"""
        tcp6 = """\
  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000000:9C40 00000000000000000000000000000000:1B58 01 00000000:00000000 00:00000000 00000000 1000 0 106
"""

        count = parse_owned_peer_tcp_endpoints(
            [tcp, tcp6],
            {"101", "102", "103", "104", "106"},
            peer_listener_port=7000,
            peer_destination_ports={7000, 8000},
        )

        self.assertEqual(count, 3)

    def test_process_peer_tcp_census_joins_procfs_socket_fds_to_peer_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            process_root = Path(temporary_directory) / "1234"
            (process_root / "fd").mkdir(parents=True)
            (process_root / "net").mkdir()
            (process_root / "fd" / "7").symlink_to("socket:[12345]")
            (process_root / "fd" / "8").symlink_to("/tmp/not-a-socket")
            (process_root / "net" / "tcp").write_text(
                "  sl local_address rem_address st tx_queue rx_queue tr "
                "tm->when retrnsmt uid timeout inode\n"
                "0: 0100007F:1B58 0100007F:9C40 01 00000000:00000000 "
                "00:00000000 00000000 1000 0 12345\n",
                encoding="utf-8",
            )

            count = process_peer_tcp_endpoint_count(
                1234,
                peer_listener_port=7000,
                peer_destination_ports={8000},
                proc_root=Path(temporary_directory),
            )

        self.assertEqual(count, 1)

    def test_unreadable_process_has_no_peer_socket_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            count = process_peer_tcp_endpoint_count(
                1234,
                peer_listener_port=7000,
                peer_destination_ports={8000},
                proc_root=Path(temporary_directory),
            )

        self.assertIsNone(count)

    def test_container_peer_census_resolves_the_broker_host_pid(self) -> None:
        node = SimpleNamespace(
            container=SimpleNamespace(created=True, name="runnel-node-2")
        )
        with patch(
            "cluster_resources.subprocess.run",
            return_value=SimpleNamespace(
                returncode=0, stdout="/runnel-node-2 4321\n"
            ),
        ) as inspect:
            pids = _container_host_pids([node])

        self.assertEqual(pids, {"runnel-node-2": 4321})
        self.assertEqual(
            inspect.call_args.args[0],
            [
                "docker",
                "inspect",
                "--format",
                "{{.Name}} {{.State.Pid}}",
                "runnel-node-2",
            ],
        )

    def test_container_procfs_fallback_counts_pid_one_sockets_with_a_bounded_probe(self) -> None:
        output = (
            "__RUNNEL_UIDS__10001 10001\n"
            "SOCKET socket:[12345]\n"
            "__RUNNEL_TCP4__\n"
            "  sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
            "   0: 0100007F:1B59 0100007F:1B5A 01 00000000:00000000 00:00000000 00000000 1000 0 12345\n"
            "__RUNNEL_TCP6__\n"
        )
        with patch(
            "cluster_resources.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout=output),
        ) as docker_exec:
            count = container_peer_tcp_endpoint_count(
                "runnel-node-1",
                peer_listener_port=7001,
                peer_destination_ports={7002},
            )

        self.assertEqual(
            count,
            {"count": 1, "process_identity_verified": True},
        )
        command = docker_exec.call_args.args[0]
        self.assertEqual(command[:3], ["docker", "exec", "runnel-node-1"])
        self.assertIn("/proc/1/fd/*", command[-1])
        self.assertIn("/proc/1/net/tcp", command[-1])
        self.assertIn(
            "if [ -e /proc/1/net/tcp6 ]; then cat /proc/1/net/tcp6 || exit 1; fi",
            command[-1],
        )
        self.assertIn('[ "$exec_uid" = "$broker_uid" ] || exit 2', command[-1])
        self.assertEqual(
            docker_exec.call_args.kwargs["timeout"],
            DEFAULT_PROBE_TIMEOUT_SECONDS,
        )

    def test_container_procfs_fallback_rejects_a_mismatched_process_uid(self) -> None:
        with patch(
            "cluster_resources.subprocess.run",
            return_value=SimpleNamespace(
                returncode=0,
                stdout="__RUNNEL_UIDS__1000 10001\nSOCKET socket:[12345]\n",
            ),
        ):
            count = container_peer_tcp_endpoint_count(
                "runnel-node-1",
                peer_listener_port=7001,
                peer_destination_ports={7002},
            )

        self.assertIsNone(count)

    def test_container_procfs_fallback_timeout_is_bounded_and_unavailable(self) -> None:
        with patch(
            "cluster_resources.subprocess.run",
            side_effect=subprocess.TimeoutExpired("docker exec", 2),
        ) as docker_exec:
            count = container_peer_tcp_endpoint_count(
                "runnel-node-1",
                peer_listener_port=7001,
                peer_destination_ports={7002},
            )

        self.assertIsNone(count)
        self.assertEqual(
            docker_exec.call_args.kwargs["timeout"],
            DEFAULT_PROBE_TIMEOUT_SECONDS,
        )

    def test_container_procfs_fallback_rejects_a_failed_tcp6_read(self) -> None:
        with patch(
            "cluster_resources.subprocess.run",
            return_value=SimpleNamespace(
                returncode=1,
                stdout=(
                    "__RUNNEL_UIDS__10001 10001\n"
                    "__RUNNEL_TCP4__\n"
                    "__RUNNEL_TCP6__\n"
                ),
            ),
        ):
            count = container_peer_tcp_endpoint_count(
                "runnel-node-1",
                peer_listener_port=7001,
                peer_destination_ports={7002},
            )

        self.assertIsNone(count)

    def test_container_peer_census_uses_exec_fallback_with_explicit_provenance(self) -> None:
        node = SimpleNamespace(
            node_id=1,
            peer_port=7001,
            peer_address_port=7001,
            process=None,
            container=SimpleNamespace(created=True, name="runnel-node-1"),
        )
        cluster = SimpleNamespace(runtime="container", nodes=[node])
        with (
            patch(
                "cluster_resources._container_host_pids",
                return_value={"runnel-node-1": 201},
            ),
            patch(
                "cluster_resources.process_peer_tcp_endpoint_count",
                return_value=None,
            ),
            patch(
                "cluster_resources.container_peer_tcp_endpoint_count",
                return_value={"count": 0, "process_identity_verified": True},
            ) as fallback,
        ):
            census = peer_connection_census(cluster)

        self.assertTrue(census["available"])
        self.assertEqual(
            census["per_node"]["1"],
            {
                "available": True,
                "established_socket_endpoint_count": 0,
                "observation_source": "docker_exec_container_procfs_fd_inode_join",
                "docker_host_pid_resolved": True,
                "container_process_identity_verified": True,
            },
        )
        self.assertEqual(
            census["observation_sources"],
            ["docker_exec_container_procfs_fd_inode_join"],
        )
        fallback.assert_called_once_with(
            "runnel-node-1",
            peer_listener_port=7001,
            peer_destination_ports=set(),
        )

    def test_container_peer_census_remains_unavailable_when_both_procfs_paths_fail(self) -> None:
        node = SimpleNamespace(
            node_id=1,
            peer_port=7001,
            peer_address_port=7001,
            process=None,
            container=SimpleNamespace(created=True, name="runnel-node-1"),
        )
        cluster = SimpleNamespace(runtime="container", nodes=[node])
        with (
            patch(
                "cluster_resources._container_host_pids",
                return_value={"runnel-node-1": 201},
            ),
            patch(
                "cluster_resources.process_peer_tcp_endpoint_count",
                return_value=None,
            ),
            patch(
                "cluster_resources.container_peer_tcp_endpoint_count",
                return_value=None,
            ),
        ):
            census = peer_connection_census(cluster)

        self.assertFalse(census["available"])
        self.assertEqual(
            census["per_node"]["1"],
            {
                "available": False,
                "unavailable_reason": "procfs_socket_ownership_unavailable",
            },
        )

    def test_peer_census_uses_broker_owned_process_sockets_for_each_node(self) -> None:
        nodes = [
            SimpleNamespace(
                node_id=1,
                peer_port=7001,
                peer_address_port=7001,
                process=SimpleNamespace(pid=101, poll=lambda: None),
                container=None,
            ),
            SimpleNamespace(
                node_id=2,
                peer_port=7002,
                peer_address_port=7002,
                process=SimpleNamespace(pid=102, poll=lambda: None),
                container=None,
            ),
        ]
        cluster = SimpleNamespace(runtime="process", nodes=nodes)
        with patch(
            "cluster_resources.process_peer_tcp_endpoint_count",
            side_effect=[3, 5],
        ) as count_sockets:
            census = peer_connection_census(cluster)

        self.assertTrue(census["available"])
        self.assertEqual(
            {
                node_id: value["established_socket_endpoint_count"]
                for node_id, value in census["per_node"].items()
            },
            {"1": 3, "2": 5},
        )
        self.assertEqual(
            count_sockets.call_args_list[0].kwargs,
            {
                "peer_listener_port": 7001,
                "peer_destination_ports": {7002},
            },
        )

    def test_container_peer_census_reports_missing_process_without_a_count(self) -> None:
        node = SimpleNamespace(
            node_id=2,
            peer_port=7000,
            peer_address_port=7000,
            process=None,
            container=SimpleNamespace(created=True, name="runnel-node-2"),
        )
        cluster = SimpleNamespace(runtime="container", nodes=[node])
        with patch("cluster_resources._container_host_pids", return_value={}):
            census = peer_connection_census(cluster)

        self.assertFalse(census["available"])
        self.assertEqual(
            census["per_node"]["2"],
            {
                "available": False,
                "unavailable_reason": "broker_process_unavailable",
            },
        )

    def test_container_peer_census_uses_each_brokers_procfs_network_namespace(self) -> None:
        nodes = [
            SimpleNamespace(
                node_id=1,
                peer_port=7000,
                peer_address_port=7000,
                process=None,
                container=SimpleNamespace(created=True, name="runnel-node-1"),
            ),
            SimpleNamespace(
                node_id=2,
                peer_port=7000,
                peer_address_port=7000,
                process=None,
                container=SimpleNamespace(created=True, name="runnel-node-2"),
            ),
        ]
        cluster = SimpleNamespace(runtime="container", nodes=nodes)
        with (
            patch(
                "cluster_resources._container_host_pids",
                return_value={"runnel-node-1": 201, "runnel-node-2": 202},
            ),
            patch(
                "cluster_resources.process_peer_tcp_endpoint_count",
                side_effect=[4, 6],
            ) as count_sockets,
        ):
            census = peer_connection_census(cluster)

        self.assertTrue(census["available"])
        self.assertEqual(
            {
                node_id: value["established_socket_endpoint_count"]
                for node_id, value in census["per_node"].items()
            },
            {"1": 4, "2": 6},
        )
        self.assertEqual(
            count_sockets.call_args_list[0].args,
            (201,),
        )
        self.assertEqual(
            count_sockets.call_args_list[1].kwargs,
            {
                "peer_listener_port": 7000,
                "peer_destination_ports": {7000},
            },
        )

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

    def test_slow_consumer_backpressure_options_are_opt_in_and_bounded(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--scenarios",
                "slow_consumer_backpressure",
                "--slow-consumer-timeout-seconds",
                "12.5",
            ],
        ):
            args = parse_args()

        self.assertEqual(args.scenarios, ["slow_consumer_backpressure"])
        self.assertEqual(args.slow_consumer_timeout_seconds, 12.5)
        self.assertEqual(
            DEFAULT_SLOW_CONSUMER_BACKPRESSURE_TIMEOUT_SECONDS, 60.0
        )
        with patch.object(
            sys,
            "argv",
            [
                "cluster.py",
                "--slow-consumer-timeout-seconds",
                str(MAX_SLOW_CONSUMER_BACKPRESSURE_TIMEOUT_SECONDS + 1),
            ],
        ):
            with self.assertRaises(SystemExit):
                parse_args()

    def test_slow_consumer_backpressure_verifies_duplicate_delivery_window(self) -> None:
        state = {"next_offset": 0, "in_flight": None}

        class FakeClient:
            def request(self, request: dict[str, object]) -> tuple[dict[str, object], int]:
                if request["op"] == "poll":
                    if state["in_flight"] is None:
                        state["in_flight"] = state["next_offset"]
                    return {
                        "type": "message",
                        "offset": state["in_flight"],
                        "payload": "payload",
                        "delivery_attempt": 1,
                    }, 100
                if request["op"] == "ack":
                    if request["offset"] != state["in_flight"]:
                        raise AssertionError(f"unexpected acknowledgement: {request}")
                    state["next_offset"] += 1
                    state["in_flight"] = None
                    return {"type": "acknowledged"}, 200
                raise AssertionError(f"unexpected request: {request}")

            def close(self) -> None:
                return None

        clients = [FakeClient() for _ in range(3)]
        cluster = SimpleNamespace(
            node_count=3,
            stats=object(),
            metrics=lambda: None,
            client=lambda index, **_: clients[index],
        )

        def run_measurement(_stats: object, operation: object, **_: object) -> dict:
            return operation()

        with (
            patch("cluster_scenarios.preload") as preload,
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch("cluster_scenarios.time.sleep"),
        ):
            result = run_slow_consumer_backpressure(
                cluster,
                "events",
                "payload",
                messages=3,
                processing_delay_ms=10,
                timeout_seconds=1,
            )

        preload.assert_called_once_with(cluster, "events", "payload", 3)
        backpressure = result["metadata"]["backpressure"]
        self.assertEqual(backpressure["duplicate_polls"], 3)
        self.assertEqual(backpressure["duplicate_matches"], 3)
        self.assertEqual(backpressure["max_logical_in_flight_deliveries_observed"], 1)
        self.assertTrue(backpressure["delivery_window_verified"])
        self.assertFalse(backpressure["publisher_throttling_or_rejection_exercised"])

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
            patch("cluster_scenarios.preload") as preload,
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch("cluster_scenarios.poll", return_value=({"offset": 0, "payload": payload}, 100)),
            patch("cluster_scenarios.acknowledge", return_value=200) as acknowledge,
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

    def test_retained_recovery_declares_latency_and_resource_sample_windows(self) -> None:
        events: list[str] = []

        class Measurements:
            def begin(self) -> object:
                events.append("resource_start")
                return object()

            def end(self, _token: object) -> dict[str, str]:
                events.append("resource_end")
                return {"window": "recorded"}

        client = SimpleNamespace(close=lambda: events.append("client_close"))
        cluster = SimpleNamespace(
            nodes=[SimpleNamespace(node_id=1)],
            stats=Measurements(),
            metrics=lambda: None,
            restart_node=lambda _: events.append("node_restart") or 2_000_000,
            client=lambda _: client,
        )
        clock_values = iter((100, 200))
        timer_names = iter(("latency_start", "latency_end"))

        def record_timer_boundary() -> int:
            events.append(next(timer_names))
            return next(clock_values)

        def poll_record(*_args: object) -> tuple[dict[str, object], int]:
            events.append("replay")
            return {"offset": 0, "payload": "payload"}, 100

        def acknowledge_record(*_args: object) -> int:
            events.append("acknowledge")
            return 200

        with (
            patch("cluster_scenarios.preload"),
            patch(
                "cluster_scenarios.time.perf_counter_ns",
                side_effect=record_timer_boundary,
            ),
            patch("cluster_scenarios.poll", side_effect=poll_record),
            patch("cluster_scenarios.acknowledge", side_effect=acknowledge_record),
        ):
            result = run_retained_recovery(
                cluster, "retained-events", "payload", MIN_RETAINED_RECOVERY_MESSAGES
            )

        self.assertEqual(
            events,
            [
                "resource_start",
                "latency_start",
                "node_restart",
                "replay",
                "acknowledge",
                "client_close",
                "latency_end",
                "resource_end",
            ],
        )
        self.assertEqual(
            result["metadata"]["latency_scope"],
            "pre_restart_through_earliest_replay_acknowledgement_and_client_close",
        )
        self.assertEqual(
            result["metadata"]["resource_sample_scope"],
            "before_recovery_operation_through_operation_return",
        )
        self.assertEqual(result["resource_samples"], {"window": "recorded"})

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
            patch("cluster_scenarios.preload"),
            patch("cluster_scenarios.measure_scenario", side_effect=run_measurement),
            patch(
                "cluster_scenarios.poll",
                return_value=({"offset": 0, "payload": "corrupt"}, 100),
            ),
            patch("cluster_scenarios.acknowledge") as acknowledge,
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

    def test_retained_hot_path_excludes_preload_and_checks_following_offsets(self) -> None:
        clients = [SimpleNamespace(close=lambda: None) for _ in range(3)]
        cluster = SimpleNamespace(
            node_count=3,
            stats=object(),
            metrics=lambda: None,
            connected_clients=lambda: _ClientsContext(clients),
        )
        retained_messages = MIN_RETAINED_RECOVERY_MESSAGES

        def run_measurement(
            _stats: object,
            operation: str,
            message_size: int,
            action: object,
            *,
            metadata: dict[str, object],
            metrics: object,
        ) -> dict[str, object]:
            self.assertEqual(operation, "cluster_retained_hot_path")
            self.assertEqual(message_size, len("payload"))
            self.assertIs(metrics, cluster.metrics)
            return metric(
                operation,
                action(),  # type: ignore[operator]
                2_000_000,
                message_size=message_size,
                metadata=metadata,
            )

        with (
            patch("cluster_scenarios.preload") as preload,
            patch("cluster_scenarios.publish_messages", return_value=[100, 200]) as publish_messages,
            patch("cluster_scenarios.measure_message_batch", side_effect=run_measurement),
        ):
            result = run_retained_hot_path(
                cluster,
                "retained-events",
                "payload",
                messages=2,
                retained_messages=retained_messages,
            )

        preload.assert_called_once_with(
            cluster, "retained-events", "payload", retained_messages
        )
        publish_args, publish_kwargs = publish_messages.call_args
        self.assertEqual(publish_args[1:4], ("retained-events", "payload", 2))
        self.assertEqual(publish_kwargs["expected_offset"], retained_messages)
        self.assertEqual(result["operation"], "cluster_retained_hot_path")
        self.assertEqual(result["messages"], 2)
        self.assertEqual(result["metadata"]["retained_messages"], retained_messages)
        self.assertTrue(result["metadata"]["publish_setup_excluded"])

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
