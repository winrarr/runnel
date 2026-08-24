import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import compare  # noqa: E402


class ComparisonBenchmarkTests(unittest.TestCase):
    def test_comparison_suite_distinguishes_single_and_three_node_runs(self) -> None:
        self.assertEqual(compare.benchmark_suite(1), "native-comparison")
        self.assertEqual(compare.benchmark_suite(3), "cluster-comparison")

    def test_three_node_arguments_select_competitor_only_publish_mode(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["compare.py", "--nodes", "3", "--backends", "kafka,redpanda,nats"],
        ):
            args = compare.parse_args()

        self.assertEqual(args.nodes, 3)
        self.assertEqual(args.backends, ["kafka", "redpanda", "nats"])

    def test_three_node_arguments_reject_runnel(self) -> None:
        with patch.object(sys, "argv", ["compare.py", "--nodes", "3", "--backends", "runnel"]):
            with self.assertRaises(SystemExit):
                compare.parse_args()

    def test_kafka_three_node_environment_has_shared_controller_quorum_and_rf(self) -> None:
        names = ["run-kafka-1", "run-kafka-2", "run-kafka-3"]
        environment = compare.kafka_environment(names[1], 2, names)

        self.assertEqual(environment["KAFKA_NODE_ID"], "2")
        self.assertEqual(
            environment["KAFKA_CONTROLLER_QUORUM_VOTERS"],
            "1@run-kafka-1:9093,2@run-kafka-2:9093,3@run-kafka-3:9093",
        )
        self.assertEqual(environment["KAFKA_MIN_INSYNC_REPLICAS"], "3")
        self.assertEqual(environment["KAFKA_DEFAULT_REPLICATION_FACTOR"], "3")

    def test_redpanda_and_nats_commands_advertise_unique_cluster_members(self) -> None:
        redpanda = compare.redpanda_command("run-redpanda-2", 2, "run-redpanda-1")
        nats = compare.nats_server_command(
            "run-nats-2",
            ["run-nats-1", "run-nats-2", "run-nats-3"],
            "run-nats-cluster",
        )

        self.assertIn("--seeds", redpanda)
        self.assertIn("run-redpanda-1:33145", redpanda)
        self.assertIn("nats://run-nats-1:6222,nats://run-nats-3:6222", nats)
        self.assertEqual(nats[nats.index("--cluster_name") + 1], "run-nats-cluster")
        self.assertEqual(
            compare.nats_server_command("run-nats-1", ["run-nats-1"], "unused"),
            ["-js", "-sd", "/data"],
        )

    def test_redpanda_readiness_requires_all_brokers(self) -> None:
        output = (
            "BROKERS\n"
            + "=" * 7
            + "\nID    HOST          PORT\n"
            "0*    redpanda-0    9092\n"
            "1     redpanda-1    9092\n"
            "2     redpanda-2    9092\n"
        )

        compare.require_redpanda_broker_count(output, 3)
        with self.assertRaises(compare.ComparisonError):
            compare.require_redpanda_broker_count(output.replace("2     redpanda-2", ""), 3)

    def test_cluster_resource_summary_keeps_nodes_and_sums_cpu_and_memory(self) -> None:
        result = compare.combine_resource_summaries(
            [
                {
                    "samples": 2,
                    "cpu_seconds": 1.25,
                    "memory_bytes_avg": 100,
                    "memory_bytes_max": 120,
                    "elapsed_seconds": 0.5,
                },
                {
                    "samples": 3,
                    "cpu_seconds": 2.75,
                    "memory_bytes_avg": 200,
                    "memory_bytes_max": 240,
                    "elapsed_seconds": 0.7,
                },
            ]
        )

        self.assertEqual(result["samples"], 2)
        self.assertEqual(result["cpu_seconds"], 4.0)
        self.assertEqual(result["memory_bytes_avg"], 300)
        self.assertEqual(result["memory_bytes_max"], 360)
        self.assertEqual(result["elapsed_seconds"], 0.7)
        self.assertEqual(len(result["nodes"]), 2)


if __name__ == "__main__":
    unittest.main()
