import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import compare  # noqa: E402
import common  # noqa: E402


class ComparisonBenchmarkTests(unittest.TestCase):
    def test_scenario_operation_reads_current_and_legacy_result_fields(self) -> None:
        self.assertEqual(compare.scenario_operation({"operation": "publish"}), "publish")
        self.assertEqual(compare.scenario_operation({"name": "durable_publish"}), "durable_publish")
        with self.assertRaises(compare.ComparisonError):
            compare.scenario_operation({"messages": 10})

    def test_runnel_adapter_accepts_current_scenario_results(self) -> None:
        def run(command: list[str], **_: object) -> SimpleNamespace:
            output = Path(command[command.index("--output") + 1])
            output.write_text(
                json.dumps(
                    {
                        "container": {
                            "image": "runnel:test",
                            "image_id": "sha256:test",
                            "startup_seconds": 0.1,
                            "resource_samples": {},
                        },
                        "scenarios": [
                            {
                                "operation": "durable_publish",
                                "messages": 1,
                                "message_size_bytes": 100,
                                "throughput_messages_per_second": 1,
                                "latency_microseconds": {},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(compare.subprocess, "run", side_effect=run):
            result = compare.run_runnel(
                image="runnel:test",
                cpus="2",
                memory="2g",
                messages=1,
                sizes=[100],
            )

        self.assertEqual(result["scenarios"][0]["operation"], "publish")

    def test_source_metadata_uses_ci_identity_when_available(self) -> None:
        with (
            patch("common.git_revision", return_value="abc123"),
            patch.dict(
                compare.os.environ,
                {
                    "GITHUB_REPOSITORY": "example/runnel",
                    "GITHUB_RUN_ID": "42",
                    "GITHUB_SERVER_URL": "https://github.example",
                    "GITHUB_REF_NAME": "main",
                    "GITHUB_EVENT_NAME": "schedule",
                    "GITHUB_WORKFLOW": "Competitor benchmark history",
                    "BENCHMARK_PROFILE": "scheduled",
                },
                clear=True,
            ),
        ):
            metadata = common.source_metadata(full_revision=True)

        self.assertEqual(metadata["repository"], "example/runnel")
        self.assertEqual(metadata["revision"], "abc123")
        self.assertEqual(metadata["ref"], "main")
        self.assertEqual(metadata["run_id"], "42")
        self.assertEqual(
            metadata["run_url"], "https://github.example/example/runnel/actions/runs/42"
        )
        self.assertEqual(metadata["profile"], "scheduled")

    def test_backend_metadata_records_the_measurement_client_image(self) -> None:
        self.assertEqual(
            compare.backend_metadata("kafka", 1)["client_image"], compare.KAFKA_IMAGE
        )
        self.assertEqual(
            compare.backend_metadata("nats", 3)["client_image"], compare.NATS_BOX_IMAGE
        )
        self.assertEqual(
            compare.backend_metadata("runnel", 1)["client_image"], "host Python runtime"
        )

    def _complete_backend_record(self, name: str, nodes: int = 1) -> dict:
        record = compare.backend_metadata(name, nodes)
        operations = {
            "publish-only": "publish",
            "consume-with-ack": "consume_ack",
            "consume-without-ack": "consume",
        }
        record["scenarios"] = [
            {"operation": operations[comparison_class]}
            for comparison_class in record["semantic_metadata"]["scenario_classes"]
        ]
        compare.annotate_scenario_metadata(record)
        return record

    def test_backend_metadata_declares_semantic_boundaries_and_native_baseline(self) -> None:
        metadata = compare.backend_metadata("kafka", 1)
        semantic = metadata["semantic_metadata"]

        self.assertEqual(
            semantic["acknowledgement_boundary"], metadata["acknowledgement"]
        )
        self.assertEqual(semantic["replication_topology"], metadata["replication"])
        self.assertEqual(
            semantic["measurement_boundary"], metadata["measurement_boundary"]
        )
        self.assertEqual(
            semantic["client_identity"],
            {"name": metadata["measurement_client"], "image": compare.KAFKA_IMAGE},
        )
        self.assertEqual(
            semantic["scenario_classes"], ["publish-only", "consume-without-ack"]
        )
        self.assertFalse(semantic["comparison"]["apples_to_apples"])
        self.assertFalse(semantic["comparison"]["ranking_eligible"])
        self.assertTrue(semantic["comparison"]["experimental"])
        self.assertEqual(
            semantic["comparison"]["mismatch_dimensions"],
            list(compare.COMPARISON_MISMATCH_DIMENSIONS),
        )

    def test_backend_metadata_records_each_scenario_semantic_boundary(self) -> None:
        for name, nodes in (
            ("runnel", 1),
            ("kafka", 1),
            ("redpanda", 1),
            ("nats", 1),
            ("kafka", 3),
            ("redpanda", 3),
            ("nats", 3),
        ):
            with self.subTest(name=name, nodes=nodes):
                semantic = compare.backend_metadata(name, nodes)["semantic_metadata"]
                self.assertEqual(
                    set(semantic["scenario_boundaries"]),
                    set(semantic["scenario_classes"]),
                )
                for boundaries in semantic["scenario_boundaries"].values():
                    self.assertEqual(set(boundaries), set(compare.SCENARIO_BOUNDARY_FIELDS))
                    self.assertTrue(
                        all(isinstance(value, str) and value for value in boundaries.values())
                    )

                record = self._complete_backend_record(name, nodes)
                for scenario in record["scenarios"]:
                    comparison_class = compare.scenario_comparison_class(
                        scenario["operation"]
                    )
                    self.assertEqual(
                        scenario["metadata"]["semantic_boundaries"],
                        semantic["scenario_boundaries"][comparison_class],
                    )

    def test_semantic_validation_accepts_complete_backend_records(self) -> None:
        cases = (
            ("runnel", 1),
            ("kafka", 1),
            ("redpanda", 1),
            ("nats", 1),
            ("kafka", 3),
            ("redpanda", 3),
            ("nats", 3),
        )
        for name, nodes in cases:
            with self.subTest(name=name, nodes=nodes):
                record = self._complete_backend_record(name, nodes)
                compare.validate_backend_record(name, record)

    def test_semantic_validation_rejects_incomplete_backend_metadata(self) -> None:
        record = self._complete_backend_record("runnel")
        del record["semantic_metadata"]["client_identity"]

        with self.assertRaisesRegex(compare.ComparisonError, "client_identity"):
            compare.validate_backend_record("runnel", record)

    def test_semantic_validation_rejects_missing_scenario_boundary(self) -> None:
        record = self._complete_backend_record("runnel")
        del record["semantic_metadata"]["scenario_boundaries"]["publish-only"][
            "latency_boundary"
        ]

        with self.assertRaisesRegex(compare.ComparisonError, "latency_boundary"):
            compare.validate_backend_record("runnel", record)

    def test_semantic_validation_rejects_scenario_replication_mismatch(self) -> None:
        record = self._complete_backend_record("kafka", 3)
        record["semantic_metadata"]["scenario_boundaries"]["publish-only"][
            "replication_topology"
        ] = "single broker"

        with self.assertRaisesRegex(compare.ComparisonError, "replication topology"):
            compare.validate_backend_record("kafka", record)

    def test_semantic_validation_rejects_mismatched_scenario_class(self) -> None:
        record = self._complete_backend_record("runnel")
        record["scenarios"][0]["metadata"]["comparison_class"] = "consume-with-ack"

        with self.assertRaisesRegex(compare.ComparisonError, "comparison class"):
            compare.validate_backend_record("runnel", record)

    def test_summary_guardrail_explicitly_disallows_native_ranking(self) -> None:
        summary = {
            "workload": {"nodes": 1},
            "comparison_guardrail": compare.comparison_guardrail_metadata(1),
            "backends": {"runnel": self._complete_backend_record("runnel")},
        }

        compare.validate_comparison_summary(summary)
        self.assertFalse(summary["comparison_guardrail"]["apples_to_apples"])
        self.assertFalse(summary["comparison_guardrail"]["ranking_eligible"])
        self.assertTrue(summary["comparison_guardrail"]["experimental"])
        self.assertEqual(
            summary["comparison_guardrail"]["mismatch_dimensions"],
            list(compare.COMPARISON_MISMATCH_DIMENSIONS),
        )

    def test_comparison_suite_distinguishes_single_and_three_node_runs(self) -> None:
        self.assertEqual(compare.benchmark_suite(1, ["runnel"]), "runnel")
        self.assertEqual(compare.benchmark_suite(1, ["kafka", "redpanda", "nats"]), "native-comparison")
        self.assertEqual(compare.benchmark_suite(3, ["kafka", "redpanda", "nats"]), "cluster-comparison")

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
