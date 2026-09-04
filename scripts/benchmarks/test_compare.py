import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import compare  # noqa: E402
import compare_adapters  # noqa: E402
import compare_backends  # noqa: E402
import compare_cli  # noqa: E402
import compare_lifecycle  # noqa: E402
import compare_results  # noqa: E402
import common  # noqa: E402


class ComparisonBenchmarkTests(unittest.TestCase):
    def test_entrypoint_facade_keeps_focused_module_ownership(self) -> None:
        self.assertIs(compare.combine_resource_summaries, compare_lifecycle.combine_resource_summaries)
        self.assertIs(compare.backend_metadata, compare_results.backend_metadata)
        self.assertIs(compare.run_kafka_family, compare_backends.run_kafka_family)
        self.assertIs(compare.parse_args, compare_cli.parse_args)

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
        record["cpu_limit"] = "2"
        record["memory_limit"] = "2g"
        if nodes == compare.THREE_NODE_COUNT:
            record["nodes"] = [
                {"cpu_limit": "2", "memory_limit": "2g"} for _ in range(nodes)
            ]
        compare.annotate_scenario_metadata(record)
        return record

    def _complete_summary(self, nodes: int = 1) -> dict:
        backend_names = ("runnel",) if nodes == 1 else ("kafka",)
        return {
            "workload": {"nodes": nodes},
            "resource_limits": {
                "broker_cpu": "2",
                "broker_memory": "2g",
                "client_cpu": "1",
                "client_memory": "512m",
            },
            "comparison_guardrail": compare.comparison_guardrail_metadata(nodes),
            "backends": {
                name: self._complete_backend_record(name, nodes) for name in backend_names
            },
        }

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

    def test_semantic_validation_rejects_missing_backend_resource_limit(self) -> None:
        record = self._complete_backend_record("runnel")
        del record["cpu_limit"]

        with self.assertRaisesRegex(compare.ComparisonError, "cpu_limit"):
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

    def test_semantic_validation_rejects_mismatched_acknowledgement_or_durability(self) -> None:
        cases = (
            (
                "acknowledgement",
                "acknowledgement_boundary",
                "inconsistent acknowledgement_boundary",
            ),
            ("durability", "durability_boundary", "semantic metadata"),
        )
        for name, field, message in cases:
            with self.subTest(name=name):
                record = self._complete_backend_record("runnel")
                if name == "acknowledgement":
                    record["semantic_metadata"][field] = "mismatched acknowledgement"
                else:
                    record["semantic_metadata"]["scenario_boundaries"]["publish-only"][
                        field
                    ] = "mismatched durability"

                with self.assertRaisesRegex(compare.ComparisonError, message):
                    compare.validate_backend_record("runnel", record)

    def test_semantic_validation_rejects_mismatched_scenario_class(self) -> None:
        record = self._complete_backend_record("runnel")
        record["scenarios"][0]["metadata"]["comparison_class"] = "consume-with-ack"

        with self.assertRaisesRegex(compare.ComparisonError, "comparison class"):
            compare.validate_backend_record("runnel", record)

    def test_summary_guardrail_explicitly_disallows_native_ranking(self) -> None:
        summary = self._complete_summary()

        compare.validate_comparison_summary(summary)
        self.assertFalse(summary["comparison_guardrail"]["apples_to_apples"])
        self.assertFalse(summary["comparison_guardrail"]["ranking_eligible"])
        self.assertTrue(summary["comparison_guardrail"]["experimental"])
        self.assertEqual(
            summary["comparison_guardrail"]["mismatch_dimensions"],
            list(compare.COMPARISON_MISMATCH_DIMENSIONS),
        )

    def test_summary_guardrail_rejects_equivalence_or_ranking_claims(self) -> None:
        for field, value in (
            ("apples_to_apples", True),
            ("ranking_eligible", True),
            ("experimental", False),
        ):
            with self.subTest(field=field):
                summary = self._complete_summary()
                summary["comparison_guardrail"][field] = value

                with self.assertRaises(compare.ComparisonError):
                    compare.validate_comparison_summary(summary)

    def test_summary_validation_rejects_backend_resource_mismatch(self) -> None:
        summary = self._complete_summary()
        summary["backends"]["runnel"]["memory_limit"] = "4g"

        with self.assertRaisesRegex(compare.ComparisonError, "memory_limit"):
            compare.validate_comparison_summary(summary)

    def test_summary_validation_rejects_missing_client_resource_limit(self) -> None:
        summary = self._complete_summary()
        del summary["resource_limits"]["client_memory"]

        with self.assertRaisesRegex(compare.ComparisonError, "client_memory"):
            compare.validate_comparison_summary(summary)

    def test_summary_validation_rejects_incomplete_cluster_topology(self) -> None:
        summary = self._complete_summary(compare.THREE_NODE_COUNT)
        del summary["backends"]["kafka"]["nodes"]

        with self.assertRaisesRegex(compare.ComparisonError, "measured node records"):
            compare.validate_comparison_summary(summary)

    def test_summary_validation_rejects_inconsistent_cluster_node_resources(self) -> None:
        summary = self._complete_summary(compare.THREE_NODE_COUNT)
        summary["backends"]["kafka"]["nodes"][1]["cpu_limit"] = "4"

        with self.assertRaisesRegex(compare.ComparisonError, "resource limits"):
            compare.validate_comparison_summary(summary)

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
        environment = compare_adapters.kafka_environment(names[1], 2, names)

        self.assertEqual(environment["KAFKA_NODE_ID"], "2")
        self.assertEqual(
            environment["KAFKA_CONTROLLER_QUORUM_VOTERS"],
            "1@run-kafka-1:9093,2@run-kafka-2:9093,3@run-kafka-3:9093",
        )
        self.assertEqual(environment["KAFKA_MIN_INSYNC_REPLICAS"], "3")
        self.assertEqual(environment["KAFKA_DEFAULT_REPLICATION_FACTOR"], "3")

    def test_redpanda_and_nats_commands_advertise_unique_cluster_members(self) -> None:
        redpanda = compare_adapters.redpanda_command("run-redpanda-2", 2, "run-redpanda-1")
        nats = compare_adapters.nats_server_command(
            "run-nats-2",
            ["run-nats-1", "run-nats-2", "run-nats-3"],
            "run-nats-cluster",
        )

        self.assertIn("--seeds", redpanda)
        self.assertIn("run-redpanda-1:33145", redpanda)
        self.assertIn("nats://run-nats-1:6222,nats://run-nats-3:6222", nats)
        self.assertEqual(nats[nats.index("--cluster_name") + 1], "run-nats-cluster")
        self.assertEqual(
            compare_adapters.nats_server_command("run-nats-1", ["run-nats-1"], "unused"),
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

        compare_adapters.require_redpanda_broker_count(output, 3)
        with self.assertRaises(compare.ComparisonError):
            compare_adapters.require_redpanda_broker_count(
                output.replace("2     redpanda-2", ""), 3
            )

    def test_native_output_adapters_preserve_backend_measurements(self) -> None:
        kafka = compare_adapters.parse_kafka_publish(
            "100 records sent, 2000 records/sec 1 ms avg latency, 2 ms max latency, "
            "1 ms 50th, 2 ms 95th, 3 ms 99th, 4 ms 99.9th",
            100,
            100,
        )
        nats = compare_adapters.parse_nats_publish(
            "stats: 2,000 msgs/sec min: 1us avg: 2us max: 4us "
            "P50: 2us P90: 3us P99: 4us P99.9: 5us",
            100,
            100,
        )

        self.assertEqual(kafka["messages"], 100)
        self.assertEqual(kafka["latency_microseconds"]["p99"], 3000)
        self.assertEqual(nats["throughput_messages_per_second"], 2000)
        self.assertEqual(nats["latency_microseconds"]["p999"], 5)

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
