import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from cluster import metric, percentile, poll_until_redelivered, process_stats  # noqa: E402
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
