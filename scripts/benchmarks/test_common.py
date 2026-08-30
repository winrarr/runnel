import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import common  # noqa: E402


class CommonBenchmarkTests(unittest.TestCase):
    def test_publish_messages_checks_contiguous_offsets(self) -> None:
        clients = [object(), object()]
        calls: list[tuple[object, str, str]] = []

        def publish(client: object, stream: str, payload: str) -> tuple[int, int]:
            calls.append((client, stream, payload))
            return len(calls) + 4, 100

        with patch.object(common, "publish", side_effect=publish):
            latencies = common.publish_messages(
                lambda offset: clients[offset % len(clients)],
                "events",
                "payload",
                2,
                expected_offset=5,
        )

        self.assertEqual(latencies, [100, 100])
        self.assertEqual(
            calls,
            [
                (clients[0], "events", "payload"),
                (clients[1], "events", "payload"),
            ],
        )

    def test_publish_messages_rejects_an_unexpected_offset(self) -> None:
        with patch.object(common, "publish", return_value=(8, 100)):
            with self.assertRaises(common.BenchmarkError):
                common.publish_messages(
                    lambda _: object(),
                    "events",
                    "payload",
                    1,
                    expected_offset=0,
                )

    def test_consume_ack_messages_can_route_poll_and_ack_to_different_nodes(self) -> None:
        poll_clients = [object(), object()]
        ack_clients = [object(), object()]
        polls: list[object] = []
        acknowledgements: list[object] = []

        def poll(
            client: object, stream: str, consumer: str, offset: int
        ) -> tuple[dict, int]:
            polls.append(client)
            return {"offset": offset}, 100

        def acknowledge(client: object, stream: str, consumer: str, offset: int) -> int:
            acknowledgements.append(client)
            return 200

        with (
            patch.object(common, "poll", side_effect=poll),
            patch.object(common, "acknowledge", side_effect=acknowledge),
        ):
            latencies = common.consume_ack_messages(
                lambda offset: poll_clients[offset % 2],
                "events",
                "worker",
                2,
                ack_client_for=lambda offset: ack_clients[(offset + 1) % 2],
            )

        self.assertEqual(len(latencies), 2)
        self.assertEqual(polls, poll_clients)
        self.assertEqual(acknowledgements, [ack_clients[1], ack_clients[0]])

    def test_write_json_result_creates_parent_and_prints_artifact_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "result.json"
            with patch("builtins.print") as print_mock:
                common.write_json_result(output, {"messages": 2})

            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")), {"messages": 2}
            )
            self.assertEqual(print_mock.call_count, 2)
            self.assertIn(str(output), print_mock.call_args_list[1].args[0])


if __name__ == "__main__":
    unittest.main()
