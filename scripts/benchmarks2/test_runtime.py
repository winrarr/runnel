import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from runtime import DockerContainer  # noqa: E402


class DockerRuntimeTests(unittest.TestCase):
    def test_run_command_contains_shared_limits_mount_and_protocol_ports(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            container = DockerContainer(
                name="benchmark-node",
                image="runnel:test",
                network="benchmark-network",
                cpus="1.5",
                memory="2g",
                data_dir=Path(directory),
                data_target="/var/lib/runnel",
                command=["--listen", "0.0.0.0:4222"],
                environment={"RUST_LOG": "info"},
                entrypoint="/usr/local/bin/runnel",
                published_ports=(4222, 8080),
            )

            command = container.run_command()

        self.assertEqual(command[:2], ["docker", "run"])
        self.assertIn("--detach", command)
        self.assertIn("--label", command)
        self.assertIn("runnel.benchmark=true", command)
        self.assertIn("--cpus", command)
        self.assertIn("1.5", command)
        self.assertIn("--memory", command)
        self.assertIn("2g", command)
        self.assertIn("--publish", command)
        self.assertIn("127.0.0.1::4222", command)
        self.assertIn("127.0.0.1::8080", command)
        self.assertIn("--volume", command)
        self.assertIn("--entrypoint", command)
        self.assertIn("RUST_LOG=info", command)
        self.assertEqual(command[-2:], ["--listen", "0.0.0.0:4222"])


if __name__ == "__main__":
    unittest.main()
