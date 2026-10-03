import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from runtime import DockerContainer  # noqa: E402
from resources import DEFAULT_PROBE_TIMEOUT_SECONDS  # noqa: E402


class DockerRuntimeTests(unittest.TestCase):
    def test_container_cleanup_permissions_skip_host_owned_mount_root(self) -> None:
        container = DockerContainer(
            name="benchmark-node",
            image="runnel:test",
            network="benchmark-network",
            cpus="1",
            memory="1g",
            data_dir=Path("/tmp/benchmark-node"),
            data_target="/var/lib/runnel",
            created=True,
        )
        with patch(
            "runtime.subprocess.run",
            return_value=SimpleNamespace(returncode=0),
        ) as docker_exec:
            prepared = container.prepare_data_for_host_cleanup()

        self.assertTrue(prepared)
        command = docker_exec.call_args.args[0]
        self.assertEqual(command[:5], ["docker", "exec", "benchmark-node", "sh", "-c"])
        self.assertEqual(
            command[5],
            "find /var/lib/runnel -mindepth 1 -exec chmod a+rwX {} +",
        )
        self.assertNotIn("chmod -R", command[5])
        self.assertEqual(
            docker_exec.call_args.kwargs["timeout"],
            DEFAULT_PROBE_TIMEOUT_SECONDS,
        )

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
