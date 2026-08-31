import sys
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from resource_scope import ResourceScopeError, resource_limits, resource_scope_command  # noqa: E402


class ResourceScopeTests(unittest.TestCase):
    def test_builds_explicit_cpu_and_memory_scope(self) -> None:
        command = resource_scope_command(
            ["python3", "scripts/benchmarks2/cluster.py"],
            unit="runnel-benchmark-test-1",
            cpus="2",
            memory="2g",
        )

        self.assertEqual(command[:4], ["systemd-run", "--user", "--scope", "--collect"])
        self.assertIn("--property=CPUQuota=200%", command)
        self.assertIn("--property=MemoryMax=2G", command)
        self.assertEqual(command[-2:], ["python3", "scripts/benchmarks2/cluster.py"])

    def test_records_scope_provenance(self) -> None:
        self.assertEqual(
            resource_limits(cpus="1.5", memory="512m"),
            {
                "processes": "systemd user scope; benchmark client and broker nodes",
                "cpu": "1.5",
                "memory": "512M",
            },
        )

    def test_rejects_invalid_limits(self) -> None:
        with self.assertRaises(ResourceScopeError):
            resource_scope_command(["true"], unit="bad unit", cpus="2", memory="2g")
        with self.assertRaises(ResourceScopeError):
            resource_scope_command(["true"], unit="valid", cpus="0", memory="2g")


if __name__ == "__main__":
    unittest.main()
