#!/usr/bin/env python3
"""Resource observation for clustered benchmark processes and containers."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Protocol

from resources import (
    DEFAULT_PROBE_TIMEOUT_SECONDS,
    PeriodicSampler,
    directory_size,
    read_cpu_seconds,
    read_stats,
    summarize_stats,
)


class ResourceCluster(Protocol):
    runtime: str
    nodes: list[Any]


def resource_limits(*, runtime: str, cpus: str, memory: str) -> dict[str, str]:
    """Describe the cgroup budget used by a clustered benchmark run."""
    if runtime == "container":
        return {
            "processes": "Docker containers; benchmark client remains host-side",
            "cpu_per_broker": cpus,
            "memory_per_broker": memory,
        }
    native_cpu = os.environ.get("RUNNEL_BENCHMARK_CPU_LIMIT")
    native_memory = os.environ.get("RUNNEL_BENCHMARK_MEMORY_LIMIT")
    if native_cpu and native_memory:
        return {
            "processes": "systemd user scope; benchmark client and broker nodes",
            "cpu": native_cpu,
            "memory": native_memory,
        }
    return {"processes": "host-scheduled; no cgroup limit"}


def process_stats(pid: int) -> tuple[float, int] | None:
    """Return process CPU seconds and resident bytes on Linux."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields = stat[stat.rfind(")") + 2 :].split()
        clock_ticks = os.sysconf("SC_CLK_TCK")
        cpu_seconds = (int(fields[11]) + int(fields[12])) / clock_ticks
        rss = 0
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
                break
        return cpu_seconds, rss
    except (IndexError, OSError, ValueError):
        return None


def parse_owned_peer_tcp_endpoints(
    tables: list[str],
    owned_inodes: set[str],
    *,
    peer_listener_port: int,
    peer_destination_ports: set[int],
) -> int:
    """Count owned established socket endpoints on this node's peer path."""
    matched_inodes: set[str] = set()
    for table in tables:
        for line in table.splitlines()[1:]:
            fields = line.split()
            if len(fields) < 10 or fields[3] != "01":
                continue
            inode = fields[9]
            if inode not in owned_inodes:
                continue
            try:
                local_port = int(fields[1].rsplit(":", 1)[1], 16)
                remote_port = int(fields[2].rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if (
                local_port == peer_listener_port
                or remote_port in peer_destination_ports
            ):
                matched_inodes.add(inode)
    return len(matched_inodes)


def process_peer_tcp_endpoint_count(
    pid: int,
    *,
    peer_listener_port: int,
    peer_destination_ports: set[int],
    proc_root: Path = Path("/proc"),
) -> int | None:
    """Count this broker's established peer TCP sockets from Linux procfs.

    Joining the broker's socket file descriptors to its network namespace's
    TCP tables excludes sockets owned by unrelated processes in the same
    namespace. ``None`` means procfs could not provide a complete observation.
    """
    process_root = proc_root / str(pid)
    try:
        socket_inodes = set()
        for descriptor in (process_root / "fd").iterdir():
            try:
                target = os.readlink(descriptor)
            except FileNotFoundError:
                continue
            match = re.fullmatch(r"socket:\[(\d+)\]", target)
            if match is not None:
                socket_inodes.add(match.group(1))

        tables = [(process_root / "net" / "tcp").read_text(encoding="utf-8")]
        try:
            tables.append(
                (process_root / "net" / "tcp6").read_text(encoding="utf-8")
            )
        except FileNotFoundError:
            pass
    except OSError:
        return None

    return parse_owned_peer_tcp_endpoints(
        tables,
        socket_inodes,
        peer_listener_port=peer_listener_port,
        peer_destination_ports=peer_destination_ports,
    )


def _container_host_pids(nodes: list[Any]) -> dict[str, int]:
    containers = [
        node.container
        for node in nodes
        if node.container is not None and node.container.created
    ]
    if not containers:
        return {}
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.Name}} {{.State.Pid}}",
                *(container.name for container in containers),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if result.returncode != 0:
        return {}

    host_pids: dict[str, int] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        name = fields[0].removeprefix("/")
        try:
            pid = int(fields[1])
        except ValueError:
            continue
        if pid > 0:
            host_pids[name] = pid
    return host_pids


def container_peer_tcp_endpoint_count(
    container_name: str,
    *,
    peer_listener_port: int,
    peer_destination_ports: set[int],
) -> dict[str, Any] | None:
    """Count broker-owned peer endpoints from the container's own procfs.

    Some Linux hosts restrict access to another UID's ``/proc/<host-pid>/fd``.
    Running this read-only collection through Docker exec uses the configured
    container user, which can inspect the broker process in the shared
    container PID namespace without granting host-level ptrace access.
    """
    script = " ".join(
        [
            'exec_uid=$(id -u) || exit 1;',
            "broker_uid=;",
            "while read -r field real effective saved fs; do",
            'if [ "$field" = "Uid:" ]; then broker_uid=$effective; break; fi;',
            "done < /proc/1/status;",
            '[ -n "$broker_uid" ] || exit 1;',
            'printf "__RUNNEL_UIDS__%s %s\\n" "$exec_uid" "$broker_uid";',
            '[ "$exec_uid" = "$broker_uid" ] || exit 2;',
            "for fd in /proc/1/fd/*; do",
            'target=$(readlink "$fd" 2>/dev/null) || continue;',
            'case "$target" in socket:*) printf "SOCKET %s\\n" "$target";; esac;',
            "done;",
            'printf "__RUNNEL_TCP4__\\n";',
            "cat /proc/1/net/tcp || exit 1;",
            'printf "__RUNNEL_TCP6__\\n";',
            "if [ -e /proc/1/net/tcp6 ]; then cat /proc/1/net/tcp6 || exit 1; fi",
        ]
    )
    try:
        result = subprocess.run(
            ["docker", "exec", container_name, "sh", "-c", script],
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None

    _, separator, remainder = result.stdout.partition("__RUNNEL_UIDS__")
    if not separator:
        return None
    identity_line, separator, output = remainder.partition("\n")
    if not separator:
        return None
    uid_match = re.fullmatch(r"(\d+) (\d+)", identity_line)
    if uid_match is None or uid_match.group(1) != uid_match.group(2):
        return None

    socket_lines, separator, tables = output.partition("__RUNNEL_TCP4__\n")
    if not separator:
        return None
    tcp4, separator, tcp6 = tables.partition("__RUNNEL_TCP6__\n")
    if not separator:
        return None
    owned_inodes = {
        match.group(1)
        for line in socket_lines.splitlines()
        if (match := re.fullmatch(r"SOCKET socket:\[(\d+)\]", line))
        is not None
    }
    return {
        "count": parse_owned_peer_tcp_endpoints(
            [tcp4, tcp6],
            owned_inodes,
            peer_listener_port=peer_listener_port,
            peer_destination_ports=peer_destination_ports,
        ),
        "process_identity_verified": True,
    }


def peer_connection_census(cluster: ResourceCluster) -> dict[str, Any]:
    """Observe established peer socket endpoints owned by each broker."""
    started_ns = time.perf_counter_ns()
    result: dict[str, Any] = {
        "method": "linux_procfs_process_fd_inode_join",
        "scope": (
            "established TCP socket endpoints owned by each broker process; "
            "local endpoint matches its peer listener port or remote endpoint "
            "matches a configured peer destination port"
        ),
        "count_semantics": (
            "per-node socket endpoints, not unique TCP connections; one "
            "inter-node connection can appear once at each broker endpoint"
        ),
        "available": False,
        "per_node": {},
    }
    if sys.platform != "linux":
        result["unavailable_reason"] = "linux_procfs_required"
        result["observation_duration_ms"] = (
            time.perf_counter_ns() - started_ns
        ) / 1_000_000
        return result

    all_available = True
    container_pids = (
        _container_host_pids(cluster.nodes)
        if cluster.runtime == "container"
        else {}
    )
    for node in cluster.nodes:
        if cluster.runtime == "container":
            pid = (
                container_pids.get(node.container.name)
                if node.container is not None
                else None
            )
        else:
            process = node.process
            pid = (
                process.pid
                if process is not None and process.poll() is None
                else None
            )
        if pid is None:
            all_available = False
            result["per_node"][str(node.node_id)] = {
                "available": False,
                "unavailable_reason": "broker_process_unavailable",
            }
            continue

        count = process_peer_tcp_endpoint_count(
            pid,
            peer_listener_port=node.peer_port,
            peer_destination_ports={
                other.peer_address_port
                for other in cluster.nodes
                if other.node_id != node.node_id
            },
        )
        observation_source = "host_pid_procfs_fd_inode_join"
        if count is None and cluster.runtime == "container":
            container_count = container_peer_tcp_endpoint_count(
                node.container.name,
                peer_listener_port=node.peer_port,
                peer_destination_ports={
                    other.peer_address_port
                    for other in cluster.nodes
                    if other.node_id != node.node_id
                },
            )
            observation_source = "docker_exec_container_procfs_fd_inode_join"
            if container_count is not None:
                count = container_count["count"]
        if count is None:
            all_available = False
            result["per_node"][str(node.node_id)] = {
                "available": False,
                "unavailable_reason": "procfs_socket_ownership_unavailable",
            }
            continue
        result["per_node"][str(node.node_id)] = {
            "available": True,
            "established_socket_endpoint_count": count,
            "observation_source": observation_source,
            "docker_host_pid_resolved": cluster.runtime == "container",
            "container_process_identity_verified": (
                cluster.runtime == "container"
                and observation_source == "docker_exec_container_procfs_fd_inode_join"
                and container_count is not None
                and container_count["process_identity_verified"]
            ),
        }

    result["available"] = all_available
    result["observation_sources"] = sorted(
        {
            node_result["observation_source"]
            for node_result in result["per_node"].values()
            if node_result.get("available") is True
        }
    )
    result["observation_duration_ms"] = (
        time.perf_counter_ns() - started_ns
    ) / 1_000_000
    return result


class ProcessStats(PeriodicSampler):
    """Sample aggregate broker CPU time and resident memory for each scenario."""

    def __init__(self, cluster: ResourceCluster) -> None:
        super().__init__("runnel-cluster-stats", interval_seconds=0.1)
        self.cluster = cluster
        self.node_samples: list[dict[str, dict[str, float | None]]] = []
        self._last_storage_scan_ns = 0
        self._storage_bytes: dict[int, int] = {}
        self._observe_snapshot_builds = False
        self._snapshot_build_publishes_active = False
        self._snapshot_metrics_observer_duration_ns = 0

    @contextmanager
    def observe_snapshot_builds(self) -> Iterator[None]:
        """Align active-build gauges with the existing per-node RSS samples."""
        with self.lock:
            if self._observe_snapshot_builds:
                raise RuntimeError("snapshot build observation is already enabled")
            self._observe_snapshot_builds = True
        try:
            yield
        finally:
            with self.lock:
                self._snapshot_build_publishes_active = False
                self._observe_snapshot_builds = False

    @contextmanager
    def observe_snapshot_build_publishes(self) -> Iterator[None]:
        """Mark RSS/gauge samples taken while the measured publish loop runs."""
        with self.lock:
            if not self._observe_snapshot_builds:
                raise RuntimeError("snapshot build observation is not enabled")
            if self._snapshot_build_publishes_active:
                raise RuntimeError("snapshot build publish observation is already active")
            self._snapshot_build_publishes_active = True
        try:
            yield
        finally:
            with self.lock:
                self._snapshot_build_publishes_active = False

    def begin(self) -> tuple[int, int, float, int, int]:
        self._record(force_storage=True)
        with self.lock:
            sample_index = len(self.samples)
            node_sample_index = max(0, len(self.node_samples) - 1)
            cpu_start = self.samples[-1]["cpu_seconds"] if self.samples else 0.0
            observer_duration_start = self._snapshot_metrics_observer_duration_ns
        return (
            sample_index,
            node_sample_index,
            cpu_start,
            time.perf_counter_ns(),
            observer_duration_start,
        )

    def end(self, token: tuple[int, int, float, int, int]) -> dict[str, Any]:
        (
            sample_index,
            node_sample_index,
            cpu_start,
            started_ns,
            observer_duration_start,
        ) = token
        ended_ns = time.perf_counter_ns()
        self._record(force_storage=True)
        with self.lock:
            samples = list(self.samples[sample_index:])
            node_samples = list(self.node_samples[node_sample_index:])
            observer_duration_ns = (
                self._snapshot_metrics_observer_duration_ns - observer_duration_start
            )
        cpu_end = samples[-1]["cpu_seconds"] if samples else cpu_start
        result = summarize_stats(
            samples,
            cpu_seconds=cpu_end - cpu_start,
            elapsed_seconds=(ended_ns - started_ns) / 1_000_000_000,
        )
        result["per_node"] = self._summarize_nodes(node_samples)
        if any(
            "snapshot_builds_in_progress" in node_metrics
            for sample in node_samples
            for node_metrics in sample.values()
        ):
            elapsed_ns = max(0, ended_ns - started_ns)
            result["snapshot_build_memory_observations"] = {
                "sampling_interval_milliseconds": self.interval_seconds * 1_000,
                "observer_duration_milliseconds": observer_duration_ns / 1_000_000,
                "observer_duration_fraction_of_sample_window": (
                    observer_duration_ns / elapsed_ns if elapsed_ns else 0.0
                ),
                "per_node": self._summarize_snapshot_build_memory(node_samples),
            }
        return result

    def summary(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.samples)
            node_samples = list(self.node_samples)
        result = summarize_stats(samples)
        result["per_node"] = self._summarize_nodes(node_samples)
        return result

    @staticmethod
    def _summarize_nodes(
        samples: list[dict[str, dict[str, float | None]]],
    ) -> dict[str, dict[str, Any]]:
        node_ids = sorted({node_id for sample in samples for node_id in sample})
        summaries: dict[str, dict[str, Any]] = {}
        for node_id in node_ids:
            values = [sample[node_id] for sample in samples if node_id in sample]
            if not values:
                continue
            summary: dict[str, Any] = {"samples": len(values)}
            memory_values = [
                value["memory_bytes"]
                for value in values
                if isinstance(value.get("memory_bytes"), (int, float))
            ]
            if memory_values:
                summary["memory_bytes_avg"] = sum(memory_values) / len(memory_values)
                summary["memory_bytes_max"] = max(memory_values)
            storage_values = [
                value["storage_bytes"]
                for value in values
                if isinstance(value.get("storage_bytes"), (int, float))
            ]
            if storage_values:
                summary["storage_bytes_avg"] = sum(storage_values) / len(storage_values)
                summary["storage_bytes_max"] = max(storage_values)
            cpu_values = [
                value["cpu_seconds"]
                for value in values
                if isinstance(value.get("cpu_seconds"), (int, float))
            ]
            if cpu_values:
                summary["cpu_seconds"] = max(0.0, cpu_values[-1] - cpu_values[0])
            summaries[node_id] = summary
        return summaries

    @staticmethod
    def _summarize_snapshot_build_memory(
        samples: list[dict[str, dict[str, float | None]]],
    ) -> dict[str, dict[str, Any]]:
        node_ids = sorted(
            {
                node_id
                for sample in samples
                for node_id, value in sample.items()
                if "snapshot_builds_in_progress" in value
            }
        )
        summaries: dict[str, dict[str, Any]] = {}
        for node_id in node_ids:
            values = [
                sample[node_id]
                for sample in samples
                if node_id in sample
                and "snapshot_builds_in_progress" in sample[node_id]
            ]
            observed = [
                value
                for value in values
                if isinstance(value.get("snapshot_builds_in_progress"), (int, float))
            ]
            active = [
                value
                for value in observed
                if value["snapshot_builds_in_progress"] > 0
                and isinstance(value.get("memory_bytes"), (int, float))
            ]
            publish_samples = [
                value
                for value in values
                if value.get("snapshot_build_publishes_active") == 1.0
            ]
            active_publish_samples = [
                value
                for value in publish_samples
                if isinstance(value.get("snapshot_builds_in_progress"), (int, float))
                and value["snapshot_builds_in_progress"] > 0
                and isinstance(value.get("memory_bytes"), (int, float))
            ]
            summaries[node_id] = {
                "samples": len(values),
                "samples_with_observed_build_gauge": len(observed),
                "samples_with_build_in_progress": len(active),
                "samples_during_measured_publishes": len(publish_samples),
                "samples_with_build_in_progress_during_measured_publishes": len(
                    active_publish_samples
                ),
                "rss_bytes_max_during_observed_build": (
                    max(value["memory_bytes"] for value in active)
                    if active
                    else None
                ),
                "rss_bytes_max_during_observed_build_and_publishes": (
                    max(value["memory_bytes"] for value in active_publish_samples)
                    if active_publish_samples
                    else None
                ),
                "rss_scope": (
                    "sampled process RSS while one or more builds were active; "
                    "lower bound, not peak or incremental memory"
                ),
            }
        return summaries

    def _record(self, *, force_storage: bool = False) -> None:
        with self.lock:
            observe_snapshot_builds = self._observe_snapshot_builds
            snapshot_build_publishes_active = self._snapshot_build_publishes_active
        cpu = 0.0
        memory = 0
        node_sample: dict[str, dict[str, float | None]] = {}
        now_ns = time.monotonic_ns()
        scan_storage = force_storage or now_ns - self._last_storage_scan_ns >= 1_000_000_000
        if scan_storage:
            self._last_storage_scan_ns = now_ns
        for node in self.cluster.nodes:
            if scan_storage:
                self._storage_bytes[node.node_id] = directory_size(node.data_dir)
            storage_bytes = self._storage_bytes.get(node.node_id, 0)
            if self.cluster.runtime == "container":
                node_value: dict[str, float | None] = {}
                if node.container is not None and node.container.created:
                    node_cpu = read_cpu_seconds(node.container.name)
                    sample = read_stats(node.container.name)
                    if sample is not None:
                        node_value["memory_bytes"] = float(sample["memory_bytes"])
                        memory += int(sample["memory_bytes"])
                    if node_cpu is not None:
                        node_value["cpu_seconds"] = node_cpu
                        cpu += node_cpu
                if node.data_dir.exists():
                    node_value["storage_bytes"] = float(storage_bytes)
                if node_value:
                    node_sample[str(node.node_id)] = node_value
                continue
            node_value: dict[str, float | None] = {}
            if node.process is not None:
                sample = process_stats(node.process.pid)
                if sample is not None:
                    node_value["cpu_seconds"] = sample[0]
                    node_value["memory_bytes"] = float(sample[1])
                    cpu += sample[0]
                    memory += sample[1]
            if node.data_dir.exists():
                node_value["storage_bytes"] = float(storage_bytes)
            if node_value:
                node_sample[str(node.node_id)] = node_value
        if observe_snapshot_builds:
            observer_started_ns = time.perf_counter_ns()
            snapshot_metrics = self.cluster.metrics()
            observer_duration_ns = time.perf_counter_ns() - observer_started_ns
            with self.lock:
                self._snapshot_metrics_observer_duration_ns += observer_duration_ns
                snapshot_build_publishes_active = (
                    snapshot_build_publishes_active
                    and self._snapshot_build_publishes_active
                )
            for node in self.cluster.nodes:
                sample = node_sample.get(str(node.node_id))
                if sample is None:
                    continue
                metric_name = f"node_{node.node_id}.runnel_snapshot_builds_in_progress"
                sample["snapshot_builds_in_progress"] = (
                    snapshot_metrics.get(metric_name)
                    if snapshot_metrics is not None
                    else None
                )
                sample["snapshot_build_publishes_active"] = float(
                    snapshot_build_publishes_active
                )
        storage = sum(
            int(value.get("storage_bytes", 0)) for value in node_sample.values()
        )
        with self.lock:
            self.samples.append(
                {
                    "cpu_seconds": cpu,
                    "memory_bytes": float(memory),
                    "storage_bytes": float(storage),
                }
            )
            self.node_samples.append(node_sample)
