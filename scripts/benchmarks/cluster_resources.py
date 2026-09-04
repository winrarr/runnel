#!/usr/bin/env python3
"""Resource observation for clustered benchmark processes and containers."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Protocol

from resources import (
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


class ProcessStats(PeriodicSampler):
    """Sample aggregate broker CPU time and resident memory for each scenario."""

    def __init__(self, cluster: ResourceCluster) -> None:
        super().__init__("runnel-cluster-stats", interval_seconds=0.1)
        self.cluster = cluster
        self.node_samples: list[dict[str, dict[str, float]]] = []
        self._last_storage_scan_ns = 0
        self._storage_bytes: dict[int, int] = {}

    def begin(self) -> tuple[int, int, float, int]:
        self._record(force_storage=True)
        with self.lock:
            sample_index = len(self.samples)
            node_sample_index = max(0, len(self.node_samples) - 1)
            cpu_start = self.samples[-1]["cpu_seconds"] if self.samples else 0.0
        return sample_index, node_sample_index, cpu_start, time.perf_counter_ns()

    def end(self, token: tuple[int, int, float, int]) -> dict[str, Any]:
        sample_index, node_sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        self._record(force_storage=True)
        with self.lock:
            samples = list(self.samples[sample_index:])
            node_samples = list(self.node_samples[node_sample_index:])
        cpu_end = samples[-1]["cpu_seconds"] if samples else cpu_start
        result = summarize_stats(
            samples,
            cpu_seconds=cpu_end - cpu_start,
            elapsed_seconds=(ended_ns - started_ns) / 1_000_000_000,
        )
        result["per_node"] = self._summarize_nodes(node_samples)
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
        samples: list[dict[str, dict[str, float]]],
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

    def _record(self, *, force_storage: bool = False) -> None:
        cpu = 0.0
        memory = 0
        node_sample: dict[str, dict[str, float]] = {}
        now_ns = time.monotonic_ns()
        scan_storage = force_storage or now_ns - self._last_storage_scan_ns >= 1_000_000_000
        if scan_storage:
            self._last_storage_scan_ns = now_ns
        for node in self.cluster.nodes:
            if scan_storage:
                self._storage_bytes[node.node_id] = directory_size(node.data_dir)
            storage_bytes = self._storage_bytes.get(node.node_id, 0)
            if self.cluster.runtime == "container":
                node_value: dict[str, float] = {}
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
            node_value = {}
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
