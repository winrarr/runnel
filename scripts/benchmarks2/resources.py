#!/usr/bin/env python3
"""Collect resource measurements for a Docker benchmark scenario."""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from typing import Any


DEFAULT_PROBE_TIMEOUT_SECONDS = 2.0


def parse_size(value: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]?i?B)\s*", value)
    if match is None:
        return 0.0
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return float(match.group(1)) * units[match.group(2)]


def read_stats(
    container: str,
    *,
    timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> dict[str, float] | None:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        raw = json.loads(result.stdout)
        return {
            "cpu_percent": float(raw["CPUPerc"].rstrip("%")),
            "memory_bytes": parse_size(raw["MemUsage"].split(" / ", 1)[0]),
            "memory_percent": float(raw["MemPerc"].rstrip("%")),
        }
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def parse_cpu_stat(output: str) -> float | None:
    """Parse cgroup v1 or v2 CPU usage and return CPU-seconds."""
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        key, value = fields
        try:
            amount = float(value)
        except ValueError:
            continue
        if key == "usage_usec":
            return amount / 1_000_000
        if key == "cpuacct.usage":
            return amount / 1_000_000_000
    return None


def read_cpu_seconds(
    container: str,
    *,
    timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
) -> float | None:
    """Read cumulative CPU time from the container's cgroup."""
    for path in ("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpuacct/cpuacct.usage"):
        try:
            result = subprocess.run(
                ["docker", "exec", container, "cat", path],
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0:
            usage = parse_cpu_stat(result.stdout)
            if usage is not None:
                return usage
    return None


def summarize_stats(
    samples: list[dict[str, float]],
    *,
    cpu_seconds: float | None = None,
    elapsed_seconds: float | None = None,
) -> dict[str, Any]:
    """Summarize resource samples and an optional exact cgroup CPU interval."""
    summary: dict[str, Any] = {"samples": len(samples)}
    if samples:
        for field in ("cpu_percent", "memory_bytes", "memory_percent"):
            values = [sample.get(field) for sample in samples]
            if not all(isinstance(value, (int, float)) for value in values):
                continue
            summary[f"{field}_avg"] = sum(values) / len(values)
            summary[f"{field}_max"] = max(values)
    if cpu_seconds is not None:
        summary["cpu_seconds"] = max(0.0, cpu_seconds)
    if elapsed_seconds is not None:
        summary["elapsed_seconds"] = max(0.0, elapsed_seconds)
    return summary


class PeriodicSampler:
    """Collect samples in a background thread and summarize them safely."""

    def __init__(self, name: str, *, interval_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, float]] = []
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop_event.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=2)

    def summary(self) -> dict[str, Any]:
        with self.lock:
            samples = list(self.samples)
        return summarize_stats(samples)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self._record()
            self.stop_event.wait(self.interval_seconds)

    def _record(self) -> None:
        raise NotImplementedError


class StatsSampler(PeriodicSampler):
    """Capture coarse memory samples and scenario-scoped cgroup CPU intervals."""

    def __init__(
        self,
        container: str,
        *,
        probe_timeout_seconds: float = DEFAULT_PROBE_TIMEOUT_SECONDS,
        interval_seconds: float = 0.25,
    ) -> None:
        super().__init__(f"docker-stats-{container}", interval_seconds=interval_seconds)
        self.container = container
        self.probe_timeout_seconds = probe_timeout_seconds

    def begin(self) -> tuple[int, float | None, int]:
        """Begin a scenario interval after discarding the boundary sample."""
        self._record()
        with self.lock:
            sample_index = len(self.samples)
        return (
            sample_index,
            read_cpu_seconds(self.container, timeout_seconds=self.probe_timeout_seconds),
            time.perf_counter_ns(),
        )

    def end(self, token: tuple[int, float | None, int]) -> dict[str, Any]:
        """End a scenario interval and return its resource summary."""
        sample_index, cpu_start, started_ns = token
        ended_ns = time.perf_counter_ns()
        cpu_end = read_cpu_seconds(
            self.container, timeout_seconds=self.probe_timeout_seconds
        )
        self._record()
        with self.lock:
            samples = list(self.samples[sample_index:])
        cpu_seconds = None
        if cpu_start is not None and cpu_end is not None:
            cpu_seconds = cpu_end - cpu_start
        return summarize_stats(
            samples,
            cpu_seconds=cpu_seconds,
            elapsed_seconds=(ended_ns - started_ns) / 1_000_000_000,
        )

    def _record(self) -> None:
        sample = read_stats(self.container, timeout_seconds=self.probe_timeout_seconds)
        if sample is None:
            return
        with self.lock:
            self.samples.append(sample)
