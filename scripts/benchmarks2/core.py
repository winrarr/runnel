"""Shared measurement, execution, and result functions."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from statistics import median
from typing import Any, Callable, Mapping, Sequence

from .api import (
    ActionResult,
    Backend,
    Limits,
    Measurement,
    ResourceSample,
    RunResult,
    ScenarioResult,
    Workload,
)


def percentile(values: Sequence[int], percentage: float) -> float:
    if not 0 <= percentage <= 100:
        raise ValueError("percentage must be between 0 and 100")
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def measure(
    action: Callable[[], ActionResult],
    *,
    sample: ResourceSample | None = None,
) -> Measurement:
    started = time.perf_counter_ns()
    result = action()
    elapsed_ns = time.perf_counter_ns() - started
    if not result.latencies_ns:
        raise ValueError("scenario produced no measured messages")
    if any(latency < 0 for latency in result.latencies_ns):
        raise ValueError("scenario produced a negative latency")
    resources = dict(sample()) if sample else {}
    return Measurement(elapsed_ns, result, resources)


def summarize(result: ScenarioResult) -> dict[str, Any]:
    measurement = result.measurement
    latencies = measurement.action.latencies_ns
    elapsed_seconds = measurement.elapsed_ns / 1_000_000_000
    messages = len(latencies)
    summary: dict[str, Any] = {
        "operation": result.operation,
        "messages": messages,
        "message_size_bytes": result.payload_size,
        "elapsed_seconds": elapsed_seconds,
        "throughput_messages_per_second": messages / elapsed_seconds,
        "latency_microseconds": {
            "p50": percentile(latencies, 50) / 1_000,
            "p99": percentile(latencies, 99) / 1_000,
            "p999": percentile(latencies, 99.9) / 1_000,
            "max": max(latencies) / 1_000,
        },
    }
    if measurement.action.metadata:
        summary["metadata"] = dict(measurement.action.metadata)
    if measurement.resources:
        summary["resource_samples"] = dict(measurement.resources)
    return summary


def run_suite(
    backend: Backend,
    workload: Workload,
    *,
    suite: str = "runnel",
    limits: Limits = Limits(),
    nodes: int = 1,
    selected: Sequence[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    output: Path | None = None,
) -> RunResult:
    if nodes <= 0:
        raise ValueError("nodes must be positive")
    scenarios = backend.scenarios()
    names = tuple(scenarios if selected is None else selected)
    if not names:
        raise ValueError("at least one scenario is required")
    unknown = [name for name in names if name not in scenarios]
    if unknown:
        raise ValueError(f"unknown scenario(s): {', '.join(unknown)}")
    if len(set(names)) != len(names):
        raise ValueError("selected scenarios must be unique")

    runtime = backend.runtime(limits, nodes)
    runtime.start()
    measured: list[ScenarioResult] = []
    try:
        client_factory = backend.client_factory(runtime)
        for payload_size in workload.payload_sizes:
            payload = b"x" * payload_size
            for name in names:
                action = lambda name=name, payload=payload: scenarios[name](
                    runtime, client_factory, workload, payload
                )
                measured.append(
                    ScenarioResult(
                        name,
                        payload_size,
                        measure(action, sample=runtime.sample),
                    )
                )
    finally:
        runtime.stop()

    result = RunResult(
        suite=suite,
        backend=backend.name,
        workload=workload,
        scenarios=tuple(measured),
        metadata={**(metadata or {}), "nodes": nodes, "limits": asdict(limits)},
    )
    if output:
        write_result(result, output)
    return result


def _workload_dict(workload: Workload) -> dict[str, Any]:
    return asdict(workload)


def _scenario_key(scenario: ScenarioResult) -> tuple[str, int]:
    return scenario.operation, scenario.payload_size


def _run_dict(result: RunResult) -> dict[str, Any]:
    return {
        "suite": result.suite,
        "backend": result.backend,
        "workload": _workload_dict(result.workload),
        "metadata": dict(result.metadata),
        "scenarios": [summarize(scenario) for scenario in result.scenarios],
    }


def aggregate(results: Sequence[RunResult]) -> RunResult:
    if not results:
        raise ValueError("at least one result is required")
    first = results[0]
    if any(
        (result.suite, result.backend, result.workload)
        != (first.suite, first.backend, first.workload)
        for result in results[1:]
    ):
        raise ValueError("results must use the same suite, backend, and workload")
    expected_keys = {_scenario_key(scenario) for scenario in first.scenarios}
    if any(
        {_scenario_key(scenario) for scenario in result.scenarios} != expected_keys
        for result in results[1:]
    ):
        raise ValueError("results must contain the same scenarios")

    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for result in results:
        for scenario in result.scenarios:
            grouped.setdefault(_scenario_key(scenario), []).append(summarize(scenario))

    aggregates: dict[str, Any] = {}
    for key, observations in grouped.items():
        aggregate_key = f"{key[0]}:{key[1]}"
        aggregates[aggregate_key] = {
            "count": len(observations),
            "throughput_messages_per_second": median(
                observation["throughput_messages_per_second"] for observation in observations
            ),
            "latency_microseconds": {
                percentile_name: median(
                    observation["latency_microseconds"][percentile_name]
                    for observation in observations
                )
                for percentile_name in ("p50", "p99", "p999", "max")
            },
            "observations": observations,
        }

    return RunResult(
        suite=first.suite,
        backend=first.backend,
        workload=first.workload,
        scenarios=first.scenarios,
        metadata={**first.metadata, "aggregate": aggregates},
    )


def _metric(scenario: ScenarioResult, name: str) -> float:
    value: Any = summarize(scenario)
    for part in name.split("."):
        value = value[part]
    return float(value)


def compare(current: RunResult, baseline: RunResult) -> dict[str, Any]:
    if (current.suite, current.backend, current.workload) != (
        baseline.suite,
        baseline.backend,
        baseline.workload,
    ):
        raise ValueError("current and baseline must describe the same benchmark")
    baseline_scenarios = {_scenario_key(scenario): scenario for scenario in baseline.scenarios}
    comparison: dict[str, Any] = {}
    for scenario in current.scenarios:
        key = _scenario_key(scenario)
        previous = baseline_scenarios.get(key)
        if previous is None:
            continue
        values: dict[str, Any] = {}
        for metric_name in (
            "throughput_messages_per_second",
            "latency_microseconds.p99",
        ):
            before = _metric(previous, metric_name)
            after = _metric(scenario, metric_name)
            values[metric_name] = {
                "baseline": before,
                "current": after,
                "delta_percent": ((after - before) / before * 100) if before else None,
            }
        comparison[f"{key[0]}:{key[1]}"] = values
    return comparison


def write_result(result: RunResult, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(_run_dict(result), indent=2) + "\n", encoding="utf-8")
