#!/usr/bin/env python3
"""Aggregate repeated normalized benchmark results using medians.

Every repetition is expected to use the same revision, workload, resource
limits, and benchmark suite. The aggregate keeps the existing normalized shape
and adds repetition summaries so the dashboard can show the observed range.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from statistics import median
from typing import Any


SCALAR_METRICS = (
    "throughput_messages_per_second",
    "elapsed_milliseconds",
    "throughput_megabytes_per_second",
)
LATENCY_METRICS = ("p50", "p99", "p999", "max")


class AggregationError(RuntimeError):
    """Repeated benchmark results cannot be combined safely."""


def numeric_values(values: list[Any]) -> list[float]:
    return [float(value) for value in values if isinstance(value, (int, float))]


def summary(values: list[Any]) -> dict[str, float | int]:
    numbers = numeric_values(values)
    if not numbers:
        return {}
    median_value = float(median(numbers))
    result: dict[str, float | int] = {
        "count": len(numbers),
        "min": min(numbers),
        "median": median_value,
        "max": max(numbers),
    }
    if median_value != 0:
        result["relative_range_percent"] = (
            (max(numbers) - min(numbers)) / abs(median_value) * 100
        )
    return result


def aggregate_mapping(mappings: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in mappings[0]:
        values = [mapping.get(key) for mapping in mappings]
        numbers = numeric_values(values)
        if len(numbers) == len(mappings):
            result[key] = float(median(numbers))
        else:
            result[key] = copy.deepcopy(mappings[0][key])
    return result


def scenario_key(scenario: dict[str, Any]) -> tuple[Any, ...]:
    metadata = scenario.get("metadata", {})
    return (
        scenario.get("operation", "unknown"),
        scenario.get("message_size_bytes"),
        json.dumps(metadata, sort_keys=True),
    )


def scenario_summary(scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for metric in SCALAR_METRICS:
        values = [scenario.get(metric) for scenario in scenarios]
        metric_summary = summary(values)
        if metric_summary:
            result[metric] = metric_summary

    for percentile in LATENCY_METRICS:
        values = [
            scenario.get("latency_microseconds", {}).get(percentile)
            for scenario in scenarios
            if isinstance(scenario.get("latency_microseconds"), dict)
        ]
        metric_summary = summary(values)
        if metric_summary:
            result[f"latency_{percentile}"] = metric_summary

    cpu_efficiency = []
    for scenario in scenarios:
        messages = scenario.get("messages")
        resources = scenario.get("resource_samples", {})
        cpu_seconds = resources.get("cpu_seconds") if isinstance(resources, dict) else None
        if (
            isinstance(messages, (int, float))
            and messages > 0
            and isinstance(cpu_seconds, (int, float))
            and cpu_seconds > 0
        ):
            cpu_efficiency.append(float(messages) / float(cpu_seconds))
    efficiency_summary = summary(cpu_efficiency)
    if efficiency_summary:
        result["cpu_efficiency_messages_per_cpu_second"] = efficiency_summary
    return result


def aggregate_scenarios(scenarios: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for scenario in scenarios:
        grouped.setdefault(scenario_key(scenario), []).append(scenario)

    aggregated: list[dict[str, Any]] = []
    for group in grouped.values():
        result = aggregate_mapping(group)
        if any("latency_microseconds" in scenario for scenario in group):
            latency_values = [
                scenario.get("latency_microseconds", {})
                for scenario in group
                if isinstance(scenario.get("latency_microseconds"), dict)
            ]
            result["latency_microseconds"] = aggregate_mapping(latency_values)
        if any("resource_samples" in scenario for scenario in group):
            resources = [
                scenario.get("resource_samples", {})
                for scenario in group
                if isinstance(scenario.get("resource_samples"), dict)
            ]
            result["resource_samples"] = aggregate_mapping(resources)
        result["repetitions"] = len(group)
        result["repetition_summary"] = scenario_summary(group)
        aggregated.append(result)
    return aggregated


def aggregate_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise AggregationError("at least one normalized result is required")

    reference = results[0]
    for result in results[1:]:
        for key in ("comparison_mode", "benchmark_suite", "resource_limits", "workload"):
            if result.get(key) != reference.get(key):
                raise AggregationError(f"repetitions disagree on {key}")
        if result.get("source", {}).get("revision") != reference.get("source", {}).get("revision"):
            raise AggregationError("repetitions disagree on source revision")
        if result.get("backends", {}).keys() != reference.get("backends", {}).keys():
            raise AggregationError("repetitions disagree on backend set")

    backends: dict[str, Any] = {}
    for backend_name in reference.get("backends", {}):
        backend_results = [result["backends"][backend_name] for result in results]
        expected_scenarios = sorted(scenario_key(scenario) for scenario in backend_results[0].get("scenarios", []))
        for backend_result in backend_results[1:]:
            actual_scenarios = sorted(
                scenario_key(scenario) for scenario in backend_result.get("scenarios", [])
            )
            if actual_scenarios != expected_scenarios:
                raise AggregationError(
                    f"repetitions disagree on scenarios for backend {backend_name!r}"
                )
        backend = copy.deepcopy(backend_results[0])
        backend["scenarios"] = aggregate_scenarios(
            [scenario for result in backend_results for scenario in result.get("scenarios", [])]
        )
        resources = [
            backend_result.get("resource_samples", {})
            for backend_result in backend_results
            if isinstance(backend_result.get("resource_samples"), dict)
        ]
        if resources:
            backend["resource_samples"] = aggregate_mapping(resources)
            backend["repetition_summary"] = {
                key: summary([resource.get(key) for resource in resources])
                for key in resources[0]
                if summary([resource.get(key) for resource in resources])
            }
        backend["repetitions"] = len(results)
        backends[backend_name] = backend

    source = copy.deepcopy(reference.get("source", {}))
    source["repetitions"] = len(results)
    source["repetition_run_ids"] = [
        result.get("source", {}).get("run_id") for result in results
    ]
    generated_at = max(result.get("generated_at", "") for result in results)
    return {
        "history_schema_version": reference.get("history_schema_version", 1),
        "generated_at": generated_at,
        "source": source,
        "environment": copy.deepcopy(reference.get("environment", {})),
        "comparison_mode": reference.get("comparison_mode"),
        "benchmark_suite": reference.get("benchmark_suite", "unknown"),
        "resource_limits": copy.deepcopy(reference.get("resource_limits", {})),
        "workload": copy.deepcopy(reference.get("workload", {})),
        "aggregate": {
            "repetitions": len(results),
            "first_generated_at": min(result.get("generated_at", "") for result in results),
            "last_generated_at": generated_at,
        },
        "backends": backends,
        "source_result": "aggregate",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        results = [json.loads(path.read_text(encoding="utf-8")) for path in args.inputs]
        if any(not isinstance(result, dict) for result in results):
            raise AggregationError("every input must contain a JSON object")
        aggregate = aggregate_results(results)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(aggregate, indent=2) + "\n", encoding="utf-8")
    except (OSError, json.JSONDecodeError, AggregationError) as error:
        raise SystemExit(f"could not aggregate benchmark results: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
