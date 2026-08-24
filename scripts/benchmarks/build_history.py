#!/usr/bin/env python3
"""Build the generated benchmark history data consumed by the static dashboard."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from normalize import normalize_result


METRIC_DEFINITIONS = [
    ("throughput_messages_per_second", "Throughput", "messages/s", True),
    ("latency_p50", "p50 latency", "µs", False),
    ("latency_p99", "p99 latency", "µs", False),
    ("latency_p999", "p99.9 latency", "µs", False),
    ("cpu_efficiency_messages_per_cpu_second", "CPU efficiency", "messages/CPU-second", True),
    ("cpu_percent_max", "Peak broker CPU", "%", False),
    ("memory_bytes_max", "Peak broker memory", "bytes", False),
]
METRIC_CONFIG = {name: higher_better for name, _, _, higher_better in METRIC_DEFINITIONS}


def parse_timestamp(value: str) -> float:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def load_runs(runs_dir: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in sorted(runs_dir.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict) or "backends" not in raw:
            continue
        if raw.get("history_schema_version") == 1:
            normalized = raw
        else:
            try:
                normalized = normalize_result(raw, source_name=path.name)
            except RuntimeError:
                continue
        normalized["_path"] = path.name
        runs.append(normalized)
    runs.sort(key=lambda run: parse_timestamp(run.get("generated_at", "")))
    return runs


def add_point(
    points: list[dict[str, Any]],
    *,
    run: dict[str, Any],
    backend: str,
    operation: str,
    size: int | None,
    metric: str,
    value: float,
    unit: str,
    repetitions: int | None = None,
    repetition_summary: dict[str, Any] | None = None,
) -> None:
    if not isinstance(value, (int, float)):
        return
    source = run.get("source", {})
    point = {
        "timestamp": run.get("generated_at"),
        "timestamp_ms": parse_timestamp(run.get("generated_at", "")) * 1000,
        "run_file": run.get("_path"),
        "profile": source.get("profile", "local"),
        "revision": source.get("revision", "unknown"),
        "run_url": source.get("run_url"),
        "comparison_mode": run.get("comparison_mode", "unknown"),
        "benchmark_suite": benchmark_suite(run),
        "benchmark_series": benchmark_series(run, backend),
        "backend": backend,
        "operation": operation,
        "message_size_bytes": size,
        "metric": metric,
        "value": float(value),
        "unit": unit,
    }
    if repetitions is not None:
        point["repetitions"] = repetitions
    if repetition_summary:
        metric_summary = repetition_summary.get(metric)
        if isinstance(metric_summary, dict):
            point["range"] = {
                key: metric_summary[key]
                for key in ("min", "median", "max")
                if key in metric_summary
            }
    points.append(point)


def benchmark_suite(run: dict[str, Any]) -> str:
    explicit = run.get("benchmark_suite")
    if isinstance(explicit, str) and explicit:
        return explicit
    if run.get("comparison_mode") == "cluster-baseline":
        return "cluster"
    if run.get("workload", {}).get("single_node") is True:
        return "native-comparison"
    return "other"


def benchmark_series(run: dict[str, Any], backend: str) -> str:
    """Return the user-facing history series for a backend measurement."""
    suite = benchmark_suite(run)
    if backend == "runnel" and suite == "runnel":
        return "runnel"
    if backend == "runnel" and suite == "native-comparison":
        return "runnel-native-comparison"
    return suite


def build_points(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    units = {name: unit for name, _, unit, _ in METRIC_DEFINITIONS}
    for run in runs:
        for backend_name, backend in run.get("backends", {}).items():
            for scenario in backend.get("scenarios", []):
                operation = str(scenario.get("operation", "unknown"))
                size_value = scenario.get("message_size_bytes")
                size = int(size_value) if isinstance(size_value, (int, float)) else None
                repetitions = scenario.get(
                    "repetitions", run.get("aggregate", {}).get("repetitions")
                )
                repetition_summary = scenario.get("repetition_summary", {})
                throughput = scenario.get("throughput_messages_per_second")
                if isinstance(throughput, (int, float)):
                    add_point(
                        points,
                        run=run,
                        backend=backend_name,
                        operation=operation,
                        size=size,
                        metric="throughput_messages_per_second",
                        value=float(throughput),
                        unit=units["throughput_messages_per_second"],
                        repetitions=repetitions,
                        repetition_summary=repetition_summary,
                    )

                for percentile, metric in (
                    ("p50", "latency_p50"),
                    ("p99", "latency_p99"),
                    ("p999", "latency_p999"),
                ):
                    latency = scenario.get("latency_microseconds", {})
                    value = latency.get(percentile) if isinstance(latency, dict) else None
                    if isinstance(value, (int, float)):
                        add_point(
                            points,
                            run=run,
                            backend=backend_name,
                            operation=operation,
                            size=size,
                            metric=metric,
                            value=float(value),
                            unit=units[metric],
                            repetitions=repetitions,
                            repetition_summary=repetition_summary,
                        )

                resources = scenario.get("resource_samples", {})
                if not isinstance(resources, dict):
                    continue
                for metric, key in (
                    ("cpu_percent_max", "cpu_percent_max"),
                    ("memory_bytes_max", "memory_bytes_max"),
                ):
                    value = resources.get(key)
                    if isinstance(value, (int, float)):
                        add_point(
                            points,
                            run=run,
                            backend=backend_name,
                            operation=operation,
                            size=size,
                            metric=metric,
                            value=float(value),
                            unit=units[metric],
                            repetitions=repetitions,
                            repetition_summary=repetition_summary,
                        )

                cpu_seconds = resources.get("cpu_seconds")
                messages = scenario.get("messages")
                efficiency_summary = repetition_summary.get(
                    "cpu_efficiency_messages_per_cpu_second", {}
                )
                efficiency_value = (
                    efficiency_summary.get("median")
                    if isinstance(efficiency_summary, dict)
                    else None
                )
                if not isinstance(messages, (int, float)) or messages <= 0:
                    continue
                if not isinstance(efficiency_value, (int, float)):
                    if not isinstance(cpu_seconds, (int, float)) or cpu_seconds <= 0:
                        continue
                    efficiency_value = float(messages) / float(cpu_seconds)
                add_point(
                    points,
                    run=run,
                    backend=backend_name,
                    operation=operation,
                    size=size,
                    metric="cpu_efficiency_messages_per_cpu_second",
                    value=float(efficiency_value),
                    unit=units["cpu_efficiency_messages_per_cpu_second"],
                    repetitions=repetitions,
                    repetition_summary=repetition_summary,
                )

            resources = backend.get("resource_samples", {})
            backend_repetitions = backend.get(
                "repetitions", run.get("aggregate", {}).get("repetitions")
            )
            backend_summary = backend.get("repetition_summary", {})
            for metric, key in (
                ("cpu_percent_max", "cpu_percent_max"),
                ("memory_bytes_max", "memory_bytes_max"),
            ):
                value = resources.get(key) if isinstance(resources, dict) else None
                if isinstance(value, (int, float)):
                    add_point(
                        points,
                        run=run,
                        backend=backend_name,
                        operation="broker",
                        size=None,
                        metric=metric,
                        value=float(value),
                        unit=units[metric],
                        repetitions=backend_repetitions,
                        repetition_summary=backend_summary,
                    )
    return points


def site_data(runs: list[dict[str, Any]]) -> dict[str, Any]:
    public_runs = []
    for run in runs:
        source = run.get("source", {})
        public_runs.append(
            {
                "timestamp": run.get("generated_at"),
                "profile": source.get("profile", "local"),
                "revision": source.get("revision", "unknown"),
                "repository": source.get("repository"),
                "run_url": source.get("run_url"),
                "event": source.get("event"),
                "workflow": source.get("workflow"),
                "run_file": run.get("_path"),
                "comparison_mode": run.get("comparison_mode"),
                "benchmark_suite": benchmark_suite(run),
                "repetitions": run.get("aggregate", {}).get(
                    "repetitions", source.get("repetitions", 1)
                ),
                "backends": sorted(run.get("backends", {}).keys()),
                "resource_limits": run.get("resource_limits", {}),
                "workload": run.get("workload", {}),
            }
        )
    points = build_points(runs)
    add_comparable_deltas(runs, points)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "runs": public_runs,
        "points": points,
    }


def comparison_identity(run: dict[str, Any], backend_name: str | None = None) -> str:
    return json.dumps(
        {
            "benchmark_suite": benchmark_suite(run),
            "comparison_mode": run.get("comparison_mode"),
            "resource_limits": run.get("resource_limits", {}),
            "workload": run.get("workload", {}),
            "backends": {
                name: {
                    key: backend.get(key)
                    for key in (
                        "image",
                        "acknowledgement",
                        "replication",
                        "measurement_boundary",
                        "measurement_client",
                    )
                }
                for name, backend in run.get("backends", {}).items()
            },
        },
        sort_keys=True,
    )


def add_comparable_deltas(runs: list[dict[str, Any]], points: list[dict[str, Any]]) -> None:
    points_by_run: dict[str | None, list[dict[str, Any]]] = {}
    for point in points:
        points_by_run.setdefault(point.get("run_file"), []).append(point)

    previous: dict[tuple[str, tuple[Any, ...]], dict[str, Any]] = {}
    for run in runs:
        current_points = points_by_run.get(run.get("_path"), [])
        for point in current_points:
            identity = comparison_identity(run, point["backend"])
            point_key = (
                point["backend"],
                point["operation"],
                point["message_size_bytes"],
                point["metric"],
            )
            previous_point = previous.get((identity, point_key))
            old = previous_point
            if old is None:
                previous[(identity, point_key)] = point
                continue
            old_value = old.get("value")
            current_value = point.get("value")
            if not isinstance(old_value, (int, float)) or not isinstance(
                current_value, (int, float)
            ):
                previous[(identity, point_key)] = point
                continue
            point["previous_value"] = old_value
            point["delta"] = current_value - old_value
            if old_value != 0:
                point["delta_percent"] = ((current_value - old_value) / old_value) * 100
            point["improved"] = (
                current_value > old_value
                if METRIC_CONFIG.get(point["metric"], True)
                else current_value < old_value
            )
            previous[(identity, point_key)] = point


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True, help="directory containing benchmark run JSON files")
    parser.add_argument("--output", type=Path, required=True, help="directory for generated history data")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runs = load_runs(args.runs)
    if args.output.exists():
        shutil.rmtree(args.output)
    args.output.mkdir(parents=True, exist_ok=True)
    data = site_data(runs)
    (args.output / "data.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(f"generated {len(runs)} runs and {len(data['points'])} measurements in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
