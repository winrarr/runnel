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
) -> None:
    if not isinstance(value, (int, float)):
        return
    source = run.get("source", {})
    points.append(
        {
            "timestamp": run.get("generated_at"),
            "timestamp_ms": parse_timestamp(run.get("generated_at", "")) * 1000,
            "run_file": run.get("_path"),
            "profile": source.get("profile", "local"),
            "revision": source.get("revision", "unknown"),
            "run_url": source.get("run_url"),
            "backend": backend,
            "operation": operation,
            "message_size_bytes": size,
            "metric": metric,
            "value": float(value),
            "unit": unit,
        }
    )


def build_points(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    units = {name: unit for name, _, unit, _ in METRIC_DEFINITIONS}
    for run in runs:
        for backend_name, backend in run.get("backends", {}).items():
            for scenario in backend.get("scenarios", []):
                operation = str(scenario.get("operation", "unknown"))
                size_value = scenario.get("message_size_bytes")
                size = int(size_value) if isinstance(size_value, (int, float)) else None
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
                        )

                cpu_seconds = resources.get("cpu_seconds")
                messages = scenario.get("messages")
                if (
                    isinstance(cpu_seconds, (int, float))
                    and cpu_seconds > 0
                    and isinstance(messages, (int, float))
                    and messages > 0
                    and isinstance(throughput, (int, float))
                ):
                    add_point(
                        points,
                        run=run,
                        backend=backend_name,
                        operation=operation,
                        size=size,
                        metric="cpu_efficiency_messages_per_cpu_second",
                        value=float(messages) / float(cpu_seconds),
                        unit=units["cpu_efficiency_messages_per_cpu_second"],
                    )

            resources = backend.get("resource_samples", {})
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
                "backends": sorted(run.get("backends", {}).keys()),
                "resource_limits": run.get("resource_limits", {}),
                "workload": run.get("workload", {}),
            }
        )
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "runs": public_runs,
        "points": build_points(runs),
    }


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
