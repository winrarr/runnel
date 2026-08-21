#!/usr/bin/env python3
"""Normalize a comparison result for durable benchmark history.

The comparison runner keeps native tool output for local investigation. History
only needs stable measurements and provenance, so this script deliberately
removes raw logs before a result is appended to the generated history branch.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


class NormalizationError(RuntimeError):
    """The comparison result does not have the expected shape."""


def git_revision() -> str:
    revision = os.environ.get("GITHUB_SHA")
    if revision:
        return revision
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() or "unknown"


def docker_server_version() -> str | None:
    result = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    version = result.stdout.strip()
    return version or None


def source_metadata() -> dict[str, Any]:
    repository = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server_url}/{repository}/actions/runs/{run_id}" if repository and run_id else None
    return {
        "repository": repository or "local",
        "revision": git_revision(),
        "ref": os.environ.get("GITHUB_REF_NAME") or os.environ.get("GITHUB_REF", "local"),
        "event": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "workflow": os.environ.get("GITHUB_WORKFLOW", "local"),
        "run_id": run_id,
        "run_url": run_url,
        "profile": os.environ.get("BENCHMARK_PROFILE", "local"),
    }


def environment_metadata() -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "host": platform.node() or "unknown",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    docker_version = docker_server_version()
    if docker_version:
        metadata["docker_server"] = docker_version
    return metadata


def benchmark_suite(result: dict[str, Any]) -> str:
    explicit = result.get("benchmark_suite")
    if isinstance(explicit, str) and explicit:
        return explicit
    if result.get("comparison_mode") == "cluster-baseline":
        return "cluster"
    if result.get("workload", {}).get("single_node") is True:
        return "native-comparison"
    return "other"


def _latency_values(scenario: dict[str, Any]) -> dict[str, float]:
    latency = scenario.get("latency_microseconds")
    if not isinstance(latency, dict):
        return {}
    return {
        str(name): float(value)
        for name, value in latency.items()
        if isinstance(value, (int, float))
    }


def normalize_result(result: dict[str, Any], *, source_name: str = "comparison") -> dict[str, Any]:
    if not isinstance(result.get("backends"), dict):
        raise NormalizationError("comparison result is missing a backends object")
    if not isinstance(result.get("workload"), dict):
        raise NormalizationError("comparison result is missing a workload object")

    backends: dict[str, Any] = {}
    for backend_name, backend in result["backends"].items():
        if not isinstance(backend, dict):
            raise NormalizationError(f"backend {backend_name!r} is not an object")
        scenarios: list[dict[str, Any]] = []
        for scenario in backend.get("scenarios", []):
            if not isinstance(scenario, dict):
                continue
            normalized_scenario: dict[str, Any] = {
                "operation": scenario.get("operation", "unknown"),
                "messages": scenario.get("messages"),
                "message_size_bytes": scenario.get("message_size_bytes"),
                "throughput_messages_per_second": scenario.get(
                    "throughput_messages_per_second"
                ),
                "latency_available": scenario.get("latency_available", "latency_microseconds" in scenario),
            }
            latency = _latency_values(scenario)
            if latency:
                normalized_scenario["latency_microseconds"] = latency
            resource_samples = scenario.get("resource_samples")
            if isinstance(resource_samples, dict):
                normalized_scenario["resource_samples"] = resource_samples
            metadata = scenario.get("metadata")
            if isinstance(metadata, dict):
                normalized_scenario["metadata"] = metadata
            for key in ("elapsed_milliseconds", "throughput_megabytes_per_second"):
                if key in scenario:
                    normalized_scenario[key] = scenario[key]
            scenarios.append(normalized_scenario)

        resource_samples = backend.get("resource_samples")
        if not isinstance(resource_samples, dict):
            resource_samples = {"samples": 0}
        backends[str(backend_name)] = {
            "image": backend.get("image"),
            "image_id": backend.get("image_id"),
            "acknowledgement": backend.get("acknowledgement"),
            "replication": backend.get("replication"),
            "measurement_boundary": backend.get("measurement_boundary"),
            "measurement_client": backend.get("measurement_client"),
            "startup_seconds": backend.get("startup_seconds"),
            "resource_samples": resource_samples,
            "scenarios": scenarios,
        }

    generated_at = result.get("generated_at")
    if not isinstance(generated_at, str):
        generated_at = datetime.now(UTC).isoformat()

    return {
        "history_schema_version": 1,
        "generated_at": generated_at,
        "source": source_metadata(),
        "environment": environment_metadata(),
        "comparison_mode": result.get("comparison_mode"),
        "benchmark_suite": benchmark_suite(result),
        "resource_limits": result.get("resource_limits", {}),
        "workload": result["workload"],
        "backends": backends,
        "source_result": source_name,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise NormalizationError("comparison result must be a JSON object")
        normalized = normalize_result(result, source_name=args.input.name)
    except (OSError, json.JSONDecodeError, NormalizationError) as error:
        raise SystemExit(f"could not normalize benchmark result: {error}") from error
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(normalized, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
