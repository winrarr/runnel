#!/usr/bin/env python3
"""Normalize a comparison result for durable benchmark history.

The comparison runner keeps native tool output for local investigation. History
only needs stable measurements and provenance, so this script deliberately
removes raw logs before a result is appended to the generated history branch.
"""

from __future__ import annotations

import argparse
import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import environment_metadata, source_metadata


class NormalizationError(RuntimeError):
    """The comparison result does not have the expected shape."""


def raw_backends(result: dict[str, Any]) -> dict[str, Any]:
    """Return canonical backends while accepting pre-envelope artifacts."""
    backends = result.get("backends")
    if isinstance(backends, dict):
        return backends

    container = result.get("container")
    scenarios = result.get("scenarios")
    if not isinstance(container, dict) or not isinstance(scenarios, list):
        raise NormalizationError("benchmark result is missing a backends object")
    return {
        "runnel": {
            **container,
            "runtime": "container",
            "acknowledgement": "local durable append",
            "replication": "single local broker engine",
            "measurement_boundary": "public line-delimited JSON protocol",
            "measurement_client": "host Python socket client",
            "client_image": "host Python runtime",
            "scenarios": scenarios,
        }
    }


def scenario_record(scenario: dict[str, Any]) -> dict[str, Any]:
    """Keep stable measured fields without carrying native tool logs."""
    fields = (
        "scenario_id",
        "operation",
        "messages",
        "message_size_bytes",
        "elapsed_seconds",
        "elapsed_milliseconds",
        "throughput_messages_per_second",
        "throughput_megabytes_per_second",
        "latency_sample_count",
        "latency_available",
        "latency_microseconds",
        "resource_samples",
        "server_metrics",
        "metadata",
        "restart_ready_seconds",
    )
    return {
        field: copy.deepcopy(scenario[field])
        for field in fields
        if field in scenario
    }


def source_record(result: dict[str, Any]) -> dict[str, Any]:
    source = result.get("source")
    if isinstance(source, dict):
        return copy.deepcopy(source)
    source = source_metadata(full_revision=True)
    revision = result.get("git_revision")
    if isinstance(revision, str) and revision:
        source["revision"] = revision
    return source


def environment_record(result: dict[str, Any]) -> dict[str, Any]:
    environment = result.get("environment")
    if isinstance(environment, dict):
        return copy.deepcopy(environment)
    host = result.get("host")
    if isinstance(host, dict):
        return copy.deepcopy(host)
    return environment_metadata(cpu_key="cpu_count", docker=True)


def benchmark_suite(result: dict[str, Any]) -> str:
    explicit = result.get("benchmark_suite")
    if isinstance(explicit, str) and explicit:
        return explicit
    if result.get("comparison_mode") == "cluster-baseline":
        return "cluster"
    if result.get("backend") == "runnel":
        return "runnel"
    if result.get("workload", {}).get("single_node") is True:
        return "native-comparison"
    return "other"


def normalize_result(result: dict[str, Any], *, source_name: str = "comparison") -> dict[str, Any]:
    if not isinstance(result.get("workload"), dict):
        raise NormalizationError("comparison result is missing a workload object")

    raw_backend_records = raw_backends(result)
    backends: dict[str, Any] = {}
    for backend_name, backend in raw_backend_records.items():
        if not isinstance(backend, dict):
            raise NormalizationError(f"backend {backend_name!r} is not an object")
        scenarios = [
            scenario_record(scenario)
            for scenario in backend.get("scenarios", [])
            if isinstance(scenario, dict)
        ]

        resource_samples = backend.get("resource_samples")
        if not isinstance(resource_samples, dict):
            resource_samples = {"samples": 0}
        normalized_backend = {
            field: copy.deepcopy(backend[field])
            for field in (
                "image",
                "image_id",
                "image_ids",
                "runtime",
                "cpu_limit",
                "memory_limit",
                "acknowledgement",
                "replication",
                "measurement_boundary",
                "measurement_client",
                "client_image",
                "semantic_metadata",
                "startup_seconds",
                "nodes",
            )
            if field in backend
        }
        normalized_backend.update(
            {"resource_samples": resource_samples, "scenarios": scenarios}
        )
        backends[str(backend_name)] = normalized_backend

    generated_at = result.get("generated_at")
    if not isinstance(generated_at, str):
        generated_at = datetime.now(UTC).isoformat()

    source = source_record(result)
    return {
        "history_schema_version": 1,
        "schema_version": 2,
        "run_id": result.get("run_id") or source.get("run_id") or source_name,
        "started_at": result.get("started_at"),
        "generated_at": generated_at,
        "finished_at": result.get("finished_at"),
        "status": result.get("status", "complete"),
        "command": copy.deepcopy(result.get("command")),
        "source": source,
        "environment": environment_record(result),
        "comparison_mode": result.get("comparison_mode"),
        "benchmark_suite": benchmark_suite(result),
        "resource_limits": result.get(
            "resource_limits",
            {
                key: result["container"][key]
                for key in ("cpu_limit", "memory_limit")
                if isinstance(result.get("container"), dict) and key in result["container"]
            },
        ),
        "workload": result["workload"],
        "backends": backends,
        "comparison_guardrail": copy.deepcopy(result.get("comparison_guardrail")),
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
