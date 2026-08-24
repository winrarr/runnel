#!/usr/bin/env python3
"""Render a concise, safe Markdown comment from a Runnel benchmark result."""

from __future__ import annotations

import argparse
import html
import json
import math
from pathlib import Path
from typing import Any


MAX_SCENARIOS = 32
MAX_TEXT_LENGTH = 120


def _number(value: object) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return value


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _bounded_text(value: object) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    if len(text) > MAX_TEXT_LENGTH:
        text = text[: MAX_TEXT_LENGTH - 1].rstrip() + "…"
    return text


def _cell(value: object) -> str:
    """Escape a scalar for a Markdown table without emitting arbitrary markup."""
    text = html.escape(_bounded_text(value), quote=False)
    return text.replace("\\", "\\\\").replace("|", "\\|")


def _code(value: object) -> str:
    text = _bounded_text(value).replace("`", "'")
    return f"`{html.escape(text, quote=False)}`"


def _format_number(value: object, decimals: int = 1) -> str | None:
    number = _number(value)
    if number is None:
        return None
    if number == int(number):
        return f"{int(number):,}"
    return f"{number:,.{decimals}f}".rstrip("0").rstrip(".")


def _format_bytes(value: object) -> str | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    unit = 0
    while number >= 1024 and unit < len(units) - 1:
        number /= 1024
        unit += 1
    formatted = _format_number(number, 1)
    return f"{formatted} {units[unit]}" if formatted is not None else None


def _format_duration_us(value: object) -> str | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    if number >= 1_000_000:
        formatted = _format_number(number / 1_000_000, 2)
        unit = "s"
    elif number >= 1_000:
        formatted = _format_number(number / 1_000, 2)
        unit = "ms"
    else:
        formatted = _format_number(number, 2)
        unit = "µs"
    return f"{formatted} {unit}" if formatted is not None else None


def _format_seconds(value: object) -> str | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    formatted = _format_number(number, 3)
    return f"{formatted} s" if formatted is not None else None


def _format_percent(value: object) -> str | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    formatted = _format_number(number, 1)
    return f"{formatted}%" if formatted is not None else None


def _format_scalar(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(_bounded_text(item) for item in value)
    if isinstance(value, dict):
        return _bounded_text(_canonical(value))
    number = _number(value)
    if number is not None:
        return _format_number(number) or "—"
    return _bounded_text(value)


def _sources(result: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    scenarios = result.get("scenarios")
    if isinstance(scenarios, list):
        sources.append(
            {
                "name": _bounded_text(result.get("backend") or result.get("engine") or "runnel"),
                "data": result,
                "scenarios": scenarios,
                "resource_samples": _mapping_or_empty(
                    _mapping_or_empty(result.get("container")).get("resource_samples")
                ),
            }
        )

    backends = result.get("backends")
    if isinstance(backends, dict):
        for name, backend in backends.items():
            if not str(name).lower().startswith("runnel") or not isinstance(backend, dict):
                continue
            backend_scenarios = backend.get("scenarios")
            if not isinstance(backend_scenarios, list):
                continue
            sources.append(
                {
                    "name": _bounded_text(name),
                    "data": backend,
                    "scenarios": backend_scenarios,
                    "resource_samples": _mapping_or_empty(backend.get("resource_samples")),
                }
            )

    if not sources:
        raise ValueError("input does not contain Runnel benchmark scenarios")
    return sources


def _mapping_or_empty(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _workload(result: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any] | None:
    value = result.get("workload")
    if isinstance(value, dict):
        return value
    for source in sources:
        value = source["data"].get("workload")
        if isinstance(value, dict):
            return value
    return None


def _limits(result: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    value = result.get("resource_limits")
    if isinstance(value, dict):
        return value
    for source in sources:
        data = source["data"]
        value = data.get("resource_limits")
        if isinstance(value, dict):
            return value
        container = data.get("container")
        if isinstance(container, dict):
            limits = {
                key: container[key]
                for key in ("cpu_limit", "memory_limit")
                if key in container
            }
            if limits:
                return limits
        limits = {key: data[key] for key in ("cpu_limit", "memory_limit") if key in data}
        if limits:
            return limits
    return {}


def _revision(result: dict[str, Any], sources: list[dict[str, Any]]) -> object:
    for key in ("git_revision", "revision"):
        if result.get(key):
            return result[key]
    for source in sources:
        for key in ("git_revision", "revision"):
            if source["data"].get(key):
                return source["data"][key]
    return "unknown"


def _image(source: dict[str, Any]) -> object | None:
    data = source["data"]
    if data.get("image"):
        return data["image"]
    container = data.get("container")
    if isinstance(container, dict):
        return container.get("image")
    return None


def _format_workload(workload: dict[str, Any] | None) -> str:
    if workload is None:
        return "not recorded"
    parts: list[str] = []
    messages = _format_number(workload.get("messages"))
    if messages is not None:
        parts.append(f"{messages} messages")
    sizes = workload.get("payload_sizes_bytes")
    if isinstance(sizes, list):
        formatted_sizes = [_format_bytes(size) for size in sizes]
        formatted_sizes = [size for size in formatted_sizes if size is not None]
        if formatted_sizes:
            if len(formatted_sizes) > 8:
                formatted_sizes = formatted_sizes[:8] + ["…"]
            parts.append(f"payload {', '.join(formatted_sizes)}")
    for key, label in (
        ("warmup", "warmup"),
        ("concurrency", "concurrency"),
        ("nodes", "nodes"),
        ("ack_timeout_ms", "ack timeout ms"),
    ):
        value = _format_number(workload.get(key))
        if value is not None:
            parts.append(f"{label} {value}")
    return "; ".join(parts) if parts else "recorded"


def _format_limits(limits: dict[str, Any]) -> str:
    if not limits:
        return "not recorded"
    preferred = [
        key
        for key in ("cpu", "cpu_limit", "broker_cpu", "memory", "memory_limit", "broker_memory")
        if key in limits
    ]
    keys = preferred + [key for key in limits if key not in preferred]
    parts: list[str] = []
    for key in keys[:8]:
        value = limits[key]
        if value is None:
            continue
        label = _bounded_text(key.removesuffix("_limit").replace("_", " ")).replace("`", "'")
        label = html.escape(label, quote=False)
        if "cpu" in key:
            label = label.replace("cpu", "CPU")
        elif "memory" in key:
            label = label.replace("memory", "memory")
        parts.append(f"{label} {_code(value)}")
    if len(keys) > 8:
        parts.append("…")
    return "; ".join(parts) if parts else "not recorded"


def _format_observed_resources(sources: list[dict[str, Any]]) -> str | None:
    parts: list[str] = []
    for source in sources:
        resource = source["resource_samples"]
        observed: list[str] = []
        for key, label, formatter in (
            ("cpu_percent_avg", "CPU avg", _format_percent),
            ("cpu_percent_max", "CPU max", _format_percent),
            ("memory_bytes_avg", "memory avg", _format_bytes),
            ("memory_bytes_max", "memory max", _format_bytes),
        ):
            formatted = formatter(resource.get(key))
            if formatted is not None:
                observed.append(f"{label} {formatted}")
        if observed:
            prefix = f"{_bounded_text(source['name'])}: " if len(sources) > 1 else ""
            parts.append(prefix + "; ".join(observed))
    return "; ".join(parts) if parts else None


def _scenario_record(source: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    operation = scenario.get("operation") or scenario.get("name") or "unnamed"
    return {
        "source": source["name"],
        "scenario": scenario,
        "operation": _bounded_text(operation),
        "messages": scenario.get("messages"),
        "message_size_bytes": scenario.get("message_size_bytes"),
    }


def _metric_value(record: dict[str, Any], metric: str) -> float | int | None:
    scenario = record["scenario"]
    if metric == "throughput":
        return _number(scenario.get("throughput_messages_per_second"))
    latency = _mapping_or_empty(scenario.get("latency_microseconds"))
    if metric in {"p50", "p99", "p999"}:
        keys = ("p999", "p99.9", "p99_9") if metric == "p999" else (metric,)
        for key in keys:
            value = _number(latency.get(key))
            if value is not None:
                return value
        return None
    resource = _mapping_or_empty(scenario.get("resource_samples"))
    if metric == "cpu":
        for value in (resource.get("cpu_seconds"), scenario.get("cpu_seconds")):
            number = _number(value)
            if number is not None:
                return number
        return None
    if metric == "memory":
        for value in (
            resource.get("memory_bytes_max"),
            resource.get("memory_bytes_avg"),
            scenario.get("memory_bytes_max"),
        ):
            number = _number(value)
            if number is not None:
                return number
        return None
    raise ValueError(f"unknown metric {metric}")


def _format_metric(metric: str, value: object) -> str | None:
    if metric == "throughput":
        formatted = _format_number(value)
        return f"{formatted} msg/s" if formatted is not None else None
    if metric in {"p50", "p99", "p999"}:
        return _format_duration_us(value)
    if metric == "cpu":
        return _format_seconds(value)
    if metric == "memory":
        return _format_bytes(value)
    raise ValueError(f"unknown metric {metric}")


def _scenario_identity(record: dict[str, Any], workload_key: str | None) -> tuple[str, str, str, str, str] | None:
    if workload_key is None:
        return None
    return (
        _bounded_text(record["source"]),
        workload_key,
        _bounded_text(record["operation"]),
        _canonical(record.get("messages")),
        _canonical(record.get("message_size_bytes")),
    )


def workload_identity(result: dict[str, Any]) -> str | None:
    """Return the canonical workload identity used for safe baseline matching."""
    sources = _sources(result)
    workload = _workload(result, sources)
    return _canonical(workload) if workload is not None else None


def _percent_delta(current: object, baseline: object) -> float | None:
    current_number = _number(current)
    baseline_number = _number(baseline)
    if current_number is None or baseline_number is None or baseline_number == 0:
        return None
    return (current_number - baseline_number) / abs(baseline_number) * 100


def _with_delta(formatted: str | None, current: object, baseline: object | None) -> str:
    if formatted is None:
        return "—"
    if baseline is None:
        return formatted
    delta = _percent_delta(current, baseline)
    if delta is None:
        return formatted
    return f"{formatted} (Δ {delta:+.1f}%)"


def render_report(result: dict[str, Any], baseline: dict[str, Any] | None = None) -> str:
    """Render a bounded report from a raw container or clustered result."""
    current_sources = _sources(result)
    current_workload = _workload(result, current_sources)
    current_workload_key = _canonical(current_workload) if current_workload is not None else None
    current_records = [
        _scenario_record(source, scenario)
        for source in current_sources
        for scenario in source["scenarios"]
        if isinstance(scenario, dict)
    ]

    baseline_records: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    baseline_sources: list[dict[str, Any]] = []
    baseline_workload_key: str | None = None
    if baseline is not None:
        baseline_sources = _sources(baseline)
        baseline_workload_key = workload_identity(baseline)
        if current_workload_key is not None and current_workload_key == baseline_workload_key:
            for source in baseline_sources:
                for scenario in source["scenarios"]:
                    if not isinstance(scenario, dict):
                        continue
                    record = _scenario_record(source, scenario)
                    identity = _scenario_identity(record, baseline_workload_key)
                    if identity is not None:
                        baseline_records.setdefault(identity, record)

    shown_records = current_records[:MAX_SCENARIOS]
    metric_names = ("throughput", "p50", "p99", "p999", "cpu", "memory")
    visible_metrics = [
        metric
        for metric in metric_names
        if any(_metric_value(record, metric) is not None for record in shown_records)
    ]
    multiple_sources = len({record["source"] for record in shown_records}) > 1

    lines = ["## Runnel benchmark", ""]
    lines.append(f"- Revision: {_code(_revision(result, current_sources))}")
    lines.append(f"- Workload: {_cell(_format_workload(current_workload))}")
    lines.append(f"- Limits: {_format_limits(_limits(result, current_sources))}")
    images = [_image(source) for source in current_sources if _image(source) is not None]
    if images:
        image_text = ", ".join(_code(image) for image in images[:4])
        if len(images) > 4:
            image_text += ", …"
        lines.append(f"- Target: {image_text}")
    observed = _format_observed_resources(current_sources)
    if observed is not None:
        lines.append(f"- Observed resources: {_cell(observed)}")

    if baseline is not None:
        lines.append(f"- Baseline revision: {_code(_revision(baseline, baseline_sources))}")
        if current_workload_key is None or current_workload_key != baseline_workload_key:
            lines.append("> Baseline deltas omitted: workload identity differs or is not recorded.")
        elif not all(
            _scenario_identity(record, current_workload_key) in baseline_records
            for record in shown_records
        ):
            lines.append("> Baseline deltas are shown only for matching operation, message count, and message size.")
        else:
            lines.append("> Baseline deltas compare matching workload, operation, message count, and message size.")

    lines.append("")
    if not shown_records:
        lines.append("No measured scenarios found.")
        return "\n".join(lines) + "\n"

    headers = (["Source"] if multiple_sources else []) + ["Operation", "Messages", "Size"]
    headers.extend(
        {
            "throughput": "Throughput",
            "p50": "p50",
            "p99": "p99",
            "p999": "p99.9",
            "cpu": "CPU",
            "memory": "Memory",
        }[metric]
        for metric in visible_metrics
    )
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---:" if index else "---" for index in range(len(headers))) + " |")

    for record in shown_records:
        baseline_record = None
        identity = _scenario_identity(record, current_workload_key)
        if identity is not None:
            baseline_record = baseline_records.get(identity)
        cells = []
        if multiple_sources:
            cells.append(_cell(record["source"]))
        cells.extend(
            [
                _cell(record["operation"]),
                _cell(_format_scalar(record.get("messages")) if record.get("messages") is not None else "—"),
                _cell(
                    _format_bytes(record.get("message_size_bytes"))
                    or (_format_scalar(record.get("message_size_bytes")) if record.get("message_size_bytes") is not None else "—")
                ),
            ]
        )
        for metric in visible_metrics:
            current_value = _metric_value(record, metric)
            baseline_value = _metric_value(baseline_record, metric) if baseline_record else None
            cells.append(_cell(_with_delta(_format_metric(metric, current_value), current_value, baseline_value)))
        lines.append("| " + " | ".join(cells) + " |")

    if len(current_records) > MAX_SCENARIOS:
        lines.append("")
        lines.append(f"Showing the first {MAX_SCENARIOS} scenarios.")
    return "\n".join(lines) + "\n"


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON in {path}: {error.msg}") from error
    except OSError as error:
        raise ValueError(f"could not read {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"result in {path} must be a JSON object")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="benchmark result JSON")
    parser.add_argument("--output", type=Path, required=True, help="Markdown report path")
    parser.add_argument("--baseline", type=Path, help="optional compatible benchmark result JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = _load(args.input)
        baseline = _load(args.baseline) if args.baseline else None
        report = render_report(result, baseline)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    except (OSError, ValueError) as error:
        raise SystemExit(f"pr_report.py: error: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
