#!/usr/bin/env python3
"""Build a dependency-free static benchmark history dashboard."""

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
                size = scenario.get("message_size_bytes")
                size = int(size) if isinstance(size, (int, float)) else None
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
                for percentile, metric in (("p50", "latency_p50"), ("p99", "latency_p99"), ("p999", "latency_p999")):
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

            resources = backend.get("resource_samples", {})
            for metric, key in (("cpu_percent_max", "cpu_percent_max"), ("memory_bytes_max", "memory_bytes_max")):
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


def render_html(data: dict[str, Any]) -> str:
    encoded = json.dumps(data, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    definitions = json.dumps(
        [
            {"metric": metric, "title": title, "unit": unit, "higherBetter": higher_better}
            for metric, title, unit, higher_better in METRIC_DEFINITIONS
        ],
        separators=(",", ":"),
    )
    template = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Runnel benchmark history</title>
  <style>
    :root { color-scheme: light dark; --bg: #10151d; --panel: #18212c; --text: #e8eef5; --muted: #9eacba; --line: #344554; --accent: #75c7ff; }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 2rem; background: var(--bg); color: var(--text); font: 15px/1.5 system-ui, sans-serif; }
    main { max-width: 1280px; margin: 0 auto; }
    h1, h2, h3 { line-height: 1.2; }
    h1 { margin-bottom: .4rem; }
    h2 { margin-top: 2rem; }
    .muted { color: var(--muted); }
    .notice { padding: 1rem; border: 1px solid var(--line); border-radius: .6rem; background: var(--panel); color: var(--muted); }
    .controls { display: flex; flex-wrap: wrap; gap: 1rem; margin: 1.5rem 0; padding: 1rem; background: var(--panel); border: 1px solid var(--line); border-radius: .6rem; }
    label { display: grid; gap: .3rem; color: var(--muted); }
    select { min-width: 12rem; padding: .45rem; border: 1px solid var(--line); border-radius: .35rem; background: var(--bg); color: var(--text); }
    .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(30rem, 1fr)); gap: 1rem; }
    .chart-card { min-width: 0; padding: 1rem; background: var(--panel); border: 1px solid var(--line); border-radius: .6rem; }
    .chart-card h3 { margin: 0 0 .5rem; }
    .chart-card svg { display: block; width: 100%; height: auto; overflow: visible; }
    .axis { stroke: var(--line); stroke-width: 1; }
    .gridline { stroke: var(--line); stroke-width: .7; stroke-dasharray: 3 4; }
    .axis-label, .empty { fill: var(--muted); font-size: 11px; }
    .legend { display: flex; flex-wrap: wrap; gap: .7rem; margin-top: .5rem; color: var(--muted); font-size: .85rem; }
    .swatch { display: inline-block; width: .7rem; height: .7rem; margin-right: .25rem; border-radius: 50%; }
    table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); }
    th, td { padding: .6rem .7rem; text-align: left; border-bottom: 1px solid var(--line); }
    th { color: var(--muted); font-weight: 500; }
    a { color: var(--accent); }
    code { font-size: .9em; }
    @media (max-width: 700px) { body { padding: 1rem; } .charts { grid-template-columns: 1fr; } select { min-width: 10rem; } }
  </style>
</head>
<body>
<main>
  <h1>Runnel benchmark history</h1>
  <p class="muted">Automatic measurements from the pinned native-tool broker comparison.</p>
  <p class="notice">Interpret series according to their recorded measurement boundaries. Runnel and JetStream include acknowledgement paths; Kafka and Redpanda consumer figures currently measure fetch throughput without per-message application acknowledgement.</p>
  <section class="controls" aria-label="Chart filters">
    <label>Profile<select id="profile"></select></label>
    <label>Operation<select id="operation"></select></label>
    <label>Payload size<select id="size"></select></label>
  </section>
  <section id="charts" class="charts" aria-live="polite"></section>
  <h2>Recent runs</h2>
  <table>
    <thead><tr><th>Time</th><th>Profile</th><th>Commit</th><th>Event</th><th>Backends</th><th>Resources</th></tr></thead>
    <tbody id="runs"></tbody>
  </table>
</main>
<script id="benchmark-data" type="application/json">__DATA__</script>
<script>
const data = JSON.parse(document.getElementById('benchmark-data').textContent);
const definitions = __DEFINITIONS__;
const colors = ['#75c7ff', '#ff9f68', '#9be28f', '#d7a5ff', '#ffd166', '#ff7aa2', '#76e4d1'];
const profileSelect = document.getElementById('profile');
const operationSelect = document.getElementById('operation');
const sizeSelect = document.getElementById('size');
const charts = document.getElementById('charts');

function unique(values) { return [...new Set(values)].sort((a, b) => String(a).localeCompare(String(b))); }
function escapeText(value) { return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])); }
function formatValue(value, unit) {
  if (unit === 'bytes') {
    const units = ['B', 'KiB', 'MiB', 'GiB']; let n = value; let i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return `${n.toFixed(n >= 100 ? 0 : 1)} ${units[i]}`;
  }
  if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
function filteredPoints(metric, operation, size, profile) {
  return data.points.filter(point => point.metric === metric && point.operation === operation &&
    (size === null || point.message_size_bytes === size) && (profile === 'all' || point.profile === profile));
}
function renderChart(definition, operation, size, profile) {
  const points = filteredPoints(definition.metric, operation, size, profile);
  const card = document.createElement('article'); card.className = 'chart-card';
  const heading = document.createElement('h3'); heading.textContent = definition.title; card.appendChild(heading);
  if (!points.length) { const empty = document.createElement('p'); empty.className = 'muted'; empty.textContent = 'No measurements for this selection.'; card.appendChild(empty); return card; }
  const grouped = {};
  points.forEach(point => (grouped[point.backend] ||= []).push(point));
  Object.values(grouped).forEach(series => series.sort((a, b) => a.timestamp_ms - b.timestamp_ms));
  const allValues = points.map(point => point.value); let min = Math.min(...allValues); let max = Math.max(...allValues);
  min = Math.min(0, min); if (max === min) max = min + 1; const width = 900, height = 330, left = 70, right = 20, top = 20, bottom = 48;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  const times = points.map(point => point.timestamp_ms); const timeMin = Math.min(...times), timeMax = Math.max(...times);
  const x = value => timeMax === timeMin ? left + plotWidth / 2 : left + ((value - timeMin) / (timeMax - timeMin)) * plotWidth;
  const y = value => top + plotHeight - ((value - min) / (max - min)) * plotHeight;
  const ticks = [0, .25, .5, .75, 1];
  let svg = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeText(definition.title)} over time">`;
  ticks.forEach(tick => { const value = min + (max - min) * tick; const position = y(value); svg += `<line class="gridline" x1="${left}" x2="${width - right}" y1="${position}" y2="${position}"/><text class="axis-label" x="${left - 8}" y="${position + 4}" text-anchor="end">${escapeText(formatValue(value, definition.unit))}</text>`; });
  svg += `<line class="axis" x1="${left}" x2="${left}" y1="${top}" y2="${height - bottom}"/><line class="axis" x1="${left}" x2="${width - right}" y1="${height - bottom}" y2="${height - bottom}"/>`;
  Object.entries(grouped).forEach(([backend, series], index) => {
    const color = colors[index % colors.length]; const path = series.map((point, pointIndex) => `${pointIndex ? 'L' : 'M'} ${x(point.timestamp_ms)} ${y(point.value)}`).join(' ');
    svg += `<path d="${path}" fill="none" stroke="${color}" stroke-width="2"/>`;
    series.forEach(point => { const detail = `${new Date(point.timestamp).toLocaleString()} · ${backend} · ${formatValue(point.value, definition.unit)}`; svg += `<circle cx="${x(point.timestamp_ms)}" cy="${y(point.value)}" r="3.5" fill="${color}"><title>${escapeText(detail)}</title></circle>`; });
  });
  svg += `<text class="axis-label" x="${left + plotWidth / 2}" y="${height - 8}" text-anchor="middle">time</text></svg>`;
  card.insertAdjacentHTML('beforeend', svg);
  const legend = document.createElement('div'); legend.className = 'legend';
  Object.keys(grouped).forEach((backend, index) => { const item = document.createElement('span'); item.innerHTML = `<span class="swatch" style="background:${colors[index % colors.length]}"></span>${escapeText(backend)}`; legend.appendChild(item); });
  card.appendChild(legend); return card;
}
function refresh() {
  const profile = profileSelect.value; const operation = operationSelect.value; const sizeValue = sizeSelect.value;
  const size = sizeValue === 'all' ? null : Number(sizeValue); charts.replaceChildren();
  definitions.forEach(definition => charts.appendChild(renderChart(definition, operation, size, profile)));
}
function populate() {
  const profiles = unique(data.points.map(point => point.profile)); profileSelect.replaceChildren(new Option('All profiles', 'all'), ...profiles.map(value => new Option(value, value)));
  const operations = unique(data.points.filter(point => point.metric === 'throughput_messages_per_second').map(point => point.operation)); operationSelect.replaceChildren(...operations.map(value => new Option(value, value)));
  const sizes = unique(data.points.filter(point => point.operation === operationSelect.value && point.message_size_bytes !== null).map(point => point.message_size_bytes)); sizeSelect.replaceChildren(new Option('All sizes', 'all'), ...sizes.map(value => new Option(`${value} bytes`, value))); if (sizes.length) sizeSelect.value = String(sizes[0]);
  profileSelect.addEventListener('change', refresh); operationSelect.addEventListener('change', () => { populateSizes(); refresh(); }); sizeSelect.addEventListener('change', refresh); refresh();
}
function populateSizes() { const sizes = unique(data.points.filter(point => point.operation === operationSelect.value && point.message_size_bytes !== null).map(point => point.message_size_bytes)); const old = sizeSelect.value; sizeSelect.replaceChildren(new Option('All sizes', 'all'), ...sizes.map(value => new Option(`${value} bytes`, value))); if ([...sizeSelect.options].some(option => option.value === old)) sizeSelect.value = old; else if (sizes.length) sizeSelect.value = String(sizes[0]); }
function renderRuns() {
  const tbody = document.getElementById('runs'); [...data.runs].reverse().slice(0, 50).forEach(run => { const row = document.createElement('tr'); const time = new Date(run.timestamp).toLocaleString(); const commit = run.run_url ? `<a href="${escapeText(run.run_url)}">${escapeText(run.revision.slice(0, 12))}</a>` : `<code>${escapeText(run.revision.slice(0, 12))}</code>`; row.innerHTML = `<td>${escapeText(time)}</td><td>${escapeText(run.profile)}</td><td>${commit}</td><td>${escapeText(run.event || '')}</td><td>${escapeText(run.backends.join(', '))}</td><td>${escapeText(JSON.stringify(run.resource_limits || {}))}</td>`; tbody.appendChild(row); });
}
populate(); renderRuns();
</script>
</body>
</html>
'''
    return template.replace("__DATA__", encoded).replace("__DEFINITIONS__", definitions)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True, help="directory containing normalized run JSON files")
    parser.add_argument("--output", type=Path, required=True, help="directory for the generated site")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runs = load_runs(args.runs)
    if args.output.exists():
        shutil.rmtree(args.output)
    args.output.mkdir(parents=True, exist_ok=True)
    data = site_data(runs)
    (args.output / "data.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    (args.output / "index.html").write_text(render_html(data), encoding="utf-8")
    print(f"generated {len(runs)} runs and {len(data['points'])} measurements in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
