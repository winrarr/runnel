#!/usr/bin/env python3
"""Run a local current-vs-default-branch clustered benchmark.

This workflow is intended for performance-sensitive pull requests. It builds
the current commit and a detached worktree at the selected base ref, runs the
same three-node workload on the same host, aggregates repeated measurements by
median, and writes a Markdown report with relative deltas.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Iterator


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from pr_report import render_report  # noqa: E402
from resource_scope import ResourceScopeError, resource_limits, resource_scope_command  # noqa: E402


DEFAULT_BASE_REF = "origin/main"
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 200
DEFAULT_NODES = 3
DEFAULT_CONCURRENCY = 2
DEFAULT_MIN_REPETITIONS = 3
DEFAULT_MAX_REPETITIONS = 7
DEFAULT_MAX_THROUGHPUT_RANGE_PERCENT = 10.0
DEFAULT_MAX_P99_RANGE_PERCENT = 20.0
DEFAULT_PAYLOAD_SIZES = "100"
DEFAULT_CPUS = "2"
DEFAULT_MEMORY = "2g"
INCONCLUSIVE_EXIT_CODE = 2


class LocalBenchmarkError(RuntimeError):
    """An expected local benchmark setup or execution failure."""


class InconclusiveBenchmarkError(LocalBenchmarkError):
    """The benchmark completed but did not meet its stability thresholds."""


@dataclass(frozen=True)
class BenchmarkOptions:
    base_ref: str
    repetitions: int | None
    min_repetitions: int
    max_repetitions: int
    max_throughput_range_percent: float
    max_p99_range_percent: float
    cpu_limit: str
    memory_limit: str
    messages: int
    warmup: int
    nodes: int
    concurrency: int
    payload_sizes: str
    include_recovery: bool


@dataclass(frozen=True)
class BenchmarkTarget:
    name: str
    root: Path
    target_dir: Path
    output_dir: Path
    log_dir: Path

    @property
    def binary(self) -> Path:
        return self.target_dir / "release" / "runnel"

    @property
    def script(self) -> Path:
        return self.root / "scripts" / "benchmarks" / "cluster.py"


def git(*arguments: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def require_git(*arguments: str, cwd: Path = ROOT) -> str:
    result = git(*arguments, cwd=cwd)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise LocalBenchmarkError(f"git {' '.join(arguments)} failed: {detail}")
    return result.stdout.strip()


def ensure_clean_worktree() -> None:
    status = require_git("status", "--porcelain", "--untracked-files=all")
    if status:
        raise LocalBenchmarkError(
            "the current worktree must be clean so the report identifies the tested commit; "
            "commit or stash changes before running this benchmark"
        )


def ensure_ref_exists(ref: str) -> str:
    return require_git("rev-parse", "--verify", f"{ref}^{{commit}}")


@contextmanager
def detached_worktree(ref: str) -> Iterator[Path]:
    temporary_root = Path(tempfile.mkdtemp(prefix="runnel-pr-benchmark-"))
    worktree = temporary_root / "base"
    try:
        result = git("worktree", "add", "--detach", str(worktree), ref)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise LocalBenchmarkError(f"could not create base worktree at {ref}: {detail}")
        yield worktree
    finally:
        git("worktree", "remove", "--force", str(worktree))
        shutil.rmtree(temporary_root, ignore_errors=True)


def benchmark_command(
    target: BenchmarkTarget,
    options: BenchmarkOptions,
    output: Path,
    log_dir: Path,
    *,
    build: bool,
) -> list[str]:
    command = [sys.executable, str(target.script)]
    if build:
        command.append("--build")
    command.extend(
        [
            "--binary",
            str(target.binary),
            "--messages",
            str(options.messages),
            "--warmup",
            str(options.warmup),
            "--nodes",
            str(options.nodes),
            "--concurrency",
            str(options.concurrency),
            "--payload-sizes",
            options.payload_sizes,
            "--output",
            str(output),
            "--log-dir",
            str(log_dir),
        ]
    )
    if not options.include_recovery:
        command.append("--skip-recovery")
    return command


def run_benchmark(
    target: BenchmarkTarget,
    options: BenchmarkOptions,
    output: Path,
    log_dir: Path,
    *,
    build: bool,
    repetition: int,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    limits = resource_limits(cpus=options.cpu_limit, memory=options.memory_limit)
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_TARGET_DIR": str(target.target_dir),
            "BENCHMARK_PROFILE": "local-pull-request",
            "RUST_BACKTRACE": "1",
            "RUNNEL_BENCHMARK_CPU_LIMIT": limits["cpu"],
            "RUNNEL_BENCHMARK_MEMORY_LIMIT": limits["memory"],
        }
    )
    benchmark = benchmark_command(target, options, output, log_dir, build=build)
    command = resource_scope_command(
        benchmark,
        unit=f"runnel-benchmark-{os.getpid()}-{target.name}-{repetition + 1}",
        cpus=options.cpu_limit,
        memory=options.memory_limit,
    )
    print(f"Running {target.name} benchmark: {shlex_join(command)}", flush=True)
    try:
        subprocess.run(command, cwd=target.root, env=environment, check=True)
    except subprocess.CalledProcessError as error:
        raise LocalBenchmarkError(f"{target.name} benchmark failed with exit code {error.returncode}") from error


def normalize(raw: Path, normalized: Path, root: Path) -> None:
    command = [
        sys.executable,
        str(root / "scripts" / "benchmarks" / "normalize.py"),
        "--input",
        str(raw),
        "--output",
        str(normalized),
    ]
    subprocess.run(command, cwd=root, check=True)


def aggregate(inputs: list[Path], output: Path) -> dict[str, object]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "benchmarks" / "aggregate.py"),
        "--inputs",
        *(str(path) for path in inputs),
        "--output",
        str(output),
    ]
    subprocess.run(command, cwd=ROOT, check=True)
    return load_json(output)


def reportable_result(result: dict[str, object]) -> dict[str, object]:
    """Remove host-specific binary paths before the report is pasted publicly."""
    sanitized = copy.deepcopy(result)
    backends = sanitized.get("backends")
    if isinstance(backends, dict):
        for backend in backends.values():
            if isinstance(backend, dict) and "image" in backend:
                backend["image"] = "local release binary"
    return sanitized


def load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LocalBenchmarkError(f"could not read benchmark result {path}: {error}") from error
    if not isinstance(value, dict):
        raise LocalBenchmarkError(f"benchmark result {path} must contain a JSON object")
    return value


def stamp_resource_limits(path: Path, options: BenchmarkOptions) -> None:
    """Record the enclosing scope even when the tested revision is older."""
    result = load_json(path)
    result["resource_limits"] = resource_limits(cpus=options.cpu_limit, memory=options.memory_limit)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def shlex_join(command: list[str]) -> str:
    import shlex

    return shlex.join(command)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-ref",
        default=DEFAULT_BASE_REF,
        help=f"base commit or ref to compare against (default: {DEFAULT_BASE_REF})",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        help="exact runs per revision; diagnostic override that disables stability-based stopping",
    )
    parser.add_argument(
        "--allow-inconclusive",
        action="store_true",
        help="allow a non-stable result for diagnostics; never use it as optimization evidence",
    )
    parser.add_argument(
        "--min-repetitions",
        type=int,
        default=DEFAULT_MIN_REPETITIONS,
        help=f"minimum paired runs before stability can be accepted (default: {DEFAULT_MIN_REPETITIONS})",
    )
    parser.add_argument(
        "--max-repetitions",
        type=int,
        default=DEFAULT_MAX_REPETITIONS,
        help=f"maximum paired runs before reporting inconclusive (default: {DEFAULT_MAX_REPETITIONS})",
    )
    parser.add_argument(
        "--max-throughput-range-percent",
        type=float,
        default=DEFAULT_MAX_THROUGHPUT_RANGE_PERCENT,
        help=f"maximum observed throughput range for stability (default: {DEFAULT_MAX_THROUGHPUT_RANGE_PERCENT:g}%%)",
    )
    parser.add_argument(
        "--max-p99-range-percent",
        type=float,
        default=DEFAULT_MAX_P99_RANGE_PERCENT,
        help=f"maximum observed p99 range for stability (default: {DEFAULT_MAX_P99_RANGE_PERCENT:g}%%)",
    )
    parser.add_argument(
        "--cpus",
        default=DEFAULT_CPUS,
        help=f"CPU quota shared by the benchmark client and broker nodes (default: {DEFAULT_CPUS})",
    )
    parser.add_argument(
        "--memory",
        default=DEFAULT_MEMORY,
        help=f"memory limit shared by the benchmark client and broker nodes (default: {DEFAULT_MEMORY})",
    )
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--payload-sizes", default=DEFAULT_PAYLOAD_SIZES)
    parser.add_argument(
        "--include-recovery",
        action="store_true",
        help="include the restart-recovery scenario; it adds an acknowledgement-timeout delay",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Markdown report path (default: benchmark-results/pr-local/<run>/report.md)",
    )
    # `just recipe -- --flag` leaves the recipe separator in the expanded
    # command. Remove it wherever it occurs so wrappers such as
    # bench-pr-local-quick can add their own defaults before user flags.
    arguments = [argument for argument in sys.argv[1:] if argument != "--"]
    args = parser.parse_args(arguments)
    if args.repetitions is not None and args.repetitions <= 0:
        parser.error("repetitions must be positive")
    if args.min_repetitions <= 0 or args.max_repetitions < args.min_repetitions:
        parser.error("maximum repetitions must be at least the positive minimum")
    if args.max_throughput_range_percent <= 0 or args.max_p99_range_percent <= 0:
        parser.error("stability range thresholds must be positive")
    if args.messages <= 0 or args.warmup < 0 or args.nodes < 3 or args.concurrency <= 0:
        parser.error("messages, nodes, and concurrency must be positive; warmup cannot be negative")
    if not args.payload_sizes.strip():
        parser.error("payload sizes cannot be empty")
    return args


def _range_percent(result: dict[str, object], metric: str) -> float | None:
    maximum: float | None = None
    backends = result.get("backends")
    if not isinstance(backends, dict):
        return None
    for backend in backends.values():
        if not isinstance(backend, dict):
            continue
        scenarios = backend.get("scenarios")
        if not isinstance(scenarios, list):
            continue
        for scenario in scenarios:
            if not isinstance(scenario, dict):
                continue
            summary = scenario.get("repetition_summary")
            if not isinstance(summary, dict):
                continue
            if metric == "throughput":
                metric_summary = summary.get("throughput_messages_per_second")
            else:
                latency_summary = summary.get("latency_p99")
                metric_summary = latency_summary if isinstance(latency_summary, dict) else None
            if not isinstance(metric_summary, dict):
                continue
            value = metric_summary.get("relative_range_percent")
            if isinstance(value, (int, float)):
                maximum = max(maximum or 0.0, float(value))
    return maximum


def _scenario_key(backend_name: object, scenario: dict[str, object]) -> tuple[str, str, str, str, str]:
    operation = scenario.get("operation", scenario.get("name", "unknown"))
    return (
        str(backend_name),
        str(operation),
        json.dumps(scenario.get("message_size_bytes"), sort_keys=True),
        json.dumps(scenario.get("messages"), sort_keys=True),
        json.dumps(scenario.get("metadata", {}), sort_keys=True),
    )


def _metric_samples(result: dict[str, object], metric: str) -> dict[tuple[str, str, str, str, str], list[float]]:
    samples: dict[tuple[str, str, str, str, str], list[float]] = {}
    backends = result.get("backends")
    if not isinstance(backends, dict):
        return samples
    for backend_name, backend in backends.items():
        if not isinstance(backend, dict):
            continue
        scenarios = backend.get("scenarios")
        if not isinstance(scenarios, list):
            continue
        for scenario in scenarios:
            if not isinstance(scenario, dict):
                continue
            repetition_summary = scenario.get("repetition_summary")
            if not isinstance(repetition_summary, dict):
                continue
            metric_summary = (
                repetition_summary.get("throughput_messages_per_second")
                if metric == "throughput"
                else repetition_summary.get("latency_p99")
            )
            if not isinstance(metric_summary, dict):
                continue
            values = metric_summary.get("samples")
            if not isinstance(values, list):
                continue
            numeric = [
                float(value)
                for value in values
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            ]
            if numeric:
                samples[_scenario_key(backend_name, scenario)] = numeric
    return samples


def _quartile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _tukey_outliers(values: list[float]) -> set[int]:
    """Return Tukey-fence indices for a sample set large enough to filter."""
    if len(values) < 4:
        return set()
    first_quartile = _quartile(values, 0.25)
    third_quartile = _quartile(values, 0.75)
    spread = third_quartile - first_quartile
    lower_fence = first_quartile - 1.5 * spread
    upper_fence = third_quartile + 1.5 * spread
    return {
        index
        for index, value in enumerate(values)
        if value < lower_fence or value > upper_fence
    }


def _range_for_samples(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    center = float(median(values))
    if center == 0:
        return None
    return (max(values) - min(values)) / abs(center) * 100


def _direction_summary(
    pairs: list[tuple[list[float], list[float], set[int]]],
    *,
    metric: str,
) -> dict[str, object]:
    deltas: list[float] = []
    improved = worsened = neutral = 0
    paired_samples = 0
    for current, baseline, excluded in pairs:
        retained_indices = [
            index for index in range(min(len(current), len(baseline))) if index not in excluded
        ]
        if not retained_indices:
            continue
        current_median = float(median([current[index] for index in retained_indices]))
        baseline_median = float(median([baseline[index] for index in retained_indices]))
        if baseline_median == 0:
            continue
        delta = (current_median - baseline_median) / abs(baseline_median) * 100
        deltas.append(delta)
        paired_samples += len(retained_indices)
        if delta == 0:
            neutral += 1
        elif (metric == "throughput" and delta > 0) or (metric == "p99" and delta < 0):
            improved += 1
        else:
            worsened += 1

    if not deltas:
        return {"status": "unavailable"}
    if improved > worsened:
        direction = "generally improved"
    elif worsened > improved:
        direction = "generally worsened"
    else:
        direction = "mixed/unclear"
    return {
        "status": "available",
        "direction": direction,
        "median_delta_percent": float(median(deltas)),
        "improved_scenarios": improved,
        "worsened_scenarios": worsened,
        "neutral_scenarios": neutral,
        "scenario_count": len(deltas),
        "paired_samples": paired_samples,
    }


def _metric_interpretation(
    current: dict[str, object],
    baseline: dict[str, object],
    metric: str,
) -> dict[str, object]:
    current_samples = _metric_samples(current, metric)
    baseline_samples = _metric_samples(baseline, metric)
    pairs: list[tuple[list[float], list[float], set[int]]] = []
    raw_ranges: list[float] = []
    filtered_ranges: list[float] = []
    candidate_outlier_pairs = 0
    total_pairs = 0
    for key in sorted(current_samples.keys() & baseline_samples.keys()):
        current_values = current_samples[key]
        baseline_values = baseline_samples[key]
        pair_count = min(len(current_values), len(baseline_values))
        if pair_count == 0:
            continue
        current_values = current_values[:pair_count]
        baseline_values = baseline_values[:pair_count]
        excluded = _tukey_outliers(current_values) | _tukey_outliers(baseline_values)
        pairs.append((current_values, baseline_values, excluded))
        total_pairs += pair_count
        candidate_outlier_pairs += len(excluded)
        for values in (current_values, baseline_values):
            value_range = _range_for_samples(values)
            if value_range is not None:
                raw_ranges.append(value_range)
            retained = [value for index, value in enumerate(values) if index not in excluded]
            value_range = _range_for_samples(retained)
            if value_range is not None:
                filtered_ranges.append(value_range)

    if not pairs:
        return {"status": "unavailable"}
    filtered_pairs = _direction_summary(pairs, metric=metric)
    raw_pairs = _direction_summary(
        [(current_values, baseline_values, set()) for current_values, baseline_values, _ in pairs],
        metric=metric,
    )
    return {
        "status": "available",
        "raw": raw_pairs,
        "filtered": filtered_pairs,
        "total_pairs": total_pairs,
        "candidate_outlier_pairs": candidate_outlier_pairs,
        "retained_pairs": total_pairs - candidate_outlier_pairs,
        "raw_max_range_percent": max(raw_ranges) if raw_ranges else None,
        "filtered_max_range_percent": max(filtered_ranges) if filtered_ranges else None,
    }


def benchmark_interpretation(
    current: dict[str, object], baseline: dict[str, object]
) -> dict[str, object]:
    """Describe direction and robust outlier sensitivity without making an inference claim."""
    return {
        "method": "Matched scenario medians; higher throughput and lower p99 are improvements.",
        "throughput": _metric_interpretation(current, baseline, "throughput"),
        "p99": _metric_interpretation(current, baseline, "p99"),
        "outlier_method": "Diagnostic Tukey 1.5xIQR fences per scenario and revision; a pair is excluded if either side is flagged.",
    }


def assess_stability(
    current: dict[str, object],
    baseline: dict[str, object],
    options: BenchmarkOptions,
    repetitions: int,
) -> dict[str, object]:
    """Assess whether both revisions have sufficiently repeatable measurements."""
    def maximum_range(metric: str) -> float | None:
        values = [
            value
            for value in (_range_percent(current, metric), _range_percent(baseline, metric))
            if value is not None
        ]
        return max(values) if values else None

    throughput_range = maximum_range("throughput")
    p99_range = maximum_range("p99")
    observed = [value for value in (throughput_range, p99_range) if value is not None]
    stable = (
        options.repetitions is None
        and repetitions >= options.min_repetitions
        and bool(observed)
        and (throughput_range is None or throughput_range <= options.max_throughput_range_percent)
        and (p99_range is None or p99_range <= options.max_p99_range_percent)
    )
    if options.repetitions is not None:
        status = "fixed-repetitions"
    elif stable:
        status = "stable"
    else:
        status = "inconclusive"
    reasons: list[str] = []
    if repetitions < options.min_repetitions:
        reasons.append("minimum repetitions were not reached")
    if throughput_range is None:
        reasons.append("no throughput range was available")
    elif throughput_range > options.max_throughput_range_percent:
        reasons.append("throughput range exceeded its limit")
    if p99_range is None:
        reasons.append("no p99 range was available")
    elif p99_range > options.max_p99_range_percent:
        reasons.append("p99 range exceeded its limit")
    return {
        "status": status,
        "repetitions": repetitions,
        "minimum_repetitions": options.min_repetitions,
        "maximum_repetitions": options.max_repetitions,
        "maximum_throughput_range_percent": options.max_throughput_range_percent,
        "maximum_p99_range_percent": options.max_p99_range_percent,
        "observed_throughput_range_percent": throughput_range,
        "observed_p99_range_percent": p99_range,
        "inconclusive_reasons": reasons,
        "interpretation": benchmark_interpretation(current, baseline),
    }


def require_stable(stability: dict[str, object], *, allow_inconclusive: bool) -> None:
    """Reject non-authoritative results unless the caller explicitly requests diagnostics."""
    if allow_inconclusive or stability.get("status") == "stable":
        return

    status = stability.get("status", "unknown")
    raise InconclusiveBenchmarkError(
        "authoritative benchmark finished with "
        f"{status}; stable repeated measurements are required. "
        "Rerun under the same controlled conditions or use --allow-inconclusive "
        "only for a diagnostic run."
    )


def main() -> int:
    args = parse_args()
    try:
        ensure_clean_worktree()
        base_revision = ensure_ref_exists(args.base_ref)
        current_revision = ensure_ref_exists("HEAD")
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        run_dir = ROOT / "benchmark-results" / "pr-local" / f"{timestamp}-{os.getpid()}"
        run_dir.mkdir(parents=True, exist_ok=False)
        report_path = args.output or run_dir / "report.md"
        if not report_path.is_absolute():
            report_path = ROOT / report_path

        options = BenchmarkOptions(
            base_ref=args.base_ref,
            repetitions=args.repetitions,
            min_repetitions=args.repetitions or args.min_repetitions,
            max_repetitions=args.repetitions or args.max_repetitions,
            max_throughput_range_percent=args.max_throughput_range_percent,
            max_p99_range_percent=args.max_p99_range_percent,
            cpu_limit=args.cpus,
            memory_limit=args.memory,
            messages=args.messages,
            warmup=args.warmup,
            nodes=args.nodes,
            concurrency=args.concurrency,
            payload_sizes=args.payload_sizes,
            include_recovery=args.include_recovery,
        )
        current = BenchmarkTarget(
            name="pull-request",
            root=ROOT,
            target_dir=run_dir / "target-current",
            output_dir=run_dir / "current",
            log_dir=run_dir / "logs-current",
        )

        with detached_worktree(args.base_ref) as base_root:
            base = BenchmarkTarget(
                name="default-branch",
                root=base_root,
                target_dir=run_dir / "target-base",
                output_dir=run_dir / "base",
                log_dir=run_dir / "logs-base",
            )
            # Alternate revisions so host scheduling or thermal changes are
            # less likely to bias every measurement toward one side. Stop once
            # both revisions meet the explicit repeatability thresholds.
            completed_repetitions = 0
            current_result: dict[str, object] | None = None
            base_result: dict[str, object] | None = None
            stability: dict[str, object] | None = None
            for repetition in range(options.max_repetitions):
                run_target_once(current, options, repetition, build=repetition == 0)
                run_target_once(base, options, repetition, build=repetition == 0)
                completed_repetitions = repetition + 1
                current_result = aggregate_target(current, completed_repetitions)
                base_result = aggregate_target(base, completed_repetitions)
                stability = assess_stability(current_result, base_result, options, completed_repetitions)
                if stability["status"] == "stable" or options.repetitions is not None:
                    break

            if current_result is None or base_result is None or stability is None:
                raise LocalBenchmarkError("benchmark produced no repetitions")
            current_result["benchmark_stability"] = stability

        report = render_report(
            reportable_result(current_result),
            reportable_result(base_result),
            heading="Local clustered Runnel benchmark",
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        print(f"\nMarkdown report: {report_path}")
        print(report, end="")
        print(f"Artifacts: {run_dir}")
        print(f"Current revision: {current_revision[:12]}")
        print(f"Base revision: {base_revision[:12]} ({args.base_ref})")
        require_stable(stability, allow_inconclusive=args.allow_inconclusive)
        return 0
    except InconclusiveBenchmarkError as error:
        print(f"pr_local.py: error: {error}", file=sys.stderr)
        return INCONCLUSIVE_EXIT_CODE
    except (LocalBenchmarkError, ResourceScopeError, OSError, subprocess.CalledProcessError) as error:
        print(f"pr_local.py: error: {error}", file=sys.stderr)
        return 1


def run_target_once(
    target: BenchmarkTarget,
    options: BenchmarkOptions,
    repetition: int,
    *,
    build: bool,
) -> dict[str, object]:
    raw = target.output_dir / f"{target.name}-{repetition + 1}.json"
    normalized_path = target.output_dir / f"{target.name}-{repetition + 1}-normalized.json"
    run_benchmark(
        target,
        options,
        raw,
        target.log_dir / str(repetition + 1),
        build=build,
        repetition=repetition,
    )
    # Older base revisions may not know about the wrapper's cgroup metadata.
    # Stamp the actual enclosing scope into both results so aggregation and the
    # report cannot mistake an equivalently limited run for an unlimited one.
    stamp_resource_limits(raw, options)
    normalize(raw, normalized_path, target.root)
    return load_json(normalized_path)


def aggregate_target(target: BenchmarkTarget, repetitions: int) -> dict[str, object]:
    inputs = [
        target.output_dir / f"{target.name}-{repetition}-normalized.json"
        for repetition in range(1, repetitions + 1)
    ]
    return aggregate(inputs, target.output_dir / f"{target.name}-aggregate.json")


if __name__ == "__main__":
    raise SystemExit(main())
