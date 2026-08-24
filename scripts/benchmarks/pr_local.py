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
from typing import Iterator


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from pr_report import render_report  # noqa: E402


DEFAULT_BASE_REF = "origin/main"
DEFAULT_MESSAGES = 1_000
DEFAULT_WARMUP = 200
DEFAULT_NODES = 3
DEFAULT_CONCURRENCY = 2
DEFAULT_REPETITIONS = 3
DEFAULT_PAYLOAD_SIZES = "100"


class LocalBenchmarkError(RuntimeError):
    """An expected local benchmark setup or execution failure."""


@dataclass(frozen=True)
class BenchmarkOptions:
    base_ref: str
    repetitions: int
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
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_TARGET_DIR": str(target.target_dir),
            "BENCHMARK_PROFILE": "local-pull-request",
            "RUST_BACKTRACE": "1",
        }
    )
    command = benchmark_command(target, options, output, log_dir, build=build)
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
        default=DEFAULT_REPETITIONS,
        help=f"independent runs per revision, aggregated by median (default: {DEFAULT_REPETITIONS})",
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
    arguments = sys.argv[1:]
    if arguments[:1] == ["--"]:
        arguments = arguments[1:]
    args = parser.parse_args(arguments)
    if (
        args.repetitions <= 0
        or args.messages <= 0
        or args.warmup < 0
        or args.nodes < 3
        or args.concurrency <= 0
    ):
        parser.error("repetitions, messages, nodes, and concurrency must be positive; warmup cannot be negative")
    if not args.payload_sizes.strip():
        parser.error("payload sizes cannot be empty")
    return args


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
            # less likely to bias every measurement toward one side.
            for repetition in range(options.repetitions):
                run_target_once(current, options, repetition, build=repetition == 0)
                run_target_once(base, options, repetition, build=repetition == 0)

            current_result = aggregate_target(current, options)
            base_result = aggregate_target(base, options)

        report = render_report(
            reportable_result(current_result),
            base_result,
            heading="Local clustered Runnel benchmark",
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        print(f"\nMarkdown report: {report_path}")
        print(report, end="")
        print(f"Artifacts: {run_dir}")
        print(f"Current revision: {current_revision[:12]}")
        print(f"Base revision: {base_revision[:12]} ({args.base_ref})")
        return 0
    except (LocalBenchmarkError, OSError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"pr_local.py: error: {error}") from error


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
    )
    normalize(raw, normalized_path, target.root)
    return load_json(normalized_path)


def aggregate_target(target: BenchmarkTarget, options: BenchmarkOptions) -> dict[str, object]:
    inputs = [
        target.output_dir / f"{target.name}-{repetition}-normalized.json"
        for repetition in range(1, options.repetitions + 1)
    ]
    return aggregate(inputs, target.output_dir / f"{target.name}-aggregate.json")


if __name__ == "__main__":
    raise SystemExit(main())
