#!/usr/bin/env python3
"""Retry the authoritative local benchmark until it produces stable evidence."""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
PR_LOCAL = SCRIPT_DIR / "pr_local.py"
DEFAULT_MAX_ATTEMPTS = 3
sys.path.insert(0, str(SCRIPT_DIR))

from pr_local import INCONCLUSIVE_EXIT_CODE  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=(
            "maximum complete benchmark runs, including inconclusive attempts "
            f"(default: {DEFAULT_MAX_ATTEMPTS})"
        ),
    )
    arguments = sys.argv[1:] if argv is None else argv
    arguments = [argument for argument in arguments if argument != "--"]
    args, benchmark_args = parser.parse_known_args(arguments)
    if args.max_attempts <= 0:
        parser.error("max attempts must be positive")
    if any(argument == "--allow-inconclusive" for argument in benchmark_args):
        parser.error("--allow-inconclusive is incompatible with until-stable")
    if any(
        argument == "--repetitions" or argument.startswith("--repetitions=")
        for argument in benchmark_args
    ):
        parser.error("--repetitions disables stability checks; use --max-repetitions instead")
    if any(
        argument == "--output" or argument.startswith("--output=")
        for argument in benchmark_args
    ):
        parser.error("--output is incompatible with until-stable; keep each attempt report")
    return args, benchmark_args


def run_until_stable(
    benchmark_args: Sequence[str],
    max_attempts: int,
    runner: Callable[..., subprocess.CompletedProcess[object]] = subprocess.run,
) -> int:
    command = [sys.executable, str(PR_LOCAL), *benchmark_args]
    for attempt in range(1, max_attempts + 1):
        print(f"Authoritative benchmark attempt {attempt}/{max_attempts}", flush=True)
        result = runner(command, cwd=ROOT, check=False)
        if result.returncode == 0:
            print(f"Stable benchmark evidence obtained on attempt {attempt}.")
            return 0
        if result.returncode != INCONCLUSIVE_EXIT_CODE:
            print(
                "Benchmark stopped after a non-inconclusive failure; "
                f"exit code {result.returncode}.",
                file=sys.stderr,
            )
            return result.returncode
        if attempt < max_attempts:
            print(
                "Benchmark attempt was inconclusive; starting another complete "
                "controlled run.",
                file=sys.stderr,
            )

    print(
        f"Benchmark remained inconclusive after {max_attempts} complete attempts; "
        "inspect every report before retrying with a larger --max-attempts budget.",
        file=sys.stderr,
    )
    return INCONCLUSIVE_EXIT_CODE


def main(argv: Sequence[str] | None = None) -> int:
    args, benchmark_args = parse_args(argv)
    return run_until_stable(benchmark_args, args.max_attempts)


if __name__ == "__main__":
    raise SystemExit(main())
