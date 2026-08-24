#!/usr/bin/env python3
"""Validate Conventional Commit subjects or a pull-request title."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys


SUBJECT_RE = re.compile(
    r"^(?P<type>[a-z][a-z0-9-]*)(?:\((?P<scope>[^()\r\n]+)\))?(?P<breaking>!)?: (?P<description>\S.*)$"
)


def git_subjects(commit_range: str) -> list[tuple[str, str]]:
    result = subprocess.run(
        ["git", "log", "--no-merges", "--format=%H%x00%s", commit_range],
        check=True,
        capture_output=True,
        text=True,
    )
    subjects: list[tuple[str, str]] = []
    for line in result.stdout.splitlines():
        commit, separator, subject = line.partition("\0")
        if separator:
            subjects.append((commit, subject))
    return subjects


def validate(subjects: list[tuple[str, str]]) -> int:
    failures = 0
    for identity, subject in subjects:
        if SUBJECT_RE.fullmatch(subject) is None:
            print(
                f"invalid Conventional Commit subject for {identity[:12]}: {subject!r}",
                file=sys.stderr,
            )
            print(
                "expected <type>[optional scope][!]: description, for example "
                "feat(protocol): add binary payloads",
                file=sys.stderr,
            )
            failures += 1
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--range", dest="commit_range", help="git revision range to inspect")
    source.add_argument("--title", help="pull-request title to inspect")
    args = parser.parse_args()

    if args.title is not None:
        subjects = [("pull-request", args.title)]
    else:
        subjects = git_subjects(args.commit_range)

    failures = validate(subjects)
    if failures:
        return 1

    if subjects:
        print(f"validated {len(subjects)} Conventional Commit subject(s)")
    else:
        print("no non-merge commits in the selected range")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
