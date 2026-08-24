#!/usr/bin/env python3

from contextlib import redirect_stderr
from io import StringIO
import unittest

from check_conventional_commits import validate


class ConventionalCommitTests(unittest.TestCase):
    def test_accepts_common_subjects(self) -> None:
        subjects = [
            ("a" * 40, "feat(protocol): add binary payloads"),
            ("b" * 40, "fix(storage): reject corrupt frames"),
            ("c" * 40, "perf!: replace the provisional log format"),
            ("d" * 40, "docs: explain recovery"),
        ]

        with redirect_stderr(StringIO()):
            self.assertEqual(validate(subjects), 0)

    def test_rejects_subject_without_type(self) -> None:
        with redirect_stderr(StringIO()):
            self.assertEqual(validate([("a" * 40, "Add binary payloads")]), 1)


if __name__ == "__main__":
    unittest.main()
