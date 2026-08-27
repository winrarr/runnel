# ADR 0021: Scheduled benchmark history

- Status: accepted; supersedes the history cadence in [ADR 0017](0017-benchmark-cadence-and-evidence.md)
- Date: 2026-08-27

## Decision

Run the longer Runnel-only benchmark history suite daily and manually, not on every push to `main`. Keep same-host pull-request comparisons as the primary evidence for performance changes. Competitor comparisons remain weekly and manual.

## Rationale

Most merges do not change a runtime path, so running the full history suite for documentation, design, and correctness changes adds queue time without adding useful trend data. The suite takes roughly 11–20 minutes on the hosted runner, and its non-cancelling concurrency group serializes runs after a burst of merges. A daily cadence preserves long-term trend coverage while the local pull-request workflow handles change-specific performance evidence.

## Consequences

- A main-branch history point represents the scheduled or manually selected revision rather than every merge.
- Manual runs remain available after a significant performance change or when investigating a suspected trend.
- The benchmark history is a trend signal, not a pull-request merge gate.
