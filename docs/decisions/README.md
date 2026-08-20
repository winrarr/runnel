# Architecture decisions

Accepted decisions live here as one file per decision. Supersede an old decision with a new record rather than rewriting history.

Current decisions:

- 0001-single-node-durable-log.md: initial storage and recovery shape.
- 0002-use-just-for-development-commands.md: canonical local workflow interface.
- 0003-cli-driven-local-smoke-test.md: canonical process-level local verification path.
- 0004-multi-raft-first-distributed-engine.md: first distributed engine and three-node topology.
- 0005-replicated-stream-metadata.md: historical stable stream identity decision; superseded by ADR 0006.
- 0006-separate-metadata-and-data-groups.md: separate metadata and per-stream data groups with reconciled creation.
- 0007-snapshot-based-replica-recovery.md: snapshot-based replacement replica recovery.
- 0008-container-benchmark-harness.md: resource-limited container benchmarks and comparable-broker adapter boundary.
- 0009-native-broker-comparison-baseline.md: pinned native-tool comparison as an explicitly provisional baseline.
- 0010-benchmark-history-pages.md: generated benchmark history and GitHub Pages dashboard.
- 0011-no-formal-rust-msrv.md: no formal source-build compiler guarantee.
- 0012-dashboard-source-in-main.md: hand-authored dashboard in `main:/docs/benchmarks` with generated history data kept separate.
