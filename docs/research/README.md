# Research notes

This directory contains source-backed investigations that inform Runnel's
architecture and product choices. Research is evidence, not an accepted design:
observations, inferences, hypotheses, and unresolved questions should remain
clearly separated.

Each substantial note should record:

- its scope, status, and last-review date;
- the behavior, measurements, standards, papers, or other primary sources it
  examined;
- the conclusions that follow directly from those sources;
- inferences and recommendations, marked as such;
- open questions and the evidence needed to resolve them.

Use the documentation categories this way:

- `docs/research/` records external evidence, competitor comparisons, and
  measured investigations;
- `docs/design/` turns relevant evidence into an unsettled architecture or
  implementation proposal;
- `docs/decisions/` records an accepted choice and its durable rationale;
- `docs/backlog.md` records unfinished product outcomes;
- `docs/tech-debt.md` records known shortcomings intentionally left in the
  implementation.

Research notes should link to the design proposals and decisions they inform.
Do not treat a research recommendation as a compatibility promise until an ADR
accepts it and the relevant implementation and verification work is complete.

## Current research

- [Distributed architecture options](distributed-architecture-options.md)
  compares Multi-Raft, sequenced quorum/copysets, chain replication, and other
  multi-node approaches against Runnel's workload profiles.
- [Message encoding and compression](message-encoding-and-compression.md)
  compares representation and compression choices, competitor behavior, and
  research findings relevant to Runnel's latency, throughput, and storage goals.
- [Raft follower recovery and replacement](raft-recovery-and-replacement.md)
  records the evidence and open design questions around crashed, stale, and
  empty replicas in the early clustered backend.
- [TD-012 peer transport ownership](td-012-peer-transport-ownership.md)
  records the OpenRaft network lifecycle constraints, scoped ownership choice,
  alternatives, and unresolved pooling questions.
