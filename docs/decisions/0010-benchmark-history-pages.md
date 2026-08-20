# ADR 0010: Publish a static benchmark dashboard over generated history data

- Status: superseded by ADR 0012
- Date: 2026-08-20

## Context

Performance measurements are only useful over time if the workload, software revision, broker images, resource limits, and measurement boundaries remain inspectable. Local JSON output and short-lived workflow artifacts do not provide a convenient historical view. The project needs a low-maintenance dashboard without adding a service, database, or runtime dependency to the broker.

## Decision

The original implementation ran the container comparison workflow on pushes to `main`, on a weekly schedule, and on demand. It normalized successful results into a provenance-bearing schema and appended them, together with `site/data.json`, to a generated `benchmark-history` branch. It also generated dashboard code in a separate `benchmark-pages` branch. The dashboard still fetches public history data at runtime; ADR 0012 replaces the generated dashboard branch with source assets in `main:/docs/benchmarks`.

The history branch and site are derived output. The benchmark scripts, result schema, and workflow are the sources of truth. The dashboard must preserve separate series for different operations, payload sizes, benchmark profiles, and measurement boundaries. It must not imply that Kafka/Redpanda fetch throughput is equivalent to a consumer path with application acknowledgement.

Use workflow concurrency so history updates are serialized. The benchmark workflow needs repository-content write access but no Pages deployment permissions or Pages environment. Do not make benchmark measurements a correctness-CI gate.

## Consequences

- Every successful automatic run has a durable, reviewable machine-readable history record and a human-readable graph.
- The main branch stays free of generated benchmark data and Pages output.
- The dashboard remains deployable without coupling every benchmark result to a Pages deployment job.
- A generated branch adds repository history and needs periodic retention or compaction if the measurement volume becomes large.
- Shared GitHub-hosted runners remain noisy; repeated reference runs and variance-aware reporting are required before performance gates are considered.
- The public data fetch depends on GitHub raw-content availability and may need cache-busting when history changes.
