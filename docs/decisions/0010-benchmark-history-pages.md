# ADR 0010: Publish benchmark history through a generated Pages branch

- Status: accepted
- Date: 2026-08-20

## Context

Performance measurements are only useful over time if the workload, software revision, broker images, resource limits, and measurement boundaries remain inspectable. Local JSON output and short-lived workflow artifacts do not provide a convenient historical view. The project needs a low-maintenance dashboard without adding a service, database, or runtime dependency to the broker.

## Decision

Run the existing container comparison workflow on pushes to `main`, on a weekly schedule, and on demand. Keep the raw result as a workflow artifact. Normalize successful results into a small provenance-bearing schema and append them to a generated `benchmark-history` branch. Build a dependency-free static site from that branch and deploy it through GitHub Pages.

The history branch and site are derived output. The benchmark scripts, result schema, and workflow are the sources of truth. The dashboard must preserve separate series for different operations, payload sizes, benchmark profiles, and measurement boundaries. It must not imply that Kafka/Redpanda fetch throughput is equivalent to a consumer path with application acknowledgement.

Use workflow concurrency so history updates are serialized. Use explicit Pages deployment permissions and the `github-pages` environment. Do not make benchmark measurements a correctness-CI gate.

## Consequences

- Every successful automatic run has a durable, reviewable machine-readable history record and a human-readable graph.
- The main branch stays free of generated benchmark data and Pages output.
- A generated branch adds repository history and needs periodic retention or compaction if the measurement volume becomes large.
- Shared GitHub-hosted runners remain noisy; repeated reference runs and variance-aware reporting are required before performance gates are considered.
- Publishing the Pages site requires enabling GitHub Actions as the repository's Pages source.
