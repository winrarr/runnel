# ADR 0017: Separate optimization and competitor benchmark cadences

- Status: accepted
- Date: 2026-08-24

## Decision

Runnel performance evidence is maintained as three distinct benchmark suites:

1. A local Runnel-only pull-request comparison is the primary evidence workflow. `just bench-pr-local` compares the current revision with the default branch under a fixed Linux CPU/memory budget and repeats paired runs until stability thresholds are met or the result is reported inconclusive. It produces machine-readable artifacts and a Markdown summary for the pull request; it is not a merge gate.
2. A longer Runnel-only benchmark runs daily and on changes to `main`. Its repeated results form the primary history for evaluating Runnel optimizations.
3. A competitor comparison runs weekly and manually, using the documented native and replicated comparison workloads. It is stored as a separate history series and is not used to decide whether an internal Runnel optimization worked.

The dashboard may contain more Runnel points than competitor points. It must keep suites, workload identity, resource limits, and measurement boundaries distinct, and comparisons must only be made between compatible points.

## Rationale

Runnel-only runs provide the most sensitive feedback for changes to Runnel because they can use the same client, protocol, durability mode, workload, and resource budget over time. Competitor runs are more expensive and their available clients and acknowledgement boundaries are not yet fully equivalent, so running them at the same cadence would add cost and noise without improving internal optimization decisions.

Local pull-request results are useful for evaluating a change because both revisions share the host, workload, and explicit resource budget. Host scheduling and storage variance can still make a run inconclusive, so the helper reports repeatability rather than turning one favorable sample into a gate. Hosted pull-request benchmark jobs were removed because their runner variance did not provide useful optimization evidence.

## Consequences

- The Runnel history should be interpreted as the primary optimization trend.
- Competitor history is a lower-frequency release and positioning signal.
- Pull-request benchmark artifacts are produced by the developer or delegated worker locally and pasted into the review; no hosted PR workflow or write-capable benchmark-comment credential is required.
- Future performance gates require a separate decision based on observed variance and should not be inferred from this cadence decision.
