# ADR 0017: Separate optimization and competitor benchmark cadences

- Status: accepted
- Date: 2026-08-24

## Decision

Runnel performance evidence is maintained as three distinct benchmark suites:

1. A short Runnel-only pull-request benchmark produces a machine-readable artifact and an informative pull-request summary. It is not a merge gate initially.
2. A longer Runnel-only benchmark runs daily and on changes to `main`. Its repeated results form the primary history for evaluating Runnel optimizations.
3. A competitor comparison runs weekly and manually, using the documented native and replicated comparison workloads. It is stored as a separate history series and is not used to decide whether an internal Runnel optimization worked.

The dashboard may contain more Runnel points than competitor points. It must keep suites, workload identity, resource limits, and measurement boundaries distinct, and comparisons must only be made between compatible points.

## Rationale

Runnel-only runs provide the most sensitive feedback for changes to Runnel because they can use the same client, protocol, durability mode, workload, and resource budget over time. Competitor runs are more expensive and their available clients and acknowledgement boundaries are not yet fully equivalent, so running them at the same cadence would add cost and noise without improving internal optimization decisions.

Pull-request results are useful for catching large regressions quickly, but host scheduling and storage variance make them unsuitable as a required gate until repeated-run variance and regression thresholds are understood.

## Consequences

- The Runnel history should be interpreted as the primary optimization trend.
- Competitor history is a lower-frequency release and positioning signal.
- The PR workflow must not grant write-capable credentials to untrusted pull-request code merely to publish a comment.
- Future performance gates require a separate decision based on observed variance and should not be inferred from this cadence decision.
