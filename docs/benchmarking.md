# Benchmarking and performance evidence

This document is the shared policy for deciding when to benchmark, interpreting benchmark results, and reporting performance evidence. The `justfile` and benchmark scripts define executable behavior; [scripts/benchmarks/README.md](../scripts/benchmarks/README.md) documents workload and harness details.

## When to benchmark

Benchmark a change when its goal is to improve throughput, latency, or tail latency, or when it changes a hot path with a plausible runtime effect. Performance-neutral documentation, tests, and configuration changes do not need the pull-request comparison unless they plausibly change runtime cost.

Before running a benchmark, state the expected effects and non-effects: the behavior, runtime path, workload, and resource dimensions that should change or remain unchanged. Separate correctness, recovery, resource, operational, and test improvements from performance claims. A benchmark is useful only when it exercises the changed path; record any workload mismatch or coverage gap.

## Authoritative local comparison

Run the same-host comparison after committing a performance-sensitive change:

```text
just bench-pr-local
```

It compares the current revision with `origin/main` using the same three-node workload, alternated paired runs, and a fixed Linux systemd user scope. The default budget is 2 CPUs and 2 GiB shared across the benchmark client and broker nodes. It runs at least three and at most seven pairs, stopping early when every throughput range is at most 10% and every p99 range is at most 20% for both revisions. It writes raw results, logs, and a Markdown report under `benchmark-results/pr-local/` and exits nonzero when the result is not stable.

When a complete run is inconclusive because the host remains noisy, retry the complete authoritative workflow while retaining every attempt:

```text
just bench-pr-local-until-stable
```

This command holds the exclusive benchmark lock and stops at the first stable result, a hard failure, or its explicit maximum-attempt budget. It rejects diagnostic and fixed-repetition options. `just bench-pr-local-quick` and fixed-repetition runs with `--allow-inconclusive` are diagnostics only and cannot support an optimization claim.

Other benchmark workflows provide complementary evidence: `just bench` covers local Criterion paths, `just bench-container` covers the resource-limited single-node container path, `just bench-cluster` covers the real clustered workload with native broker processes, `just bench-cluster-container` runs the same clustered workload with one bounded container per broker, and `just bench-compare` and `just bench-compare-cluster` provide separate native-tool comparison series. Their workload, durability, resource, and measurement boundaries must remain attached to each result. Use the exclusive benchmark lock for authoritative measurements and `just isolated <workflow>` for concurrent process-heavy diagnostics.

## Interpreting results

The throughput and p99 range limits are descriptive repeatability requirements. They are not significance tests, confidence intervals, or estimates of the probability that host noise caused a result.

An inconclusive report means that at least one raw metric range for one revision exceeded its configured limit by the maximum repetition count, or that the minimum repetition count was not reached. It does not by itself mean that the change generally improved, generally worsened, or behaved randomly. The report must identify the failed range and separately summarize matched scenario medians as improved, worsened, tied, or mixed when samples are available.

The report may include a Tukey 1.5×IQR outlier-sensitivity view. This is diagnostic only: retain every raw observation, keep raw ranges authoritative, and never select a filtered result because it is favorable. Small samples make automatic outlier removal especially fragile because a legitimate workload mode can look like host noise.

Distinguish a stable improvement from noise, an inconclusive result, a blocked run, and a regression. A stable result supports the stated evidence only for the measured workload and resources; it does not establish a universal performance property.

## Required handoff

Every feature or bug-fix handoff that could affect performance must include:

- expected effects and non-effects, including behavior, runtime paths, workloads, and resource dimensions;
- correctness, recovery, resource, operational, and test improvements separately from performance claims;
- whether each benchmark was required, whether it exercised the changed path, and its coverage gaps;
- the exact revision, `origin/main` baseline, workload, durability mode, resource limits, isolation, command, repetition count, stability thresholds, and result status;
- directional scenario medians, raw range failures, and outlier diagnostics when available;
- any blocked, noisy, inconclusive, or regressed result; and
- an evidence-based recommendation to merge, revise, rerun, or defer.

Workers must report these fields even when a benchmark was not required or could not run. An orchestrating agent must collect and relay the report from every delegated worker rather than silently dropping missing, blocked, or inconclusive evidence.

## History and comparisons

Repeated benchmark history is aggregated by median while retaining raw samples and observed ranges. Compare a history point only with a compatible suite, workload, resource budget, broker image, measurement boundary, and comparison mode. Native competitor results are engineering baselines with explicitly different client and acknowledgement semantics; they are not a final product ranking.

Raw benchmark artifacts use one version-2 envelope across single-node, clustered, comparison, and pull-request runs. The envelope carries run identity, full source and environment provenance, workload and resource limits, target semantics, scenario metrics, recovery metadata, and optional broker `/metrics` deltas. Normalized history is a deliberately smaller projection; retain the raw artifacts when diagnosing a result or making a performance claim.
