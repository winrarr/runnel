# ADR 0020: Require stable evidence for optimization claims

- Status: accepted
- Date: 2026-08-25
- Supersedes: the pull-request acceptance criterion in [ADR 0017](0017-benchmark-cadence-and-evidence.md)

## Decision

Benchmark requirements are evaluated case by case.

A change whose goal is to improve throughput, latency, or tail latency, or a
change to a hot path with a plausible runtime effect, requires authoritative
same-host benchmark evidence before it is accepted as an optimization. The
canonical authoritative workflow is `just bench-pr-local`: it uses a fixed
workload and resource budget, compares paired current and default-branch runs,
and exits nonzero unless the configured repeatability thresholds are met. The
report and raw artifacts remain available when the result is inconclusive so
that noise or environmental problems can be investigated, but an inconclusive
result cannot support a performance claim or complete the optimization work.

The throughput and p99 range thresholds are descriptive statistical
repeatability requirements. They are not significance tests, confidence
intervals, or a probability that a result is caused by host noise. An
inconclusive result means that the raw range for at least one measured metric
and revision exceeded its configured limit by the maximum repetition count (or
that the minimum count was not reached). It does not establish that the change
is generally better, generally worse, or random. The report provides that
separate descriptive interpretation by comparing matched scenario medians and
counting improvements, regressions, and ties.

Reports include a Tukey 1.5×IQR outlier-sensitivity view. This is diagnostic:
paired samples are marked when either revision's sample is outside its
scenario's fence, but raw observations are retained and raw ranges remain the
authoritative stability requirement. Automatic filtering is deliberately not a
way to turn an inconclusive result into optimization evidence; with only three
to seven repetitions, a real workload mode can be mistaken for an outlier.

Changes that are reasonably performance-neutral use the normal correctness
verification path. They do not need the pull-request benchmark unless their
behavior plausibly affects runtime cost.

`just bench-pr-local-quick` and direct runs using `--allow-inconclusive` are
diagnostic workflows only. They may establish that a workload starts or help
investigate a hypothesis, but they are never optimization evidence. Competitor
benchmarks remain separate ranking and positioning evidence and do not replace
the current-versus-default Runnel measurement for an optimization claim.

## Rationale

The project uses explicit CPU and memory limits, host-level benchmark locking,
paired revisions, and repeated measurements to reduce noise. Those controls
make a stable result meaningful, but they cannot make an unstable result
meaningful. Previously, the helper returned success after reaching its maximum
repetitions even when throughput or p99 ranges remained too wide. That made an
inconclusive result too easy to mistake for completed performance work.

The policy is intentionally narrower than a universal benchmark gate. A
documentation, correctness-only, or otherwise performance-neutral change
should not be delayed by an unrelated noisy workload. Conversely, a change
that makes a performance claim must pay the cost of obtaining repeatable
evidence.

## Consequences

- Optimization work may require reruns or investigation of host, storage, or
  workload interference before it can be accepted.
- A failed authoritative benchmark is an actionable result, not evidence that
  the change is good or bad.
- Diagnostic runs remain available through an explicit, visible escape hatch,
  while the normal command is safe for use by any contributor or automation.
- The longer Runnel history and separate competitor suites retain their
  existing purposes; this decision governs evidence for optimization changes.
