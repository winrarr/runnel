# TD-020 bounded lease-clock floor

- Status: implemented bounded slice; TD-020 remains open
- Last reviewed: 2026-08-27
- Baseline: `1842ccac7c1209efdacf7360f792d71762bf9f6d`

This note records the smallest safe containment for clustered delivery leases. It is not an ADR or a claim that the clustered backend has clock-independent timeout semantics.

## Invariant

`PollGroup` and `AckGroup` commands already carry the leader's wall-clock observation, so applying a committed command remains deterministic across replicas and journal replay. Each data-group state machine now persists `lease_clock_ms`, the maximum observation seen in that state machine. Expiry uses that floor and the existing inclusive comparison:

```text
effective_now = max(lease_clock_ms, command.now_ms)
expired = deadline_ms <= effective_now
```

The floor is included in checkpoints and snapshots, defaults to zero for older persisted state, and is not exposed in the public protocol. Stored absolute deadlines and opaque delivery tokens are unchanged. A backward wall-clock step on a surviving or recovered replica therefore cannot move lease evaluation backwards or leave a deadline live solely because the new observation is smaller. A deadline created from a regressed leader clock is not extended or rewritten; if it is at or behind the replicated floor, the next command expires it. This preserves the existing eager-expiry safety direction and stale-delivery fencing.

## Deliberate boundary

The floor does not solve a forward clock jump, a fixed offset between successive leaders, or elapsed time during a no-leader/no-quorum interval. A later command with a larger observation can still expire a delivery early, and no command means no lazy expiry evaluation. The implementation therefore makes backward movement deterministic without claiming a bound on real-time error. TD-020 remains open until the system has an accepted timing assumption and recovery policy or replaces absolute wall-clock eligibility with an explicit leader-authorized reclaim mechanism.

## Alternatives considered

- A leader-local monotonic timer plus a replicated, token-fenced reclaim command would remove cross-node wall-clock comparison from eligibility, but requires scheduler lifecycle, leader-change re-arming, no-quorum behavior, command compatibility, and bounded active-delivery bookkeeping.
- Replicated logical ticks would avoid physical clock offsets but would add committed progress entries and a policy for idle groups, timer delay, snapshots, and recovery.
- A bounded wall-clock design could retain absolute deadlines only if deployment enforces clock-health and drift limits and the broker documents behavior when those limits are violated.
- A cluster lease authority, as used by systems with service-managed sessions or TTL state, would add a coordinator boundary that is not justified by this early static-cluster slice.

The floor is preferred for this change because it is additive at the persisted state boundary, keeps the existing command and public response model, and contains backward-step regressions without inventing an incomplete cross-node monotonic-clock abstraction.

## References and evidence

- [Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System*](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) separates logical ordering from physical-time assumptions and their required bounds.
- [The Raft paper](https://raft.github.io/raft.pdf) establishes replicated ordering and discusses bounded clock skew for lease-style optimizations; leadership succession alone does not make wall-clock deadlines equivalent.
- Rust's [`SystemTime`](https://doc.rust-lang.org/std/time/struct.SystemTime.html) is non-monotonic, while [`Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html) is process-local and cannot be persisted as a cross-node deadline.
- etcd's [failure guidance](https://etcd.io/docs/v3.6/op-guide/failures/) documents conservative lease-time extension after election, illustrating that failover timing is an explicit availability/timing policy rather than an automatic property of consensus.

These references support the boundary but do not establish a production clock-skew bound for Runnel. The unresolved risks are forward jumps, inter-node offsets, delayed command application, timer absence, and mixed-version rollout policy for the changed state interpretation.

## Verification and benchmark applicability

Focused state-machine tests cover future, equal, and past deadlines; snapshot round-trip of the clock floor followed by a lower observation; backward-time acknowledgement; and equal-deadline stale-token fencing. Existing restart and clustered delivery tests continue to cover durable attempts and token replacement. No performance benchmark applies: this changes lease bookkeeping and persistence semantics without changing a hot-path allocation, lock, encoding, I/O, or scheduling design. A future reclaim/timer implementation must measure active lease cardinality, reclaim delay, and normal publish/ack impact under controlled resources.
