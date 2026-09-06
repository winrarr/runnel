# TD-020 bounded lease-clock floor

- Status: implemented bounded slice; TD-020 remains open
- Last reviewed: 2026-09-06
- Baseline: `3d87e9316a2d6255e5660535ee340e76f3ed679a`

This note records the current containment for clustered delivery leases. It is
an unsettled design note, not an ADR or a claim that the clustered backend has
clock-independent timeout semantics. The observations below are tied to the
baseline above; later code or tests may change them.

## Observed baseline

Each stream has its own replicated Raft data group. The grouped poll and
acknowledgement paths are leader-authorized writes to that stream's data group;
requests received by another node are forwarded to the current leader. The
leader samples its local `SystemTime` as non-negative milliseconds since the
Unix epoch. A grouped poll carries that observation and an absolute deadline
formed by adding the leader's local acknowledgement timeout. A grouped
acknowledgement carries an observation but does not rewrite the existing
deadline. See the [leader operation construction](../../crates/runnel-raft/src/engine.rs)
and [data-group delivery state machine](../../crates/runnel-raft/src/delivery.rs).

The floor is held independently in each stream data group's state machine. It
is advanced only when a valid grouped poll or grouped acknowledgement for that
group is applied; ordinary publish, replay, non-grouped poll/ack, metadata
operations, and commands for another stream do not advance it. The production
group manager opens one state machine per data group. Some unit fixtures put
multiple streams in one in-memory state object to exercise evaluator logic;
that is not a cross-stream production behavior.

For an applied grouped command, expiry uses the greatest observation committed
so far in that data group:

```text
effective_now = max(lease_clock_ms, command.now_ms)
expired = deadline_ms <= effective_now
```

The inclusive comparison is intentional: an observation exactly at the stored
deadline expires the delivery. A floor update is part of the same state-machine
apply as the command, so replicas and journal replay receive the same command
observation and make the same expiry decision. The floor does not expose a
public timestamp or token, and it does not change stored absolute deadlines or
delivery-token format.

## Current supported boundary

The current implementation supports the following narrow properties:

- A backward observation on a surviving or recovered replica cannot make
  evaluation move below the greatest observation already committed in that
  data group. If a deadline is already at or below that floor, the next valid
  grouped command expires it even when its submitted observation is smaller.
- A committed forward observation can expire an active delivery early relative
  to elapsed real time. A successor with a wall clock ahead of its predecessor
  can therefore reclaim a delivery before the configured timeout would have
  elapsed on the predecessor.
- A delivery that is still unexpired remains assigned to its current member,
  including when a client retries a poll after losing its response. Once a
  command evaluates it as expired, reassignment increments the persisted
  attempt and creates a new token; an old token remains fenced.
- The floor and in-flight delivery state survive the current checkpoint,
  journal-replay, snapshot, process-restart, and tested follower/leader
  recovery paths. Current checkpoint and snapshot readers accept versions 1 and
  2; the floor is absent from legacy version-1 data and defaults to zero, while
  current writers emit version 2. This is read-forward behavior for tested
  artifacts, not a rolling-upgrade guarantee.
- Expiry is demand-driven. There is no timer or background command that
  advances the floor or reclaims a delivery. If no leader can commit a grouped
  poll or acknowledgement, an otherwise expired delivery remains in replicated
  in-flight state until a later valid command can evaluate it.

These are safety and state-transition properties, not a bound on redelivery
delay, a service-level objective, or a guarantee about elapsed wall-clock time.

## Configuration and failure boundary

`--ack-timeout-ms` is supplied independently to each broker process (default
30,000 ms) and is not part of replicated state or cluster admission. The
current code accepts zero, so a new delivery can have a deadline equal to its
creation observation and becomes eligible on the next command at that floor.
The timeout used for a new delivery is the current leader's value. A leader
change with different node configuration does not rewrite an existing
deadline, but newly assigned or redelivered work can use a different timeout.
Operators must therefore configure the value consistently across nodes if they
want one cluster-wide retry-delay interpretation; the broker does not enforce
that equality.

The clock path also has deliberate limits:

- Clock observations are local physical-clock readings, truncated to
  millisecond Unix-epoch values. A clock before the Unix epoch is mapped to
  zero by the current helper; there is no startup clock-health validation or
  diagnostic for this condition.
- Deadline addition is saturating at the `u64` representation boundary. This
  avoids wraparound but is not a meaningful real-time guarantee near that
  boundary.
- A forward jump, a fixed positive offset on a successor, delayed command
  application, or a changed timeout can cause early or late redelivery. The
  floor only prevents an already observed timestamp from moving backward; it
  cannot infer time elapsed while a group has no committed command.
- If a leader fails before a grouped command commits, that command has not
  changed delivery state. If it commits and the response is lost, the state
  machine retains the assignment and its deadline; a later command may return
  the same assignment, expire it, or fence its old acknowledgement according
  to the rules above. The client-facing operation can still be ambiguous at
  the transport boundary.
- The floor is not a cluster-wide clock and does not coordinate unrelated
  stream groups. A no-quorum interval and a stopped process do not advance it.

## Deliberate non-claims

TD-020 does not currently claim:

- that the configured acknowledgement timeout is an upper or lower bound on
  elapsed real time before redelivery;
- that successive leaders have bounded clock skew or that a deployment has
  synchronized clocks;
- that leases expire while a group is idle or has no leader/quorum;
- that a mixed-version cluster can safely exchange commands, snapshots, or
  state-machine files; or
- that the floor solves forward jumps, positive/negative inter-node offsets,
  pause or scheduling delays, process suspension, or operator timeout drift.

The state-machine version-1 compatibility tests prove that legacy state and
snapshots remain readable, but no test establishes a rolling upgrade in which
an older writer reads a current artifact containing `lease_clock_ms`. Peer
frames also have no explicit version-negotiation or lease-clock capability
level. Any release policy must be decided separately from this additive
read-forward field.

## Alternatives and recommendation

The floor is retained as a bounded containment because it preserves the current
command and public response model while preventing backward evaluation after a
replica or leader change. It does not solve the underlying physical-time
assumption.

Future options are intentionally described as outcomes and mechanisms rather
than implementation requirements:

- A leader-owned monotonic eligibility authority could record a fenced reclaim
  decision in replicated state, removing cross-node wall-clock comparison from
  ownership eligibility. It would need an explicit policy for leader changes,
  no-quorum periods, process pauses, and bounded active-delivery state.
- A replicated logical progress signal could avoid physical clock offsets, but
  would need a policy for idle groups, progress-entry cost, timer delay,
  snapshots, and recovery.
- A wall-clock design could remain viable only with an accepted clock-health
  assumption, a measurable maximum error, behavior when that assumption is
  violated, and an operational way to observe compliance.
- A service-managed lease authority could provide a shared timing boundary,
  but would add a coordinator dependency and availability trade-off that are
  not justified by this early static-cluster slice.

TD-020 should remain open until one of those timing contracts is accepted and
verified under leader changes, clock skew/jumps, process pauses, and loss of
quorum, or until delivery eligibility no longer depends on comparable node
wall clocks while preserving at-least-once delivery and stale-token fencing.

## References and evidence

The following sources support the distinction between replicated ordering and
physical-time assumptions; they do not establish a production clock bound for
Runnel:

- [Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System*](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)
  separates logical ordering from physical-time assumptions and required
  bounds.
- [The Raft paper](https://raft.github.io/raft.pdf) establishes replicated
  ordering and discusses bounded clock skew for lease-style optimizations;
  leadership succession alone does not make wall-clock deadlines equivalent.
- Rust's [`SystemTime`](https://doc.rust-lang.org/std/time/struct.SystemTime.html)
  is non-monotonic, while [`Instant`](https://doc.rust-lang.org/std/time/struct.Instant.html)
  is process-local and cannot be persisted as a cross-node deadline.
- etcd's [failure guidance](https://etcd.io/docs/v3.6/op-guide/failures/)
  documents conservative lease-time extension after election, illustrating
  that failover timing is an explicit availability/timing policy rather than an
  automatic property of consensus.

## Verification and benchmark applicability

The focused state-machine tests cover future, equal, and past deadlines;
forward jumps and a fixed successor offset; backward observations and the
persisted floor; deadlines behind the floor; acknowledgement-time expiry and
stale-token fencing; no-command expiry; journal restart and leader change; and
snapshot round-trip. The [three-process clustered tests](../../crates/runnel-server/tests/cluster_smoke.rs)
cover real follower restart, leader/process failure, reassignment, durable
attempts, and stale-token rejection. They use the host clock and do not inject
skew or jumps.

The unit fixture that advances the floor through a second stream is useful for
state-machine arithmetic but is not evidence that one stream advances another
stream's floor. There is also no focused assertion that a version-1 artifact
with an omitted floor restores zero and then rebuilds the floor through a later
command, no mixed-timeout cluster test, no clock-health telemetry, and no
measurement of real-time redelivery error under controlled clock skew or
process suspension.

No performance benchmark applies to the current floor slice: it changes lease
bookkeeping and persistence semantics without changing the intended hot-path
allocation, lock, encoding, I/O, or scheduling design. A future reclaim or
timer mechanism must measure active lease cardinality, reclaim delay, no-quorum
behavior, and normal publish/poll/ack impact under controlled resources.
