# TD-020: Clustered delivery lease time models

- Status: exploratory research; no timing policy is accepted
- Last reviewed: 2026-09-28
- Repository baseline: `6ef4fc2ec2122d00806ac9d71a6cf783c5ac8574`
- Scope: compare the current clustered grouped-delivery expiry model with practical alternatives, focusing on clock changes, failover, recovery, redelivery, and stale-delivery fencing.
- Related records: [TD-020](../tech-debt.md#td-020-clustered-delivery-leases-use-absolute-wall-clock-deadlines), [ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md), [the shared-consumer delivery backlog item](../backlog.md#make-shared-consumer-delivery-dependable).

This note is evidence for a future design choice, not an accepted API, operational contract, or implementation plan. The code and tests remain the source of current behavior.

## Current Runnel behavior

Repository facts at the reviewed baseline:

- A clustered grouped poll samples Unix wall time and computes an absolute millisecond deadline before proposing the poll command. The command replicates both the sampled time and deadline. See [`now_ms` and `poll_group`](../../crates/runnel-raft/src/engine.rs) and the [`PollGroup` command](../../crates/runnel-raft/src/state_machine.rs).
- Replicas apply the same absolute deadline from the command. The state machine persists a per-data-group maximum of observed lease-command times (`lease_clock_ms`); poll and acknowledgement application compare expiry against the maximum of that floor and the command's sampled time. The floor is included in checkpoints and snapshots. See [`delivery.rs`](../../crates/runnel-raft/src/delivery.rs) and [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs).
- The floor prevents an observed clock value from moving backward after recovery or a leader change. It does not advance with elapsed time. Expiry is evaluated as part of a grouped poll or acknowledgement, so unrelated committed commands do not advance the lease floor or evaluate expiry. The timestamp is sampled before the leader proposes the command, so command delay can separate the sample from state-machine application time.
- When an expired assignment is removed, a later assignment gets a token based on its committed Raft log identity. Acknowledgement checks the current member and token. The token prevents a superseded delivery from acknowledging progress in Runnel; it cannot stop a stale worker from continuing an application-side effect after its lease has expired.
- Tests exercise deadline boundaries, forward jumps, fixed leader offsets, a backward successor clock and persisted floor, no-command expiry, restart/snapshot recovery, and stale-token rejection. Examples include [`grouped_lease_forward_jump_and_fixed_leader_offset_expire_early`](../../crates/runnel-raft/src/lib.rs), [`grouped_lease_successor_backward_offset_delays_expiry_until_floor_catches_up`](../../crates/runnel-raft/src/lib.rs), [`grouped_lease_has_no_lazy_expiry_without_a_committed_command`](../../crates/runnel-raft/src/lib.rs), and [`grouped_lease_survives_journal_restart_and_leader_change`](../../crates/runnel-raft/src/lib.rs).

The direct consequence is that the configured acknowledgement timeout is not currently a measured real-time bound. A successor clock ahead of the prior leader can make a lease eligible early; a backward clock can delay eligibility until it catches the persisted floor. A poll or acknowledgement command carries its leader-side time sample, so command application latency is not itself the time used for expiry. If an acknowledgement is sampled before expiry and no later lease-command timestamp has advanced the floor, it may still be accepted when applied after the wall-clock deadline. Even after the deadline, a grouped poll or acknowledgement is needed to observe expiry. These timing effects change when work may be assigned again, not the Raft commit order. At-least-once delivery permits duplicate application work, and broker token fencing is narrower than fencing a consumer's external side effects.

### Coverage gaps in current evidence

- Clock behavior is tested by deterministic command-time inputs, not by running nodes with controlled real clock offset, slew, suspend, or forward/backward steps.
- The tests do not quantify maximum early or late redelivery under an operational clock envelope, nor the effect of delayed command submission/application on the selected acknowledgement semantics.
- No test demonstrates the behavior of real consumer side effects that continue after a lease expires; current fencing evidence is about broker acknowledgement and durable progress.
- No measurement establishes the per-group cost of recurring replicated ticks or a clock-health/uncertainty service.

## Primary source findings

These are source facts; Runnel-specific implications are in the next section.

| Source | Relevant finding |
| --- | --- |
| [Raft paper](https://raft.github.io/raft.pdf), §5.6 and §8 | Raft safety is designed not to depend on timing, while availability depends on a stable leader. The paper notes that using a heartbeat-derived leader lease for linearizable reads would rely on bounded clock skew. |
| [Gray and Cheriton, “Leases” (SOSP 1989)](https://www.cs.cmu.edu/afs/cs.cmu.edu/academic/class/15712-s12/www/papers/gray89.pdf) | A lease is a time-limited grant. Their failure analysis uses physical clocks and considers clock drift bounds; the paper treats non-Byzantine failures as affecting performance when the lease protocol's timing assumptions hold. |
| [Rust `Instant` documentation](https://doc.rust-lang.org/std/time/struct.Instant.html) | `Instant` is an opaque, monotonically nondecreasing measurement useful for elapsed durations. It is not guaranteed steady; suspend behavior varies by platform, and it is only useful for comparing instants. |
| [Linux `clock_gettime(2)` documentation](https://www.man7.org/linux/man-pages/man3/clock_getres.3.html) | `CLOCK_MONOTONIC` does not move backward on discontinuous wall-clock changes but is adjusted by rate slewing and excludes suspend time; `CLOCK_BOOTTIME` includes suspend. Monotonic clocks are relative to an unspecified past point. |
| [Google Spanner paper](https://research.google.com/archive/spanner-osdi2012.pdf) and [Google Cloud TrueTime documentation](https://docs.cloud.google.com/spanner/docs/true-time-external-consistency) | TrueTime exposes time with uncertainty bounds. Spanner uses these bounds to obtain ordered timestamps; commit wait ensures the chosen timestamp has passed real time. This is a system-wide clock service and protocol, not a property provided by ordinary Raft quorum replication. |
| [etcd API guarantees](https://etcd.io/docs/v3.5/learning/api_guarantees/) and [etcd failure behavior](https://etcd.io/docs/v3.7/op-guide/failures/) | etcd describes TTL expiry in wall-clock terms and explicitly warns that distributed lock users must account for physical-time behavior. Its documented leader-failure behavior extends lease timeouts after election so an old leader's lease does not expire before its granted TTL. This is one product's conservative failover policy, not a universal guarantee. |
| [Kulkarni et al., “Logical Physical Clocks”](https://cse.buffalo.edu/~demirbas/publications/hlc.pdf) | Hybrid logical clocks combine a logical component with physical time to capture causality while remaining close to physical time. The paper is about ordering and causality; it does not supply a hard real-time uncertainty bound for lease expiry. |

## Alternatives and tradeoffs

| Model | What it can improve | Costs and remaining assumptions |
| --- | --- | --- |
| Keep absolute wall-clock deadlines and define an operational clock envelope | Preserves deterministic replicated application, persistence, simple recovery, and current test model. Documenting allowed clock offset/rate error and monitoring node clocks could make expected timing behavior explicit. | A bound exists only if the deployment can enforce and observe it. Forward steps or unbounded skew still violate the envelope. The design must say whether early redelivery is permitted and what happens when clock health is unknown. |
| Leader-local monotonic timer | Avoids discontinuous wall-clock steps while one leader process owns the timer. Monotonic time is appropriate for measuring a local elapsed duration. | A monotonic instant is not a portable replicated timestamp. On failover or restart, the successor cannot compare its monotonic origin to the prior leader's. Restarting a full timeout at takeover avoids early expiry only by extending the original deadline by the pre-failure elapsed time and takeover delay; it then waits a fresh timeout after takeover. Preserving remaining time requires more replicated state and a rule for time spent without a leader. Suspend accounting also differs by clock. |
| Leader-managed expiry with replicated ticks or explicit expiry commands | Can make every state change deterministic and turn time passage into ordered group events, while avoiding cross-node comparison of a raw monotonic instant. An expiry is only effective after its event commits. | Raft orders events but does not create elapsed time. A leader timer still needs a time source; ticks need quorum commits and may lag during election, overload, or quorum loss. A per-group ticker adds recurring writes and recovery state; a shared ticker introduces another coordination boundary. Expiry becomes unavailable with quorum, consistent with the cluster's inability to commit writes. A maximum lateness claim needs explicit tick, election, and quorum assumptions. |
| Quorum-backed or uncertainty-bounded time | If an available time service exposes a real-time interval with a trustworthy error bound, expiry can be defined against the conservative end of that interval. This can support explicit early- and late-expiry bounds without comparing arbitrary node clocks. | Raft quorum alone does not provide bounded physical time. A TrueTime-like design needs clock-rate bounds, synchronization infrastructure, uncertainty monitoring, and a fail-closed policy. It adds operational dependencies and may add waiting or availability constraints. Google's implementation is not a drop-in option for a small self-hosted Runnel cluster. |
| Logical or hybrid logical timestamps | A replicated log index or HLC can order commands and preserve causality without depending solely on wall time for ordering. A logical epoch can also make ownership generations explicit. | Log indices advance with committed workload, not elapsed time, so an idle group never reaches a time deadline. HLC remains close to physical time by construction but does not make it a bounded time oracle. Either approach needs a physical-time/tick mechanism to represent elapsed acknowledgement timeout. |
| Conservative lease extension across leader election | The etcd documentation illustrates a product-level choice to preserve a lease's minimum promised life after leadership changes. This can reduce early reassignment at the cost of delaying recovery. | It does not remove all clock assumptions or establish a general maximum-lateness bound. Extending every in-flight Runnel delivery after election could delay recovery for every member; policy would need to define whether extension is whole-timeout, remaining-time, or bounded grace. |

All alternatives must retain a committed ownership transition and a new token (or equivalent generation) before a new member can acknowledge the reassigned delivery. A clock improvement alone does not fence work already running outside the broker. Consumers with non-idempotent external effects still need idempotency or a downstream fencing mechanism if overlapping processing after timeout is unsafe.

## What matters for Runnel

**Inference from sources and the inspected implementation:**

1. Keep consensus order and expiry time as separate concepts. The Raft safety proof does not make the wall-clock deadline accurate, and adding time to a replicated command does not make node clocks comparable.
2. Specify the timeout's direction and error budget before changing the clock model. A timeout might be a minimum hold period before redelivery, a target delay with bounded early/late error, or a best-effort delay. These are materially different semantics. The current code has no documented maximum early or late error.
3. Preserve demand-driven evaluation unless a new policy intentionally adds background work. A timer/tick approach must account for quorum unavailability, leader election, idle groups, resource use, and duplicate work when it fires late.
4. Keep token fencing even if time becomes more accurate. Timing decides when reassignment is eligible; token/generation validation decides whether an old acknowledgement can advance broker progress. Neither prevents an old worker from performing an external side effect.
5. Separate the wall-clock timestamp used for message metadata from elapsed timeout measurement if a future refactor introduces a clock abstraction. The current `now_ms()` helper also supplies publish timestamps; treating it as an elapsed-time abstraction without distinguishing those semantics would be misleading.

**Hypotheses to test if implementation is proposed:**

- A documented, bounded wall-clock assumption may be the lowest-cost path for the current static three-node cluster if product requirements accept timeout error within a stated envelope and operators can observe violations.
- Conservative re-establishment after leader election could avoid early expiry with less infrastructure than bounded global time, but may make failover redelivery noticeably late. The correct choice depends on the still-unstated balance between early duplicate work and late recovery.
- Replicated ticks can provide a deterministic expiry event but may cost more per group than the current model and cannot progress during quorum loss. A shared timer service could reduce writes but would create cross-group coupling.
- A future sink-fencing contract may be more important than tighter lease timing for applications that cannot tolerate concurrent external effects; that outcome is outside the current broker-only acknowledgement token.

## Disposition and evidence still needed

Keep TD-020 open and use this note to inform a design decision before any runtime change. The research does not establish that Runnel needs a TrueTime-like service, replicated ticking, or a new public configuration. The near-term missing input is a product-level timing objective: acceptable early-expiry risk, acceptable late-redelivery delay, expected clock-health failure behavior, and whether duplicate application side effects are expected to be handled by consumers. The existing [shared-consumer backlog item](../backlog.md#make-shared-consumer-delivery-dependable) already covers expiry, failover, durable progress, and stale-token rejection; a duplicate backlog item is not warranted until those timing guarantees are selected.

Any implementation proposal should then compare the selected model against the current one, define a measurable bound or explicitly state best-effort behavior, test forward/backward steps, per-node offsets, restart and leadership change, no-command/quorum-loss periods, stale-token rejection, and at-least-once recovery, and measure the cost of any recurring replicated work. No runtime policy or ADR is accepted by this research note.
