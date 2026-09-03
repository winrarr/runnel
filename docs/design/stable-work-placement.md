# Stable internal work placement

- Status: exploratory design proposal
- Scope: stable scheduling units for large shared-consumer pools
- Related outcome: [Explore stable internal work placement](../backlog.md)
- Related implementation debt: [TD-016](../tech-debt.md)

This proposal investigates whether a shared consumer should keep selecting the
next eligible record on demand, or use a bounded set of stable internal work
units. It does not change the runtime, public protocol, wire compatibility, or
the current placement of streams and replicas. It is not an ADR and makes no
performance claim about the current or proposed implementation.

The recommended candidate for a future experiment is a fixed set of virtual
lanes whose ownership is assigned cooperatively to active members. A record's
ordering key maps to a lane, while the lane's owner is an internal scheduler
fact. The current demand-driven scheduler remains the default until a focused
comparison demonstrates a material benefit for a workload that matters to
Runnel. Direct key-to-member ownership and offset ranges remain alternatives,
not selected designs.

## Terminology and implementation boundary

This proposal uses *placement lane* for a bounded unit that decides which
shared-consumer work may be selected by which member. That is distinct from the
existing local `StorageLane`. The storage executor creates one weakly retained
lane per stream, gives it FIFO execution ownership, and bounds its waiter queue;
it serializes synchronous storage operations and does not assign records,
ordering keys, or shared-consumer members. It is therefore an execution and
backpressure mechanism, not an answer to TD-016. See [`StorageExecutor` and
`StorageLane`](../../crates/runnel-core/src/lib.rs#L111-L413).

The shared-consumer scheduler is the grouped delivery path: local
[`Broker::poll_group`](../../crates/runnel-core/src/lib.rs#L791-L909) and
clustered [`apply_group_poll`](../../crates/runnel-raft/src/lib.rs#L1564-L1739).
The public operation still supplies only a stream, consumer, and transient
member, and the response still contains a record, opaque delivery token, and
attempt number. A placement lane, owner, generation, and ready queue must stay
inside that operation's implementation boundary. In particular, a future
placement change must not repurpose the storage executor's per-stream lane or
make a lane identifier part of the protocol.

## Current evidence

The current local and clustered implementations establish the semantic baseline
that placement must preserve:

| Area | Current behavior | Consequence for stable placement |
| --- | --- | --- |
| Public model | The engine and provisional protocol expose a stream, durable consumer, transient member, record, acknowledgement, opaque token, and attempt. They do not expose a partition, lane, node, or assignment. See [`Engine::poll_group`](../../crates/runnel-engine/src/lib.rs#L216-L227) and [`Request::PollGroup`/`AckGroup`](../../crates/runnel-protocol/src/lib.rs#L99-L126). | A future scheduler may add internal state, but no response or required client operation may expose its units or owners. |
| Selection | Both paths first return an existing unexpired delivery for the requesting member. They then select from records at or after the committed offset, skipping acknowledged offsets, all in-flight offsets, and a record whose key is already in flight for the consumer. The local warm path scans its bounded tail index and its cold path scans the log from a sparse checkpoint; the cluster scans the materialized message vector. | Stable routing must be an eligibility filter or indexed candidate path, not a second acknowledgement model. It must preserve the current empty, retry, and out-of-order acknowledgement meanings. |
| Concurrency | The first slice allows one outstanding delivery per member: local state indexes one offset per member and clustered polls find the member's existing delivery before selecting another. Unrelated keys can progress concurrently across members; a key is not delivered concurrently within one shared consumer. | Placement cannot claim batching or higher parallelism while this limit remains. A first experiment must isolate routing effects before adding read-ahead or multi-delivery credits. |
| Membership | A member is observed only through grouped poll and acknowledgement requests. There is no durable member registry, assignment map, explicit join protocol, or public graceful-leave operation. | Stable ownership needs an authoritative bounded member lease/lifecycle. The exact renewal, duplicate-identity, and graceful-leave behavior is unresolved and must not be smuggled into the public placement model. |
| Durability | Local consumer progress and delivery attempts are appended and synced in the consumer-state journal; active delivery ownership and deadlines are process-local. Clustered progress, attempts, in-flight ownership, lease deadlines, and tokens are fields of the replicated stream-data-group state. | A local prototype may use the existing recovery boundary, but a clustered version must replicate placement epoch and handoff state with the authoritative consumer state. |
| Fencing | The public grouped acknowledgement carries the member and opaque delivery token. Both current engines require the current member and compare the token when it is non-empty; the legacy `ack` path intentionally passes an empty token with `member == consumer`, and the core does not reject an empty token for a matching member. A token-bearing expired or replaced assignment is stale. | Every ownership transition must advance a durable or committed epoch/generation before a new owner can acknowledge. A delayed old request must fail closed as stale. |
| State bounds | Local record and sparse indexes are bounded, and its consumer-state cache and journal have fixed bounds, but grouped in-flight/attempt entries have no configured member-count bound because arbitrary member names are accepted. Clustered retained messages are materialized in a `Vec`, and grouped maps likewise have no placement-specific capacity. | A placement design must use a fixed lane/unit budget and must not create one durable owner entry, timer, or metric label per key or record. It must also state how member churn is bounded. |
| Recovery | A process or leader failure can cause an unacknowledged delivery to be redelivered after its lease boundary. The public model does not promise exactly-once processing. | Handoff must permit duplicate delivery after failure while protecting acknowledged progress and rejecting stale acknowledgements. |

### Exact current control flow and cost model

The two engines share the eligibility rule but not the same cost or expiry
boundary:

1. **Local warm delivery.** `Broker::poll_group` validates names, takes the
   stream mutex, expires due deliveries through the ordered deadline index,
   returns the same member's existing delivery if present, loads the durable
   consumer state (using a bounded best-effort cache), and calls
   `StreamLog::find_candidate`. When the committed offset is in the retained
   tail, candidate work is a linear scan of that tail; when it is older than
   the tail, the implementation scans the log from the nearest sparse
   checkpoint. The selected record's payload is read from the log, the
   delivery-attempt event is synced, and only then is the volatile in-flight
   entry returned. See [`StreamLog::find_candidate`](../../crates/runnel-core/src/lib.rs#L1591-L1625),
   [`record_is_candidate`](../../crates/runnel-core/src/lib.rs#L2041-L2055), and
   [`poll_group`](../../crates/runnel-core/src/lib.rs#L791-L909).
2. **Clustered committed delivery.** `apply_group_poll` runs as one committed
   state-machine command. It advances the replicated lease-clock floor,
   removes expired entries by iterating the consumer's in-flight map, returns
   that member's existing delivery when present, and otherwise linearly scans
   `StreamState::messages` from the committed offset until the same eligibility
   rule succeeds. It records the attempt and `GroupDelivery` in the replicated
   state; the token is derived from the assignment command's committed Raft
   log identity. See [`GroupConsumerState`](../../crates/runnel-raft/src/lib.rs#L219-L237)
   and [`apply_group_poll`](../../crates/runnel-raft/src/lib.rs#L1564-L1739).
3. **Acknowledgement boundary.** Local `ack_group` persists the acknowledgement
   event before updating the materialized checkpoint and removing in-flight
   state. It does not independently expire a deadline; an expired local
   delivery becomes redeliverable when a later local poll expires it. Clustered
   `apply_group_ack` evaluates the lease clock and removes expired entries before
   validating the acknowledgement, so a token-bearing acknowledgement after the
   deadline is stale there. Both implementations retain a tokenless compatibility
   bypass for the legacy `member == consumer` path. This is a current
   implementation difference, not a new guarantee. A future placement experiment
   must either preserve the documented common contract with focused tests or
   deliberately resolve this boundary in a separate compatibility decision.

The scan cost is therefore not one number. For the local warm path it depends
on the retained tail and blocked candidates; for local cold replay it also
depends on the sparse-checkpoint distance and bytes read; for the cluster it
depends on retained materialized messages and active in-flight entries. The
placement comparison must measure candidate records examined and expiry work,
not infer either from end-to-end latency alone.

### Eligibility and member-routing constraint

The candidate path must keep the current eligibility predicate as a common
first step:

1. start at the consumer's committed offset;
2. skip an acknowledged offset;
3. skip every offset already in flight for that consumer;
4. skip a keyed record when the same key is already in flight; and
5. allow a keyless record through this key gate, as today.

Placement may then reject a candidate whose lane is not eligible for the
requesting member, but it must not remove any of these checks or turn an
out-of-order acknowledgement into a contiguous-only cursor. The existing
one-delivery-per-member rule must also remain independent of lane count.

There is a second design constraint that is easy to miss. The current API has
no join, assignment, redirect, or long-poll operation: a member simply submits
`poll_group` with a string identity. A strict owner-only lane map therefore has
three possible behaviors, each with a different product consequence:

| Routing behavior | Benefit | Cost or compatibility question |
| --- | --- | --- |
| Only the computed owner may poll its lanes | Strong owner-local queues and cache locality. | A member can receive `empty` while another member owns ready work; the client cannot discover which member to poll. This may be a behavioral change even though no lane ID is exposed. |
| Any member may claim any ready lane | Preserves the current “any member can request eligible work” behavior. | Ownership becomes demand-driven, so stable lane locality is weaker and a fast member can consume another member's lanes. |
| Poll registers/renews a member and an internal coordinator routes lanes | Keeps lane ownership hidden and can preserve owner locality. | Requires bounded member lifecycle, duplicate identity fencing, deterministic routing, and a durable/committed handoff boundary that the current API does not define. |

The design should not select strict owner-only polling merely because the lane
map is internal. Before implementation, define whether `empty` means “no work
for this member” or “no eligible work for the consumer,” and test the choice
with a client that polls members in round-robin order. A bounded internal
router is the most promising way to keep the public API unchanged, but it is a
separate state-machine problem from hashing keys to lanes.

The current demand-driven scan is useful for a small pool because it naturally
lets a fast member request more work and does not require a rebalance. Its costs
are also explicit: each new member competes for the same eligible record scan,
there is no owner-local ready queue, and one-delivery-per-member limits the
amount of work that can be prefetched or processed in a batch. Stable placement
could reduce selection and state-cache churn, but it can also pin work to a
slow member or a hot key. Those are workload hypotheses, not measured results.

## Invariants and non-goals

Any future scheduler must preserve these invariants:

- At-least-once delivery and the existing durable acknowledgement point remain
  unchanged.
- Acknowledged progress never moves backward. Out-of-order acknowledgements
  remain durable and do not make an earlier record eligible for deletion or
  bypass its key-ordering gate.
- Records with the same ordering key are assigned to one active owner at a
  time. A handoff waits for the old owner's in-flight work to finish or expire,
  or fences it and explicitly permits redelivery; it never silently permits
  concurrent old and new ownership.
- A stale member, leader, or node cannot acknowledge after a placement epoch or
  delivery token has been superseded.
- Lane count, active-member state, in-flight deliveries, handoff work, and
  scheduler queues have explicit bounds. A large retained stream or a large
  number of distinct keys must not create unbounded placement metadata.
- A slow lane cannot prevent unrelated lanes from making progress. If a policy
  chooses to keep a slow member's lanes assigned until lease expiry, that
  blocked interval and its effect on lag must be observable.
- Placement is an internal optimization. Existing stream and consumer intent,
  offsets, keys, acknowledgement outcomes, and retry behavior remain the only
  application-facing concepts.

This exploration does not select a public partition count, require clients to
pin themselves to a member, promise global FIFO, split a single hot key, or
make placement a substitute for stream replication and storage placement.
Strict ordering for one hot key remains a single ordering-domain limit.

## Reference designs and research

These systems solve related problems with different public contracts. Their
behavior is evidence for tradeoffs, not a compatibility target for Runnel.

| Reference | Relevant evidence | Difference that matters to Runnel |
| --- | --- | --- |
| [Kafka incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A%2BKafka%2BConsumer%2BIncremental%2BRebalance%2BProtocol) and [consumer rebalance protocol](https://kafka.apache.org/42/operations/consumer-rebalance-protocol/) | Cooperative assignment lets members retain owned units while a following rebalance completes revocation. The protocol is intended to reduce stop-the-world pauses and unnecessary state movement. | Kafka's partitions are an explicit unit in its consumer model, while Runnel must keep units private. The two-step revoke-then-assign discipline is directly relevant to safe lane handoff. |
| [Kafka static membership, KIP-345](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A%2BIntroduce%2Bstatic%2Bmembership%2Bprotocol%2Bto%2Breduce%2Bconsumer%2Brebalances) | A stable member identity avoids treating every restart as a new group member, reducing rebalances for stateful consumers. The proposal also makes the tradeoff explicit: prioritizing state persistence over liveness means an absent member can retain work until its timeout. | Runnel's current member names are request parameters, not a durable membership protocol. Stable identity, duplicate identity fencing, and removal timeouts would need a deliberate lifecycle boundary rather than implicit assumptions. |
| [Pulsar Key_Shared subscriptions](https://pulsar.apache.org/docs/next/concepts-messaging/) | Key hashes map to consumers through auto-split hash ranges, consistent hashing, or sticky ranges. A key may move when consumers change, but new delivery waits for the previous key's unacknowledged work to be acknowledged or the old consumer to disconnect. Pulsar also exposes blocked-key statistics for diagnosing draining hashes. | This is close to the key-affinity problem, but its mapping and consumer modes are visible in the Pulsar subscription API. Runnel should borrow the drain/fence behavior while keeping keys and lanes out of its public protocol. A single hot key remains serial in either system. |
| [NATS JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers) | A worker can fetch up to a bounded number of messages and choose a timeout; larger batches reduce round trips, while a timeout bounds quiet-stream latency. The consumer remains pull-driven rather than requiring stable key ownership. | This is a useful control comparison: Runnel may obtain much of the throughput benefit from bounded pull batching or credits before taking on placement and handoff state. Its acknowledgement and ordering semantics are not identical to Runnel's and require an equivalent workload adapter. |
| [Consistent hashing and random trees](https://doi.org/10.1145/258533.258660) and [Dynamo's virtual nodes](https://pdos.csail.mit.edu/6.824/papers/dynamo.pdf) | Consistent hashing limits the ownership affected by membership changes; Dynamo uses multiple virtual positions per physical node to smooth distribution and handle heterogeneous capacity better than one position per node. | Runnel can apply the idea to a fixed number of internal lanes, not directly to retained records or public partitions. Virtual lanes bound scheduler state and make movement measurable, but they do not solve fencing, durable acknowledgements, or hot-key serialization. |

The direct implication is to separate two concerns. A bounded pull window or
per-member credit can improve batching without changing ownership. If stable
state locality is still valuable, many fixed lanes can make ownership movement
and cache effects measurable. An unbounded map from every key to its current
member would increase recovery and snapshot state without providing a stronger
ordering guarantee.

## Alternatives

### A. Keep demand-driven delivery

The current scheduler remains a viable target architecture for pools whose
members have heterogeneous speed or whose work is short-lived. A poll chooses
the first eligible record, and a member that finishes quickly can request the
next one without waiting for a rebalance.

Membership changes have no assignment movement: a new member simply competes
for the next eligible record, and a departed member's delivery becomes
redeliverable after expiry. Hot keys serialize naturally through the existing
key gate, while unrelated keys continue to progress. A slow member consumes
only its one outstanding slot, but the scheduler provides no cache or lane
locality and repeats candidate selection. The state is bounded by active
delivery and consumer state for a fixed member set; because the current API has
no member registry, arbitrary member churn can still create one in-flight and
attempt entry per unacknowledged member/offset until expiry or acknowledgement.
Scan work can grow with the retained tail and number of blocked candidates.
Batching needs a separate pull or credit model.

This remains the default and the comparison baseline. It is not a failure of
the baseline that it has no movement metric; it simply has no ownership map to
move.

### B. Fixed virtual lanes with cooperative ownership (candidate)

Choose a bounded lane count `L` per shared consumer. A keyed record maps to a
lane using a stable hash of a placement seed, stream, consumer, and ordering
key. A keyless record may use a stable hash of its offset, provided this does
not imply a global ordering guarantee that the current contract does not
provide. The seed must either be derived from canonical stream/consumer
identity or be persisted and replicated; a process restart must not silently
move a key to a different lane. Each lane is assigned to one active member
using rendezvous (highest-random-weight) selection or an equivalent
virtual-node map. `L` is deliberately larger than the normal member count so
that a member can own several small units.

The scheduler state is conceptually:

```text
Placement {
    lane_count: bounded integer,
    placement_seed: stable bounded value,
    placement_epoch: durable/committed integer,
    lane_owner: bounded lane -> member map,
    handoffs: bounded changed-lane -> old/new owner state,
}
```

The map does not contain a key entry. A lane's owner can be reconstructed from
the bounded member set and placement seed, while an in-progress handoff keeps
the old owner, new owner, and epoch until it completes. An indexed lane-ready
queue can avoid rescanning records that belong to other lanes, but that index
must remain bounded or rebuildable from the retained log. A ready queue must
not become one durable entry per retained record; if it is lossy or volatile,
the scheduler must be able to rediscover eligible work without changing
acknowledgement or retry state.

On a member join, leave, or lease expiry, the coordinator computes a new owner
map. Lanes whose owners do not change continue immediately. Changed lanes
enter `draining`: the old owner receives no new records for those lanes, and
the new owner cannot claim them until existing deliveries acknowledge or
expire. A committed epoch and owner generation fence old deliveries. On a
failure, the new owner may claim the lane after the configured failure/lease
boundary; an old acknowledgement then returns the existing stale-delivery
outcome. A graceful leave can shorten this interval only if its authority and
durability are explicit.

This candidate limits movement to changed lanes rather than reassigning every
retained record. It can make a lane's records, key-state cache, and eventual
batch queue local to one member. It also creates a head-of-line risk: a slow
member owns one or more lanes, and a hot lane can contain many otherwise
independent keys. A sufficiently large `L`, per-key gates within each lane,
bounded per-member credits, and later adaptive lane splitting can mitigate the
risk, but none of them should be assumed without measurement.

The member-routing choice in the preceding table is a prerequisite, not a
detail to fill in after hashing. If strict owner-only polling is selected, an
empty response for a non-owner must be distinguished internally from an empty
consumer; otherwise a round-robin client can stop polling the owner that has
work. If any member may claim a lane, the implementation must record how it
limits ownership churn and what locality remains. If polls register members,
the registration and renewal command must be included in the same durable or
committed epoch transition as the owner map.

The first implementation slice should preserve one outstanding delivery per
member and add only local, opt-in lane eligibility plus tests for the selected
member-routing behavior and epoch-fenced handoff. This isolates placement from
a batching change. A later slice may add a bounded fetch window or per-member
credits if the benchmark shows that stable ownership makes that work useful.
The public request and response should stay unchanged in both slices.

### C. Direct key-affine ownership

Map each key hash directly to a member through a consistent-hash ring or
rendezvous choice. This maximizes the opportunity for key-local application
state and avoids lane collisions for distinct keys. Virtual nodes can smooth
the distribution, and only the affected key ranges need to move on a member
change.

The cost is more difficult handoff state. Every reassigned range has to drain
unacknowledged work while preserving key ordering, and a direct mapping does
not give a natural bounded unit for keyless records, batching, or cache
eviction. Mapping keys by hash still cannot split one hot key without
weakening its ordering guarantee. A direct map is therefore attractive only if
measurements show that lane collisions, rather than candidate scan or request
overhead, dominate.

### D. Offset or record ranges

Assign contiguous offset ranges, or rotate fixed record slots between members.
This makes sequential reads and batching straightforward, but a key can cross
range boundaries and needs a separate key gate or range handoff protocol. A
hot stream remains a single sequential domain, and a new member may require
large range movement or an awkward split of active work. Offset ranges also
couple placement to retained-history and replay boundaries. This is not a good
first candidate for Runnel's key-scoped ordering model.

### Comparison summary

| Design | Membership movement | Hot keys | Slow consumers | Fencing/recovery | State and locality |
| --- | --- | --- | --- | --- | --- |
| Demand-driven | No assignment map; no rebalance movement. | One active delivery per key; unrelated keys can continue. | Naturally avoids pinning unrequested work, but offers no stable locality. | Existing delivery expiry and token fencing. | No placement state, but in-flight/attempt state follows unacknowledged members; repeated scan; one-message pull limits batching. |
| Fixed virtual lanes | Only changed bounded lanes move; cooperative drain can delay handoff. | One hot key remains one lane; collisions with other keys are possible. | A slow owner can stall its lanes until drain/expiry; unrelated lanes continue. | Placement epoch plus delivery token; old owner must be fenced before claim. | `O(L + members + in-flight)`; lane indexes can support batching/cache locality. |
| Direct key affinity | Affected hash ranges move; range drain is more complex. | Best isolation of distinct keys; one key still serial. | A slow member pins all keys assigned to it unless work stealing breaks affinity. | Per-range/key handoff and epoch state. | `O(virtual nodes + members + in-flight)` if key map is derived; strong locality, weak keyless story. |
| Offset/range ownership | Range split/merge can move large active portions. | Keys cross ranges; separate coordination is required. | Range head-of-line blocking is likely. | Range generation and replay-aware fencing. | Simple sequential batches, but placement is coupled to log history. |

The candidate ordering is therefore: retain demand-driven delivery; test fixed
virtual lanes; only investigate direct key affinity if lane collisions are a
measured bottleneck. Test pull batching independently so a placement change is
not credited for a benefit caused by fewer protocol requests.

## Candidate state transitions

The following is a semantic outline, not an implementation contract:

| Event | State action | Required safety property |
| --- | --- | --- |
| First member poll | Register or renew a bounded member lease and use the current placement epoch. | Registration does not expose lane ownership; duplicate stable identity is rejected or fenced deterministically. |
| Member join | Compute the new bounded map and increment the placement epoch. Mark only changed lanes as draining. | Unchanged lanes do not pause; the new map is not served before the epoch is durable/committed. |
| Graceful leave | Mark the member's lanes for cooperative transfer. | The leave authority is authenticated and cannot be forged by another member; unresolved API work must not be hidden in a poll side effect. |
| Lease expiry or process failure | Fence the old epoch/member and make its lanes claimable after the documented failure boundary. | Delayed polls and acknowledgements from the old owner cannot commit. Unacknowledged work may redeliver, but acknowledged progress is retained. |
| Old delivery acknowledgement | Apply it only if its member, token, lane generation, and epoch remain current. | A stale result is explicit and does not advance progress. |
| Leader or replica restart | Recover the committed map/epoch and rebuild volatile ready and lease indexes. | Recovery cannot invent a new owner from an uncommitted map or lose a durable attempt count. |
| Handoff completion | Remove the old owner after its in-flight deliveries are acked or expired; activate the new owner. | A key is never simultaneously eligible to old and new owners. |

The first local experiment can keep the existing token format opaque and add
the placement epoch to its internal derivation. The clustered experiment must
make the epoch and handoff transition part of the stream data group's
replicated state, alongside current progress and in-flight ownership. It must
not make a Raft term, node ID, or lane ID part of the public response.

## Workload hypotheses

The following are hypotheses to test, not expected results:

- **Uniform, many members:** fixed lanes may reduce repeated candidate scans and
  make ready records more cache-local once the member count is much larger
  than the current two-member baseline. The owner map and lane checks may
  instead add overhead for a small pool.
- **Skewed keys:** lane assignment may spread many keys more predictably than
  request timing, but a hash collision can create an overloaded lane. The
  demand-driven baseline may win when consumer capacities differ.
- **One hot key:** neither design can improve the strict serial throughput of
  that key. Stable placement should make the bottleneck visible and keep
  unrelated keys moving, not split the key or claim higher total throughput.
- **Membership churn:** stable lanes should move a bounded subset and preserve
  cache state on unaffected lanes. Cooperative draining can increase temporary
  lag and redelivery compared with demand-driven selection.
- **Slow consumer:** demand-driven selection may adapt better because it does
  not preassign work. Stable lanes are acceptable only if the slow member's
  blocked work is bounded and unrelated lanes remain productive.
- **Batching and cache locality:** stable ownership can make a bounded lane
  queue and per-key state reusable, but no such benefit is possible from the
  current one-message public poll alone. Batch size, queue depth, and cache
  effects must be measured separately from ownership movement.

## Narrow future implementation slice

The smallest useful implementation should be staged as follows:

1. Add an internal scheduler model and focused instrumentation without
   changing runtime behavior. Measure candidate scans, active keys, member
   completion, and delivery latency in the current demand-driven path.
2. Implement fixed virtual lanes in the local engine behind an opt-in internal
   setting. Keep the current public grouped poll/ack operations, one delivery
   per member, per-key exclusion, out-of-order acknowledgements, attempt
   persistence, and token errors. Bound `L` and reject invalid configuration.
3. Add a bounded member lifecycle and cooperative lane handoff. Persist or
   journal placement epoch and owner generation before serving a new owner;
   use the existing consumer delivery token to fence delayed work. Do not add a
   per-key owner map or a public assignment response.
4. Run local process restart and membership tests before adapting the design to
   the clustered stream data group. Only then replicate placement transitions
   and exercise leader, follower, and node failure with real broker processes.
5. Consider per-member credits or a fetch batch only as a separate change. Its
   partial-ack, timeout, memory, and crash semantics need their own focused
   contract and must not be conflated with lane ownership.

This slice does not select a lane count, membership timeout, weighting policy,
or public member registration operation. Those values should be chosen from
the workload matrix and captured in an ADR only after the failure semantics
and resource bounds are demonstrated.

## Measurement plan and acceptance scenarios

The current standard suite is useful but incomplete. The local Criterion
benchmarks cover a 100-message two-member shared poll/ack path, 100 keyed
messages across four keys and four members, and 64 unacknowledged members; the
setup is outside the measured loop and the grouped cases are sequential
member turns. See the [shared-consumer Criterion cases](../../crates/runnel-core/benches/broker.rs#L172-L275).
The cluster runner covers sequential two-member grouped delivery and a
parallel grouped case, while its bounded slow-consumer case is a non-grouped
single-member control. See the [cluster grouped cases](../../scripts/benchmarks/cluster.py#L956-L1056)
and [slow-consumer control](../../scripts/benchmarks/cluster.py#L909-L953).
These scenarios establish small uniform baselines, but they do not exercise
stable ownership movement, hot-key concentration, concurrent membership churn,
member capacity skew, grouped slow-member isolation, failure during handoff, or
consume-side batching. The standard `bench-pr-local` comparison is aimed at
the existing three-node workload and cannot establish this design question by
itself.

No focused benchmark is feasible in this change because there is no runtime
placement implementation to measure. Adding speculative benchmark code would
not produce evidence about either design. Once the first slice exists, add a
run-scoped targeted workload to the existing harness or a separate documented
benchmark; run it sequentially under the benchmark lock and retain raw
results.

### Staged measurement plan

The first runtime change should add counters to the demand-driven path before
adding a lane policy. At minimum, record candidate records examined, candidate
records rejected by acknowledged/in-flight/key gates, empty polls, existing
delivery hits, deliveries and redeliveries, expiry work, per-stream lock wait,
and the number of distinct in-flight members. The lane candidate must report
the same counters plus bounded lane load, owner changes, draining duration,
ready-queue depth, handoff redeliveries, and stale acknowledgements. Prefer
aggregate counters and bounded lane identifiers; do not add one metric label
per key, offset, or unbounded member name.

Keep *member population* separate from *simultaneous request concurrency* in
every artifact. The local direct Criterion cases call the broker synchronously
and therefore do not exercise the storage executor; real-process cases also
include the existing per-stream FIFO storage lane and bounded waiter queue. A
64-member case must state both how many members exist and how many polls can be
outstanding, keep the executor settings equal between candidates, and report
storage-queue rejection or wait separately from scheduler work. Otherwise a
placement result can be measuring admission pressure rather than work
placement.

Compare in this order:

1. **Eligibility control:** run the current and candidate selectors against the
   same preloaded local stream and deterministic key trace. Measure selector
   work separately from payload reads, consumer-state persistence, and network
   time. This identifies whether a lane index reduces scan work at all.
2. **Local behavior:** use the unchanged grouped protocol with the same
   one-delivery-per-member limit. Exercise the selected member-routing rule,
   out-of-order acknowledgements, expiry, restart, and member churn. A result
   is invalid if it only measures an implementation with extra delivery
   credits or a larger fetch batch.
3. **Cluster behavior:** after the local state transitions are proven, run the
   same public workload through three real broker processes. Replicate the
   placement epoch, owner generation, and handoff state before measuring leader,
   follower, or owner failure. Use the clustered matrix for independent fault
   cases and preserve raw artifacts.
4. **Authoritative comparison:** when the changed path is covered by the
   standard three-node workload, run `just bench-pr-local` against the recorded
   `origin/main` baseline. If it is not covered, add a relevant targeted case
   first; quick or fixed-repetition runs can diagnose but do not establish an
   optimization claim. Follow [benchmarking evidence policy](../benchmarking.md)
   for resources, repetitions, stability limits, and handoff reporting.

The current design-only change has no candidate runtime to compare, so none of
these commands is a performance gate here. A future result must include
throughput, poll/ack p50/p99/p99.9, CPU, RSS, storage I/O, scan/expiry work,
in-flight and queue bounds, and correctness counters. It must distinguish
benefit from fewer protocol round trips, larger concurrency, batching, or
changed durability boundaries.

The proposed matrix uses the same public intent for the demand-driven baseline
and the lane candidate:

| Scenario | Workload and controlled setup | Measurements and acceptance |
| --- | --- | --- |
| Uniform | 100,000 records at 100 bytes and 1 KiB; 64 members; repeat with no key and with 10,000 uniformly distributed keys; one local broker, then three real broker processes. | Compare throughput, poll/ack p50/p99/p99.9, CPU, RSS, candidate-scan work, and in-flight state. No lost or duplicated acknowledged record; same-key overlap count is zero. |
| Skewed | 100,000 keyed records with an 80/20 key distribution and 64 members; repeat with member capacities of 1x and 4x. | Report per-member work, lag, fairness, throughput, tail latency, and lane load. A stable candidate must not hide an overloaded lane or require public capacity/placement configuration. |
| Hot key | 100,000 records where 50% use one key and the remainder use 10,000 keys; 64 members. | Verify strict sequence for the hot key, its owner/lag, unrelated-key throughput and p99. The result must not claim that one hot key was parallelized. |
| Membership churn | 100,000 records, 64 active members, and ten controlled join/leave events after each 10,000 completed deliveries; repeat with one owner process stopped during a handoff. | Count changed lanes, handoff duration, pause time, redeliveries, stale acknowledgements, and recovery lag. Report movement as a fraction of `L`; no uncommitted ownership may serve, and unaffected lanes should continue. |
| Slow consumer | 64 members, one member delayed by 100 ms before acknowledgement, 100,000 records; keep the delay below the configured acknowledgement timeout. Repeat with the slow member owning a hot and a cold lane. | Measure unrelated-member throughput and p99, slow-lane lag, memory, expiry/redelivery, and queue growth. A slow member may delay its own lanes, but must not globally stall unrelated lanes or create an unbounded queue. |
| Batching/locality | For a stable membership, compare one-message polling with future bounded fetch sizes 1, 16, and 64 over 100,000 keyed records; process per-key state in the consumer harness. | Separate protocol round trips from scheduler work. Measure records/s, CPU per record, p99/p99.9, allocation/RSS, queue depth, and any available cache/scan counters. Do not attribute a gain to placement unless the batch size and protocol boundary match. |
| Failure recovery | Stop the current owner with acknowledged and unacknowledged deliveries in flight; repeat with leader/node failure in the three-process run. | Acknowledged progress never regresses; old tokens are rejected; unacknowledged records redeliver only according to the documented lease/failure boundary; recovery time and duplicate count are reported. |

For every scenario, preserve the current protocol, payload encoding, message
durability boundary, member count, lane count, timeout, process/node count,
CPU and memory limits, and setup/measurement boundary in the artifact. Use
100-byte and 1-KiB payloads, fixed seeds for key distributions, and enough
repetitions to apply the repository's stable-range rules. A proposed adoption
gate is: no correctness or fencing failure; no unbounded state or queue; no
material regression in the uniform and slow-consumer cases; and a stable
throughput or p99 benefit in a target large-pool case sufficient to justify the
additional state and recovery complexity. The exact numeric regression and
benefit thresholds belong in the implementation ADR after baseline variance is
known, rather than being presented here as measured evidence.

## Unresolved risks and decisions

The following must be resolved before implementation is treated as an
accepted architectural choice:

1. How does a member poll the work assigned to it when the current API has no
   assignment response or redirect? If `empty` can mean “no work in this
   member's lanes,” a round-robin client may never discover the owner; if any
   member may claim any lane, the locality and movement claims weaken.
2. How is a member registered, renewed, and gracefully removed without adding
   a placement concept to the public protocol? Relying on poll side effects
   alone may make liveness and duplicate identities ambiguous.
3. Is a member name a stable logical identity, or can a reconnecting process
   rotate it? Stable placement requires an identity policy and duplicate-name
   fencing, while current grouped calls only require a member string.
4. What exact durable state is needed for a handoff, and how does it migrate
   from the current volatile in-flight map and one-delivery-per-member state?
5. Should the lane map be persisted explicitly or deterministically rebuilt
   from a bounded member set and seed? Explicit state eases recovery; derived
   state reduces snapshots but makes membership discovery authoritative.
6. What lane count keeps metadata, snapshot, timer, and index costs bounded for
   many shared consumers? The answer may differ between local and clustered
   engines.
7. How should heterogeneous members be weighted without allowing a fast
   member to starve a slow one or making capacity a public placement setting?
8. How are hot lanes detected and split without moving one ordering key or
   creating unbounded per-key state? This belongs with the separate hot-domain
   exploration.
9. Does handoff wait for all active deliveries, only per-key deliveries, or
   fence immediately and redeliver? Each choice changes duplicate side effects,
   latency, and recovery cost.
10. How should the local and clustered expiry boundaries be aligned? Local
    acknowledgement does not evaluate a deadline until a later poll, while a
    clustered token-bearing acknowledgement evaluates the replicated lease
    clock first. Placement must not accidentally make this divergence wider.
11. How do leader failover and clock/lease assumptions interact with placement
   epochs? Epoch fencing cannot by itself make an expiry timer predictable.
12. What observability reports lane imbalance, draining hashes/lanes, blocked
    keys, movement, and queue pressure without high-cardinality labels?
13. Can a later batch operation preserve per-record outcomes, partial
    acknowledgement, retry, and memory bounds without exposing lanes?
14. How does a clustered placement map interact with future stream placement,
    replica replacement, snapshots, and growth from one node to a cluster?

Until these questions have implementation and failure-test evidence, the
demand-driven scheduler remains the supported behavior and stable placement
remains an optimization hypothesis.

## References

- [Current architecture](../architecture.md)
- [Product backlog](../backlog.md)
- [Local shared-consumer delivery, ADR 0013](../decisions/0013-local-shared-consumer-delivery.md)
- [Clustered shared-consumer ownership, ADR 0015](../decisions/0015-clustered-shared-consumer-ownership.md)
- [`StorageExecutor` and `StorageLane`](../../crates/runnel-core/src/lib.rs#L111-L413)
- [`Broker::poll_group` and local acknowledgement](../../crates/runnel-core/src/lib.rs#L791-L981)
- [`StreamLog::find_candidate` and local eligibility predicate](../../crates/runnel-core/src/lib.rs#L1591-L1625)
- [`apply_group_poll` and clustered acknowledgement](../../crates/runnel-raft/src/lib.rs#L1564-L1827)
- [Shared-consumer Criterion benchmarks](../../crates/runnel-core/benches/broker.rs#L172-L275)
- [Clustered grouped benchmark scenarios](../../scripts/benchmarks/cluster.py#L956-L1056)
- [Reusable shared-delivery contract assertions](../../crates/runnel-test-support/src/lib.rs#L96-L324)
- [Local grouped restart and expiry tests](../../crates/runnel-core/src/lib.rs#L3657-L3731)
- [Clustered grouped restart and dead-letter tests](../../crates/runnel-raft/src/lib.rs#L4680-L4866)
- [Clustered benchmark semantics](../../scripts/benchmarks/README.md)
- [Benchmarking and evidence policy](../benchmarking.md)
- [Kafka KIP-429: incremental cooperative rebalancing](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A%2BKafka%2BConsumer%2BIncremental%2BRebalance%2BProtocol)
- [Kafka KIP-345: static membership](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A%2BIntroduce%2Bstatic%2Bmembership%2Bprotocol%2Bto%2Breduce%2Bconsumer%2Brebalances)
- [Apache Kafka consumer rebalance protocol](https://kafka.apache.org/42/operations/consumer-rebalance-protocol/)
- [Apache Pulsar messaging and Key_Shared subscriptions](https://pulsar.apache.org/docs/next/concepts-messaging/)
- [NATS JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers)
- [Karger et al., Consistent hashing and random trees](https://doi.org/10.1145/258533.258660)
- [DeCandia et al., Dynamo: Amazon's highly available key-value store](https://pdos.csail.mit.edu/6.824/papers/dynamo.pdf)
