# ADR 0036: Select protected retained-history and disk-pressure semantics

- Status: accepted for semantic policy; runtime implementation remains open
- Date: 2026-10-06
- Baseline: `2fa8c95252a2f323495e6ffc1336ab25ce5bf272`
- Evidence class: design/research; secondary: operability correctness
- Related: [retention and disk-pressure backlog](../backlog.md#make-retention-and-disk-pressure-behavior-safe), [retention and disk-pressure design](../design/retention-disk-pressure-plan.md), [ADR 0001](0001-single-node-durable-log.md), [ADR 0014](0014-local-retry-and-dead-letter-policy.md), [ADR 0016](0016-clustered-retry-and-dead-letter-policy.md), [ADR 0024](0024-explicit-offset-replay-read.md), [ADR 0026](0026-semantic-engine-error-classification.md), [ADR 0028](0028-consumer-lag-observation-semantics.md), [ADR 0029](0029-local-typed-dead-letter-move-identities.md), and [ADR 0031](0031-protocol-v2-contract.md)

## Context

Runnel keeps all committed stream history today. Its local engine appends to
one file per stream, persists contiguous consumer checkpoints separately, and
uses a bounded in-memory consumer-state cache that is not a complete
inventory. Explicit replay reads one logical offset and does not pin history.
The clustered engine stores retained messages in replicated state and
materialized snapshots. Neither engine implements retention cleanup or
physical-capacity admission. Existing `storage_bytes` is not a physical-volume
measurement and has different local and clustered meanings.

The [source-backed retention review](../research/retention-disk-pressure-semantics.md)
and [design plan](../design/retention-disk-pressure-plan.md) compare these
boundaries with broker and storage references. This decision selects the
public semantic contract. It does not select a storage layout, API schema,
reserve value, or cleanup schedule.

## Decision

### Retained history and consumer protection

- Existing and new streams retain unlimited history by default. Enabling a
  finite time or size target is an explicit data-lifecycle choice and may not
  happen as a side effect of an upgrade, storage-format migration, or
  deployment default.
- Configured age and size values are independent eligibility targets, not
  hard caps. Either can make the oldest contiguous prefix eligible; when both
  apply, the floor may advance far enough to satisfy both if no protection
  blocks it. Age eligibility compares broker-assigned publish time with the
  broker's current time minus the configured age; a record is eligible only
  when it is strictly before that cutoff. Clustered cutoff selection must be
  deterministic for the committed retention decision. Absence means unlimited;
  an explicitly configured zero age is a zero-duration target, not an alias
  for unlimited. With `max_age=0`, a record becomes eligible on the first
  evaluation after broker time advances beyond its publish time. Size counts
  logical message bytes:
  UTF-8 key bytes when present plus payload bytes. It excludes framing,
  compression, indexes, consumer state, deduplication data, and replica copies,
  keeping the target independent of local and clustered storage
  representations. An explicitly configured zero size target likewise means
  zero retained logical message bytes, not unlimited.
- A retained-history floor is an exclusive logical offset boundary and may
  advance only through a contiguous prefix eligible under the configured
  targets. It must not pass the smallest contiguous durable consumer cursor or
  any active delivery that still requires the record. Out-of-order
  acknowledgements do not move the cursor fence. The floor is monotonic.
- The floor becomes authoritative only at the selected durable local or
  replicated state boundary. Physical deletion must not make a record
  unavailable before that floor is authoritative. Once published, a failed or
  interrupted deletion leaves reclaimable physical bytes; it does not lower or
  roll back the logical floor.
- Finite retention requires a complete, bounded view of durable consumer
  progress for the stream. If the inventory is missing, stale, corrupt, or
  incomplete, the broker must not infer that unseen consumers are absent and
  must not advance the floor. A missing checkpoint for a genuinely new
  consumer is not a pin; its first ordinary poll starts at the current
  retained floor. An existing durable cursor is never silently clamped or
  advanced.
- Finite retention does not reserve history for future consumers. A record
  that has no durable consumer or active delivery protecting it may be removed
  as soon as an age or size target makes it eligible. In particular, a single
  record larger than `max_bytes` is eligible for removal unless protected;
  the newest record receives no minimum-visibility grace. A publish response
  confirms that the append reached its durability point, not that the record
  remains available to a later consumer. Durable publish confirmation does not
  override the explicitly configured lifecycle policy.
- A public request ID remains resolvable while its record is logically
  retained. When the retained floor makes that record unavailable, the ID
  mapping is retired with it. Reusing that ID after the boundary is a new
  publish with a new offset, not a replay of the expired receipt. This follows
  ADR 0031's lifetime boundary and avoids keeping an unbounded identity index
  after the identified history has expired.
- No inactivity expiry, implicit reset, or pressure-driven expiry is accepted.
  Protected lag may keep retained history above either target. If a consumer
  is behind the retained floor due to inconsistent or externally restored
  state, ordinary polling reports unavailable history and leaves its cursor
  unchanged.

### Replay and dead-letter history

- The current one-record, read-only `replay` operation remains non-pinning and
  does not change ordinary consumer progress. An offset below the retained
  floor returns `history_unavailable` with the available half-open offset
  range, consistent with ADR 0024. It never clamps the request, returns
  ordinary `Empty`, or substitutes a later offset. A read racing a floor
  advance resolves against one authoritative stream view and returns either
  the requested record or that unavailable-history outcome.
- Durable replay sessions, replay acknowledgements, and replay pins are not
  part of this first contract. A future session requires its own bounded
  lifecycle and retention decision.
- Broker-managed dead-letter output remains unlimited and is excluded from
  finite source-stream retention settings. Finite retention cannot be applied
  directly to a broker-managed dead-letter target while target history is the
  only durable move-reconciliation record. The local engine's move identity is
  indexed in the target history so recovery can reconcile a completed target
  append before source progress advances. Deleting that history could permit
  a duplicate move after a source-state failure. Finite dead-letter retention
  requires a separate durable reconciliation design; stream naming alone must
  not imply that this dependency has been retired. The clustered atomic
  dead-letter transition retains its existing durability boundary.

### Physical capacity and outcomes

- Logical retention and physical write admission are separate policies.
  Retention may reclaim only history already eligible and not protected under
  the rules above. Disk pressure never changes the selected policy, skips a
  consumer, or deletes committed protected history to make room for a write.
- Physical-capacity reporting must distinguish volume availability,
  configured capacity, reserved headroom, and inspection freshness from
  logical retained bytes, eligible/reclaimable bytes, and protected overage.
  Missing or stale measurements are `unknown`, not zero or healthy. The
  existing `runnel_storage_bytes` meaning must not change. Consumer-specific
  attribution must obey ADR 0028's freshness, coverage, and bounded-cardinality
  rules.
- When capacity enforcement is configured, the reserve must cover the largest
  legal durable operation, bounded concurrent writes, required consumer and
  retention metadata, recovery, and the selected clustered durability point.
  New message data cannot consume its protected reserve. Bounded
  acknowledgement, metadata, and recovery work may use only capacity
  specifically budgeted for those operations. If fresh capacity evidence
  cannot establish that an operation fits within its budget, the
  broker refuses it before mutation. A proven pre-write refusal is retryable
  when capacity may later recover; a request that cannot fit under a fixed
  configured limit is rejected. If a write, sync, or state update may have
  started, the outcome is `unknown` unless the engine proves non-application,
  following ADRs 0026 and 0031. Acknowledgement progress remains unchanged
  unless its durable state update succeeds.
- Capacity preflight is not a guarantee against an external writer, quota
  change, or filesystem race. The selected engine durability boundary remains
  authoritative. This decision does not weaken clustered commit requirements,
  authorize a smaller quorum, or change replica replacement rules. Replicas
  may reclaim local bytes only after applying the same committed logical
  floor.
- Cleanup and pressure are reported separately: protected overage, pending
  eligible cleanup, and insufficient or unknown physical headroom have
  distinct meanings. Health and diagnostic reads must remain bounded and must
  not require a write to the pressured data volume. No numeric reserve,
  watermark, cleanup interval, metric name, or readiness transition is
  accepted here.

## Rationale and source comparison

Apache Kafka documents independent time and byte retention with segment-level
deletion, while a consumer's committed offset does not pin old broker segments
from cleanup ([topic configuration](https://kafka.apache.org/42/configuration/topic-configs/)).
NATS JetStream separates limit retention from acknowledgement-driven `Interest`
and `WorkQueue` policies; stream limits remain upper bounds under those modes,
so a stalled consumer can still lose backlog ([retention policy documentation](https://docs.nats.io/nats-concepts/jetstream/streams)).
Redpanda documents separate topic retention and local-capacity targets; its
tiered-storage local cleanup uses available-volume pressure ([topic properties](https://docs.redpanda.com/streaming/current/reference/properties/topic-properties/),
[tiered storage](https://docs.redpanda.com/streaming/current/manage/tiered-storage/)).
These references show that retention limits, acknowledgement policy, and
physical storage pressure are distinct controls; their policies do not
establish Runnel's replay or at-least-once promises. NATS also separates the
per-message `MaxMsgSize` admission check from stream `MaxBytes` retention and
offers `DiscardOld` versus `DiscardNew`; Runnel does not turn a soft retention
target into a producer-size rejection rule. Retaining the newest record as a
special case would make a zero-age stream retain data indefinitely while idle,
or would require a layout-specific exception. No source or product requirement
supports that exception, so an eligible newest record may be removed. NATS's
.NET client treats `MaxAge = 0` as unlimited ([stream configuration source](https://github.com/nats-io/nats.net/blob/main/src/NATS.Client.JetStream/Models/StreamConfig.cs)); Runnel instead reserves unlimited behavior for an absent target and treats an explicitly configured zero as a finite target. The future configuration surface must preserve field presence rather than collapse zero into absence.

The log-structured file-system literature treats segment cleaning as
space-reclaiming work with its own bandwidth cost ([Rosenblum and Ousterhout](https://web.stanford.edu/~ouster/cgi-bin/papers/lfs.pdf),
[Lomet and Luo](https://arxiv.org/abs/2005.00044)). Linux documents that
filesystem-statistics fields are not meaningful on every filesystem
([`statvfs(2)`](https://man7.org/linux/man-pages/man2/statvfs.2.html)), writes
may be partial ([`write(2)`](https://man7.org/linux/man-pages/man2/write.2.html)),
sync can fail with `ENOSPC` ([`fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)),
and unlinking an open file does not immediately free its blocks
([`unlink(2)`](https://man7.org/linux/man-pages/man2/unlink.2.html)). These
sources support a measured reserve, stage-aware write outcomes, and reporting
cleanup lag separately from logical history. They do not justify treating a
preflight measurement as a write guarantee.

ADR 0028 additionally bounds how consumer progress may be observed: unknown,
stale, incomplete, expired, and absent state is not zero or caught up, and
consumer identity must not become an unbounded default metric label.

Protecting durable cursors is the safest useful first policy because current
delivery is at least once, acknowledgement advances only durable contiguous
progress, and replay already exposes an explicit unavailable-history result.
An opt-in expiry mode could meet a tighter history bound, but would permit a
configured age/size rule to end redelivery and replay eligibility for
unacknowledged records. That destructive policy is rejected for this first
contract and would need a separate decision and failure evidence. Under this
contract, protected lag can cause target overage
and eventually stop new durable writes; that is an explicit availability cost
of preserving the selected history entitlement.

## Consequences and follow-up

- A finite time or size target does not guarantee bounded retained bytes while
  durable consumers protect a prefix. Physical admission remains the safety
  valve and may refuse writes; a future explicit expiry policy may be proposed
  only as a distinct destructive choice.
- Under a finite policy, future consumers are entitled only to history still
  above the retained floor. With no durable consumer, size retention may remove
  a newly published record that alone exceeds `max_bytes`; a zero-age target
  also provides no minimum age window. Operators that need an unbounded future
  replay window must leave finite retention disabled.
- A complete, bounded durable-consumer inventory is a prerequisite for finite
  retention. The current local cache cannot establish that inventory, so the
  current runtime must not enable finite cleanup based on that cache.
- The current replay read does not pin data, and dead-letter history remains
  unlimited. These are deliberate limits of the first contract, not promises
  that replay sessions or bounded dead-letter retention already exist.
- The design plan remains open for implementation architecture, filesystem
  provider behavior, numeric reserve and pressure thresholds, cleanup
  scheduling, operator surfaces, and failure/benchmark evidence. The backlog
  outcome remains open until its runtime and operational acceptance criteria
  pass.

## Alternatives considered

- **Expire lagging consumers at configured age/size limits.** Rejected for the
  first policy because it removes replay and redelivery eligibility for
  unacknowledged data, and the current engine has no complete consumer
  catalogue or explicit reset outcome. A later destructive policy requires a
  separate decision and public unavailable-history and acknowledgement
  behavior.
- **Treat age/size values as hard caps and discard oldest data under pressure.**
  Rejected because it would silently convert protected history into loss when
  disk fills. Logical policy never changes based on physical pressure.
- **Always retain the newest record or segment.** Rejected because it creates
  a special replay entitlement outside durable consumer progress, can leave an
  idle stream above a zero-age target indefinitely, and would tie semantic
  guarantees to the selected storage layout.
- **Reject a publish whose record is larger than `max_bytes`.** Rejected
  because it turns a soft retention target into per-record admission and
  rejects useful messages even when the configured lifecycle policy permits
  the old prefix to be removed. Any per-stream message-size admission limit
  needs a separately named policy.
- **Use acknowledgement-driven deletion such as interest or work-queue
  retention.** Rejected because it changes acknowledgement into a retention
  contract and can make a new consumer miss history. It would need a separately
  named stream semantic.
- **Count encoded or physical bytes for `max_bytes`.** Rejected because
  framing, compression, indexes, and replica layout differ between the local
  and clustered engines. The accepted limit counts logical key and payload
  bytes; separate measurements report physical usage.
- **Rely only on reactive `ENOSPC` handling.** Rejected as the capacity policy
  when enforcement is enabled because it provides no reserved headroom for
  bounded writes and metadata. Preflight remains advisory, and stage-uncertain
  write failures remain `unknown`.

## Verification boundary

This decision changes documentation only. It adds no runtime feature or test
result. Before implementation, focused evidence must cover complete consumer
inventory, retention-floor recovery, replay/poll below the floor, a confirmed
publish that becomes unavailable under an oversized-record or zero-age target,
request-ID deduplication before and after its retention horizon, dead-letter
reconciliation, low-space admission, external capacity races, write/sync
ambiguity, cleanup interruption, clustered committed floors, and bounded
pressure reporting. Measurements must establish reserve and cleanup values;
the design proposal is not that evidence.
