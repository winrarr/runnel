# Retention and disk-pressure design

- Status: exploratory implementation design; semantic policy accepted by [ADR 0036](../decisions/0036-retained-history-and-disk-pressure-contract.md)
- Last reviewed: 2026-10-06
- Baseline: `2fa8c95252a2f323495e6ffc1336ab25ce5bf272`
- Reading guide: [design-note conventions](README.md)
- Scope: safe retained-history policy, bounded cleanup, and durable-write admission
- Related outcome: [Make retention and disk-pressure behavior safe](../backlog.md#make-retention-and-disk-pressure-behavior-safe)
- Related decisions: [ADR 0028: consumer-lag observation semantics](../decisions/0028-consumer-lag-observation-semantics.md) and [ADR 0036: retained-history and disk-pressure contract](../decisions/0036-retained-history-and-disk-pressure-contract.md)
- Related research: [Retention and disk-pressure semantics](../research/retention-disk-pressure-semantics.md),
  [Distributed architecture exploration](../research/distributed-architecture-options.md),
  [Raft follower recovery and replacement](../research/raft-recovery-and-replacement.md),
  [Message encoding and compression study](../research/message-encoding-and-compression.md),
  and [Systems performance research for Runnel](../research/systems-performance-research.md)
- Related design evidence: [Durability and delivery policy](durability-delivery-policy.md), [clustered durability and outcomes](clustered-outcome-contract.md), and [dead-letter recovery](dead-letter-recovery.md)

ADR 0036 accepts the retained-history and physical-capacity semantics below.
This note records the supporting evidence and remaining implementation
hypotheses; it is not itself an accepted storage or API design. The accepted
contract preserves the current stream, record, consumer, acknowledgement,
replay, and ordering model. This documentation update does not change runtime
code, deployment files, or the current compatibility policy.

The current technical boundaries are authoritative in [architecture](../architecture.md)
and the accepted decisions linked from this note. Proposed policies, state
shapes, and cleanup mechanisms are inferences or illustrative mechanisms; the
stages below are outcome/evidence gates, not a prescribed module or API plan.

## Outcome and boundaries

Runnel should let operators set finite logical history targets without
silently losing data protected by durable consumer progress, and should keep
that policy separate from physical capacity admission. Protected data may
exceed a target; writes may be refused when safe physical headroom is gone.
The implementation must make the following visible to an operator and, where
it affects an application operation, to the client:

- which time and size rules are configured;
- whether durable consumer progress or an active delivery protects history;
  replay-session pins are outside the accepted contract;
- how much storage is retained, reclaimable, over budget, and reserved;
- whether a publish is accepted, explicitly rejected, retryable, or ambiguous;
- whether cleanup, recovery, or replication is preventing normal progress.

The public model remains topology-free. A client may reason about a stream,
consumer, record, offset, replay scope, retention policy, and outcome. It must
not need to know a local file, segment, Raft group, node, replica, leader,
filesystem path, or physical placement. The local and clustered engines may
use different storage implementations as long as they implement the same
semantic policy.

The proposal has two layers:

1. stream retention decides which committed history is still eligible for
   delivery or replay; and
2. storage admission decides whether a new durable operation can safely use
   the remaining physical capacity.

Retention is not consensus-log compaction. The compactable clustered Raft log
and retained broker history remain separate, as required by [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) and the current architecture.

## Evidence and current behavior

The current implementation is intentionally a small vertical slice. The
following observations are the starting point for this plan.

| Area | Accepted current behavior | Consequence for this proposal |
| --- | --- | --- |
| Local storage | `runnel-core` uses one append-only log per stream with legacy `RNL1`, checksummed uncompressed `RNL2`, and request-aware checksummed `RNL3` frames. Normal server appends use `RNL1` for ordinary records and `RNL3` when a request or move identity is present; `RNL2` is an explicit core/test format path. Request-aware IDs are bounded at 1 KiB and request-aware keys/bodies at 128 bytes/64 MiB. Appends call `sync_data` before reporting success and open scans the complete log. The newest 1,024 record locations and up to 1,024 sparse checkpoints are cached; the request-ID map is rebuilt from all `RNL3` frames and grows with distinct retained IDs. An incomplete trailing frame is truncated on recovery, while malformed complete records fail closed without rewriting the file. | Retention cannot safely delete a prefix of one mutable file. Segmentation, format metadata, and a durable retained-history floor are prerequisites; the current frames have no segment generation or retention metadata. Bounded record-location caches do not bound all retained metadata; see [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream). |
| Local consumers | Durable consumer state is a JSON checkpoint plus an append-only event journal capped at 64 KiB. It records the contiguous committed offset, out-of-order acknowledgements, delivery attempts, an optional versioned consumer policy, and the policy pinned to assigned offsets. The in-memory consumer-state cache holds at most 1,024 entries and is not a complete catalogue; active deliveries and deadlines are also in memory. Checkpoint files are not size-bounded. A restart may redeliver an unacknowledged message. | A safe deletion watermark must use durable contiguous progress, not the highest acknowledged offset, and must fence active deliveries. Neither the bounded cache nor transient deliveries can establish a complete durable-consumer inventory; any retention query needs a bounded source and explicit coverage. |
| Local replay | A new consumer starts at offset zero and ordinary polling follows its checkpoint. The additive `replay` operation reads one inclusive logical offset without creating delivery state or changing ordinary progress. All history is currently retained, so the floor is zero and `history_unavailable` applies only outside the available `[0, next)` range; there is still no runtime retention policy or replay session. | Once the accepted floor is implemented, below-floor history remains explicitly unavailable rather than turning a gap into `Empty`. No replay cursor/session is selected; any later one must remain distinct from ordinary consumer progress. |
| Local dead letters | The source record is appended to a derived dead-letter stream before the source checkpoint advances. New local moves use a bounded source-stream/consumer/offset identity and same-content reconciliation, so a known completed target append is not appended again after source-state failure or reopen. Since the previous review, test-scoped recovery checks cover partial target frames, complete frames reported failed before sync, and a source-event sync failure; a real-server test also drops the poll response and verifies recovery after restart. These do not reproduce device/power-loss failures or resolve legacy records; see [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records). | Retention must preserve this at-least-once ordering and move-identity fence, and define whether derived streams inherit or override source retention. A future policy must not delete target evidence while source progress still depends on reconciliation. |
| Clustered storage | `runnel-raft` keeps complete message vectors in each stream data group's replicated state, writes a state-machine journal before applying committed entries, and creates complete materialized snapshots. Snapshot transfer uses bounded 64 KiB chunks and retries from byte zero after interruption. Data-group membership comes from the configured peer map; the three-process setup is an evidence profile, not a universal topology. Raft log compaction is independent from broker history. | The clustered path needs replicated logical retention state but local, interruptible physical cleanup. A snapshot must not resurrect history below the committed retention floor, and capacity claims must be stated for configured membership rather than assumed three-voter behavior. |
| Clustered consumers | Progress, out-of-order acknowledgements, versioned consumer policy, attempts, in-flight ownership, deadlines, and fencing state are in the stream data-group state. Grouped polls and acknowledgements are leader-authorized writes. | Retention decisions affecting a cluster must be deterministic state-machine facts; local filesystem inspection cannot itself decide a replicated watermark. Consumer-lag observation must follow [ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md) and cannot be inferred by summing replica copies. |
| Admission | The server bounds connections, request frames, in-flight requests, and request duration. The JSON-lines request limit defaults to 1 MiB and can be configured up to 64 MiB, including JSON/base64 representation; publish batches are capped at 1,024 records and 64 MiB of encoded request bytes, with per-record outcomes rather than atomicity. Local storage work has a bounded executor. The real-server [`sustained_storage_pressure_is_bounded_observable_and_recovers` test](../../crates/runnel-server/tests/admission.rs#L850) repeats same-stream FIFO-induced executor saturation and checks rejection, health/readiness/metrics, recovery, and subsequent durable traffic. This is synthetic execution-path pressure, not filesystem-capacity or `ENOSPC` evidence. `BrokerError::kind()` and `BrokerError::outcome()` provide an engine-level semantic boundary. The server still maps failures to provisional v1 codes, and the reusable client conservatively classifies post-write timeout/disconnect cases as unknown. ADR 0031 accepts a v2 outcome/stage contract; its runtime encoding and enforcement are not implemented. | Disk admission must be checked before append, but races and `ENOSPC` still require the accepted stage-aware outcome contract from ADR 0031 to be implemented and validated. Existing frame, record-count, and storage-executor limits remain separate from a physical disk reserve. |
| Metrics and health | `/metrics` exposes request/admission counters, request latency, process-lifetime delivery counters, storage bytes, health failures, and clustered snapshot activity. Local `storage_bytes` sums `.log` file lengths; clustered `storage_bytes` sums logical stored keys and payloads. Neither includes every journal/checkpoint/snapshot byte or filesystem availability. The engine health query is subject to a one-second HTTP timeout; a failed/timed-out scrape omits engine-derived samples. There is no capacity provider or complete durable-consumer catalogue/source revision. [ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md) accepts cursor-lag semantics only, not a runtime metric or bounded query. | Existing storage bytes are not a disk budget or a cross-engine physical-usage comparison. New lag and retention signals must keep coverage/freshness separate and must not represent unknown, stale, incomplete, or expired observations as zero. Do not silently change the existing gauge definition. |
| Deployment | The illustrative Kubernetes deployment gives each of three static-cluster pods an independent 10 GiB claim, 1 GiB memory limit, 1 CPU limit, five-minute startup-probe window, and 30-second termination grace period. A `minAvailable: 2` PodDisruptionBudget protects Ready-count availability for Eviction API requests, but readiness does not establish quorum margin, replication progress, or disk capacity. It has no broker retention or capacity settings. | A broker capacity policy must work without Kubernetes, use detected available capacity conservatively, and document that PVC size is not by itself free space available to the broker. The PDB does not establish a durable-write or storage-capacity guarantee. |

These facts are also tracked as [TD-002](../tech-debt.md), [TD-005],
[TD-006], [TD-009], [TD-010], [TD-017], [TD-019], [TD-022], [TD-023], and
[TD-025].
They are evidence about the code at this baseline, not promises for a future
implementation.

### Current guarantees to preserve

The implementation must retain these accepted behaviors unless a later,
explicitly versioned decision changes them:

- a publish is not reported as durably accepted before the selected durable
  write point succeeds;
- an acknowledgement advances durable progress only after its state update
  succeeds;
- local incomplete tails are recoverable under the current format rules, and
  retention must not weaken the existing corruption handling;
- grouped delivery preserves per-key exclusion and stale-delivery fencing;
- clustered committed state is recovered through the replicated state-machine
  and snapshot boundaries, not by exposing Raft details to clients;
- consumer-lag observation, when added, does not change delivery, acknowledgement,
  retention, replay, dead-letter, or readiness behavior. [ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md)
  defines cursor lag as `H - C` only for a same-revision stream head `H` and
  contiguous durable cursor `C` with `F <= C <= H`, where `F` is the retained
  floor; a cursor below a future floor is `retention_expired`, not a numeric
  lag or a clamped cursor;
- local dead-letter movement remains at least once across separate durable
  target and source writes; the current move identity reconciles a completed
  target append without a second target record, while uncertain I/O, process
  crash timing, and legacy-record gaps remain documented;
- storage identity checks and the conservative replacement boundary from
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) and [ADR 0019](../decisions/0019-clustered-storage-identity.md) remain in force.

## Accepted semantic contract

The following outcomes are accepted by [ADR 0036](../decisions/0036-retained-history-and-disk-pressure-contract.md):

- New and existing streams default to unlimited history. A finite time or size
  target is an explicit data-lifecycle choice and is never enabled implicitly
  by an upgrade or storage migration.
- Finite age and size targets are independent eligibility triggers, not hard
  caps. `max_age` uses broker-assigned publish time. `max_bytes` counts UTF-8
  key bytes, when present, plus payload bytes; it does not count record
  framing, compression, metadata, or physical replica copies.
- An absent target means unlimited, while an explicitly configured zero is
  finite. With `max_age=0`, a record becomes eligible on the first evaluation
  after broker time advances beyond its publish time; `max_bytes=0` targets
  zero retained logical message bytes. Neither zero is a per-message admission
  rejection rule.
- Age makes a record eligible only when its publish time is strictly before
  the broker-selected cutoff. Size makes the oldest contiguous prefix
  eligible until the retained logical key-plus-payload bytes are at or below
  the target, if protection allows. Either configured target can advance the
  floor; a protected cursor can leave either target exceeded. A new consumer
  without durable progress starts at the current floor, so its replay range is
  the remaining retained suffix. A finite policy does not reserve history for
  future consumers: without a protecting durable cursor or active delivery,
  a record larger than `max_bytes` may itself be removed once eligible.
- The logical retained floor may advance only over an eligible contiguous
  prefix and may not pass the smallest durable contiguous consumer cursor or
  an active delivery still requiring that history. Out-of-order acknowledgments
  do not move the cursor fence. If a complete bounded durable-consumer
  inventory is unavailable, retention does not advance the floor.
- Protected lag may leave a stream above its configured targets. The broker
  does not expire consumers, reset cursors, or switch to destructive retention
  under disk pressure. A new consumer with no durable state starts at the
  current floor; an existing cursor is never silently clamped.
- The existing offset-based replay read does not pin history. A request below
  the floor returns `history_unavailable` with the available half-open range;
  it never returns `Empty` or a later record. Durable replay sessions and
  replay pins remain outside this contract.
- Broker-managed dead-letter output remains unlimited until durable
  reconciliation no longer depends on its retained move identity. Finite
  source retention cannot delete protected source history before source
  progress is durable.
- A public request ID remains in the deduplication horizon for the lifetime of
  its retained record. Floor advancement retires the ID mapping with the
  record; reusing that ID afterward is a new publish. Retries after expiry are
  outside the accepted replay-safety guarantee in ADR 0031.
- Logical retention and physical write admission remain separate. Capacity
  pressure cannot change the retention policy or delete committed protected
  history. When capacity enforcement is configured, reserve and pressure
  reporting must account for the selected durable operations; stale or
  incomplete capacity evidence is unknown, and a write whose durable stage is
  uncertain is not reported as a confirmed rejection.
- Logical retained bytes, eligible/reclaimable bytes, protected overage,
  physical availability, reserved headroom, and source freshness are distinct
  observations. `runnel_storage_bytes` keeps its existing engine-specific
  meaning, and consumer attribution follows ADR 0028's bounded coverage rules.

The contract does not select a public configuration schema, on-disk layout,
numeric capacity reserve, cleanup schedule, metric names, readiness policy, or
cluster quorum policy. It also does not make the backlog outcome complete:
runtime behavior, operator controls, failure evidence, and measurements remain
open.

## Evidence and implementation hypotheses

### Direct competitor and research comparison

The proposal is informed by the following direct comparisons, rechecked on
2026-10-06. These systems solve related but different problems; their behavior
is evidence for tradeoffs, not a compatibility target for Runnel.

| System | Relevant design | Implication for Runnel |
| --- | --- | --- |
| [Apache Kafka topic configuration](https://kafka.apache.org/42/configuration/topic-configs/) | The default `delete` policy removes old log segments when time or size retention is reached. `retention.bytes` is enforced per partition, and retention/cleaning is file-granular; `segment.ms` can roll an active segment so it becomes eligible for cleanup. | Segment-granular cleanup and independent time/size triggers are well-established, but Runnel also needs consumer-progress and active-delivery fences because its public contract exposes replay and acknowledgements. A physical disk reserve must remain separate from the logical topic limit. |
| [Redpanda topic properties](https://docs.redpanda.com/streaming/current/reference/properties/topic-properties/) and [tiered-storage space management](https://docs.redpanda.com/streaming/current/manage/tiered-storage/) | Redpanda exposes per-partition `retention.ms` and `retention.bytes`, rolls segments with `segment.ms`, and, with tiered storage, separates local targets from topic-wide retention. Its local tier can purge using actual available volume space to avoid disk-full conditions caused by skew. | Keep logical retained-history policy distinct from physical local capacity. Runnel should reserve space for durable writes, snapshots, and cleanup even without tiered storage, and should measure pressure from effective free space rather than claim size alone. |
| [NATS JetStream retention policies](https://docs.nats.io/learn/jetstream/retention-policies) | `Limits` retains messages until `MaxMsgs`, `MaxBytes`, or `MaxAge` is reached. `Interest` removes a message after every interested consumer acknowledges it, while `WorkQueue` removes it after the first consumer acknowledgement. Limits remain a backstop, so a stalled consumer can lose backlog. NATS also separates per-message `MaxMsgSize` from stream `MaxBytes`, exposes `DiscardOld` versus `DiscardNew`, and its .NET client defines `MaxAge = 0` as unlimited ([source](https://github.com/nats-io/nats.net/blob/main/src/NATS.Client.JetStream/Models/StreamConfig.cs)). | Keep bounded-history retention and acknowledgement-driven work-queue semantics separate. Runnel selects protected durable progress for its first policy; a future expiry mode would be a distinct destructive contract, and replay gaps remain explicit rather than becoming `Empty`. Unlike NATS's zero sentinel, an explicit Runnel zero-age value is finite; absence alone means unlimited, so a configuration surface must preserve that distinction. |

The storage research supports the physical boundaries in this plan. The
original [log-structured file-system design](https://web.stanford.edu/~ouster/cgi-bin/papers/lfs.pdf)
uses segments as the unit of cleaning and relies on a cleaner to preserve
large free areas for future writes. [Lomet and Luo's analysis of reclaiming
space in log-structured stores](https://arxiv.org/abs/2005.00044) shows that
cleaning consumes I/O and that the available slack space and cleaning order
materially affect write amplification. If an implementation uses segments,
these results support bounded cleanup work, an explicit capacity reserve, and
measurements of cleanup amplification; they do not require that storage layout.
They do not establish Runnel's consumer fencing or client outcomes, now
selected in ADR 0036 and still requiring runtime failure tests.

The accepted contract leaves three deliberate differences from the reference systems:

- Runnel's retention floor is a logical, durable fact in the clustered state
  machine and is constrained by contiguous consumer progress and active
  delivery leases; local free-space inspection cannot independently advance it.
- Physical cleanup is interruptible and may lag the logical floor. Admission
  rejects or makes a publish retryable/unknown before reserved capacity is
  violated; it does not silently convert a protected policy into destructive
  expiry.
- Work-queue behavior is not inferred from ordinary stream retention. If a
  future API needs first-ack removal, it should be a separately named semantic
  mode with its own compatibility and recovery decision.

The source review [Retention and disk-pressure semantics](../research/retention-disk-pressure-semantics.md)
maps the alternatives and evidence needs across history eligibility,
consumer/replay state, age/size/lag/reserve precedence, `ENOSPC` outcomes,
deletion recovery, and bounded operator signals. ADR 0036 resolves the public
semantic choices; the following configuration and storage mechanisms remain
hypotheses until implementation evidence supports them.

### Configuration vocabulary

The following is an illustrative schema only; ADR 0036 accepts the semantics
but not these field names or an administrative operation. The first supported
retention mode is protected. There is no selectable `expire` mode in the
accepted contract.

```text
RetentionPolicy {
    max_age: optional duration,
    max_bytes: optional logical bytes
}
```

The broker should also have a storage-admission policy with fields equivalent
to:

```text
StorageAdmissionPolicy {
    capacity_limit: optional bytes or fraction,
    reserved_capacity: bytes or fraction,
    cleanup_interval: duration,
    cleanup_budget: bytes or duration,
    max_publish_batch_bytes: bounded bytes
}
```

The exact request names and whether stream configuration is created with the
stream or through a separate administrative operation are unresolved API
choices. The semantics and validation rules should not vary between local and
clustered engines.

Absent `max_age` and `max_bytes` mean unlimited automatic retention for both
existing and new streams. Enabling either finite target is an explicit
data-lifecycle choice, not an incidental consequence of a new storage format.
When both are set, either target may make the oldest contiguous prefix
eligible. Protection can prevent the floor from satisfying either target, so
these values are not hard caps.

### Time and size retention

Time and size are independent triggers. In this segment-based implementation
hypothesis, a complete immutable segment becomes a candidate when either
condition is true:

- its newest record is older than the age cutoff; or
- logical message bytes exceed `max_bytes`.

The broker deletes the oldest eligible complete segments until the targets
are met or no safe segment remains. Thus, when both limits are set, either
trigger may advance the floor; protection can leave either target exceeded. A
record at the exact time cutoff remains retained until it is strictly older
than the cutoff. Size overage caused by segment granularity, an active delivery,
or a protected consumer is reported rather than hidden.

The accepted age semantics use the broker-assigned `published_at_ms` already
present in the logical message. A clustered cleanup decision uses a
deterministic committed cutoff; a local backward wall-clock adjustment may
delay eligibility but must never make a record disappear early. The concrete
clock and metadata mechanism remains an implementation choice.

The accepted `max_bytes` unit is logical key-plus-payload bytes, independent of
compression and record framing. Physical storage usage and the bytes needed
for cleanup or consensus remain separate measurements. The accounting index
and cleanup mechanism are implementation choices and need bounded resource
evidence.

If segment storage is selected, cleanup is segment-granular: it first seals or
rolls the active segment and never removes individual records from a segment in
place. The accepted public contract requires a contiguous logical floor, not
segments. Physical storage may remain above a target because its deletion unit
would cross the boundary, violate a safety fence, or consume cleanup workspace.

### Precedence

The accepted precedence is semantic; exact scheduling is an implementation
choice. A retention cycle and durable write must preserve this order:

1. Validate the request and preserve durable-write, checkpoint, active-lease,
   corruption, and identity safety. Disk pressure never authorizes an
   unconfigured destructive policy.
2. Apply the durable contiguous consumer and active-delivery fences. The
   current read-only replay operation does not pin history. A complete
   bounded inventory is required; incomplete coverage blocks floor advance.
3. Evaluate `max_age` and `max_bytes` as independent triggers, selecting the
   oldest complete segments that are allowed by the first two steps.
4. Publish a logical floor only through the durable local manifest or
   replicated state-machine boundary, then reclaim physical bytes.
5. Check reserved physical capacity for the proposed durable write. If
   cleanup did not leave required headroom, refuse before mutation when
   non-application is known; if the durable stage is uncertain, report
   `unknown`. Do not delete protected data merely to make the write fit.

If no policy permits safe reclamation, admission is the safety valve. Disk
pressure never changes protected retention into expiry.

### Lagging consumers and replay eligibility

The safe prefix for a normal consumer is its contiguous committed offset. A
group consumer's highest out-of-order acknowledgement does not free earlier
history until the gap is closed. A consumer with no persisted state does not
pin history merely by potentially existing; its first ordinary poll starts at
the current retained floor. A complete durable inventory is required before
any floor advance; an unknown or incomplete consumer source is not evidence of
absence.

The accepted policy protects existing durable consumer progress. Destructive
expiry is not selectable:

| Status | Retention behavior | Lagging-consumer outcome | Disk-pressure consequence |
| --- | --- | --- | --- |
| Protected (accepted) | A durable contiguous cursor and every active delivery fence the floor. A lagging consumer may keep history above either target. Durable consumer state must be complete before the fence is used. | The consumer retains normal delivery eligibility for unacknowledged history. No implicit skip, checkpoint advance, inactivity expiry, or replay pin is allowed. | If no safe history can be reclaimed before the reserve is reached, new durable writes are refused with stage-aware outcomes. |
| Expiry (deferred) | No age/size rule may advance the floor beyond a durable consumer or active delivery. | Any future destructive mode needs a separate decision defining explicit unavailability and acknowledgement fencing; it is not selectable under this contract. | Disk pressure cannot opt a stream into destructive expiry. |

An active delivery remains a fence until it is no longer valid. The first
contract has no expiry mode, so lease expiry can make a record eligible only
when all durable consumer cursors and other protections also permit the floor
to pass it. A late acknowledgement cannot succeed for history that is no
longer retained.

There is no inactivity expiry. Durable consumer state continues to protect
its cursor until progress advances. Any future consumer deletion or reset
operation that releases that protection must be explicit and must report the
resulting replay boundary; no such lifecycle API is selected here.

### Replay

The accepted replay behavior remains the bounded, read-only `replay` operation
from ADR 0024: it accepts one inclusive logical offset, returns one record with
no delivery token or attempt, and never changes ordinary consumer progress.
The local and clustered engines return `history_unavailable` with the available
half-open offset range when the requested offset is not present. Current
storage retains all records from offset zero, so the earliest boundary is
currently zero. Under the accepted retention contract, an offset below the
floor also returns `history_unavailable`; the broker neither clamps the
request nor substitutes a later record. A read racing a floor advance resolves
against one authoritative view and returns either the requested record or
that unavailable-history outcome.

The current runtime has no durable replay cursor, session, acknowledgement,
range selector, time-selector support, or retention pin. A successful
read-only offset replay returns one record from the history available at its
authoritative read point and does not promise that the record remains stored.
Any later session or multi-record replay feature needs a separate decision for
its lifecycle and any pinning; it must not be inferred from this bounded
operation or overload the ordinary consumer checkpoint.

Time-selector semantics are accepted separately in
[ADR 0038](../decisions/0038-timestamp-based-replay-selector.md): select the
lowest logical offset whose stored broker publish timestamp is at least the
requested time, with explicit no-match and deleted-prefix completeness
outcomes. The selector is not implemented. A bounded lookup index and complete
deleted-prefix maximum remain implementation gates. Offset reads below the
retained floor return `history_unavailable`; time selection also returns that
outcome whenever a deleted prefix could contain an earlier match. Neither
selector silently replays an incomplete suffix, and time selection does not
promise a wall-clock boundary from the first retained record.

### Reserved capacity and storage accounting

Admission must use filesystem or configured-volume availability, not only the
bytes attributed to message records. Define:

```text
effective_limit = configured_capacity or detected_filesystem_capacity
available = min(detected_filesystem_available,
                effective_limit - broker_physical_usage)
```

`detected_filesystem_available` already accounts for external users of a
shared volume. If the data volume is shared with unrelated files, it is
authoritative and the configured limit is only an upper bound on broker usage.
A dedicated volume is recommended operationally but is not a correctness
dependency.

The reserve is unavailable to ordinary message appends. It covers, at minimum:

- the largest accepted append or batch and its framing;
- atomic consumer-state and manifest replacements;
- one bounded cleanup working set;
- clustered journal and snapshot write amplification;
- filesystem metadata and directory-sync slack; and
- a small margin for concurrent operations that passed the same preflight.

An implementation should compute a conservative `required_headroom` from
configured maximums and reject configuration that cannot reserve space for one
maximum legal operation. It must not multiply a nominal request limit by an
unbounded number of network tasks. The existing connection, request, and
storage-executor bounds are part of this calculation.

Retention cleanup may reclaim space, but the reserve must not depend on
cleanup succeeding in the same operation. A publish admission check is:

1. validate the request and its bounded size;
2. read the current pressure state;
3. if low, schedule or perform one bounded cleanup attempt without waiting
   indefinitely;
4. recheck projected physical usage plus required headroom; and
5. append and sync only if the reserve remains intact.

The preflight is advisory against external writers and races. An actual I/O
failure after append starts remains possible and needs the failure semantics
below.

### Low-space and full-disk admission

The following pressure states are implementation vocabulary, not accepted
public state names or topology. Their outcomes must follow ADR 0036:

| State | Entry condition | New publish behavior | Other behavior |
| --- | --- | --- | --- |
| `normal` | Available space is above reserve plus cleanup/write hysteresis. | Accept only if the projected durable write stays above reserve. | Run cleanup at its normal interval. |
| `low` | Available space is below a measured low-water mark or reclaimable backlog is growing. | Run one bounded cleanup attempt, then accept only if required headroom remains. A proven refusal before mutation is retryable if capacity may recover; reject if the request cannot fit the fixed configured limit. | Keep diagnostic work bounded; operations that write durable state still require their own reserve and durability boundary. |
| `critical` | Available space is at or below a measured reserve, capacity evidence is unavailable/stale, or cleanup cannot make progress. | Refuse new capacity-consuming operations before mutation when enforcement is configured and the check cannot establish safe headroom. | Do not advance a checkpoint without a successful durable update. Keep health/metrics reads independent of writes to the pressured data volume where possible. |
| `full` | The operating system reports `ENOSPC`, quota exhaustion, or an equivalent durable-write failure. | Do not claim non-application when a write or sync may have started; report the stage-aware outcome and require intent resolution where available. | Preserve the prior committed floor/checkpoint. Bounded cleanup or recovery may retry after capacity returns. |

The pressure state should have measured hysteresis so a broker does not
oscillate around one allocation boundary. It must not make liveness depend on
a successful write. Metrics and a topology-free administrative description
should remain scrapeable while the data volume is full; exact surfaces and
numeric thresholds are implementation evidence, not policy decisions.

For clustered durability, a future selected durability profile may allow the
leader to commit a publish when the required quorum can persist it. If one
follower is full while that quorum remains writable, the cluster may continue
under the configured profile but must expose degraded redundancy and prevent
that replica from being treated as repaired. If the quorum cannot persist, the
publish is rejected or retryable before commit. A full follower must never make
an already committed publish appear uncommitted or permit a replacement to
serve an unvalidated empty state. The current implementation has no
user-selectable durability profile; these are target-contract questions whose
failure assumptions must be stated for the configured membership. The current
static replacement boundary in
[ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) remains in
force.

A client request with a stable request identity can resolve an ambiguous
publish after restart or a retry. A request without such identity cannot
reliably distinguish “not appended” from “appended but response lost”; the
accepted v2 outcome contract in ADR 0031 requires `unknown` rather than an
unsafe blind retry. Existing provisional v1 clients may continue to see a
generic storage error; v2 outcome encoding and runtime behavior remain
unimplemented.

### Interrupted cleanup

The accepted safety rule is that the logical floor becomes authoritative before
physical deletion can remove history that it declares unavailable. One
candidate cleanup mechanism uses immutable, format-tagged segments and a
durable manifest or equivalent metadata with this shape:

1. stop or rotate the active append target at a record boundary;
2. select a monotonic candidate floor using time, size, durable-consumer and
   active-delivery fences,
   and the stream policy;
3. write and sync a new manifest/state record that makes the candidate floor
   authoritative;
4. atomically publish that manifest and sync its parent directory;
5. delete or quarantine only segments no longer referenced by the published
   manifest, in bounded batches; and
6. sync the directory and record completion or failure metrics.

With this candidate, the logical floor becomes authoritative only after step
4. If the process stops before then, the old manifest and all old records
remain valid. If it
stops after then, unreferenced old segments may remain as reclaimable orphans,
but no referenced segment may have been deleted first. Startup must reconcile
orphaned temporary files and old unreferenced segments idempotently. It must
never rebuild a lower logical floor merely because deletion was interrupted.

Cleanup must not truncate the active segment, rewrite a shared segment in
place, or hold a stream lock while performing unbounded deletion. A bounded
maintenance worker may seal one segment, publish one manifest generation, and
delete a limited byte budget per turn. Foreground publish, poll, acknowledge,
health, and shutdown work retain reserved execution capacity.

In the clustered engine, retention policy and the logical floor are committed
state-machine facts. Each replica may perform the physical cleanup locally
after applying the same floor. A failed local deletion leaves extra physical
bytes and a pressure metric; it does not roll back the replicated floor or
create a different replay result on that replica. A leader change recomputes
only from committed policy/state and can safely retry an idempotent floor
advance.

## Restart and failure recovery

### Local process and storage recovery

Recovery must distinguish these cases:

- A complete synced record remains readable after restart, even if it is in an
  older segment and outside the in-memory tail index.
- A torn final frame is truncated or discarded exactly as the current recovery
  contract specifies. A malformed complete frame fails closed rather than
  being mistaken for a retention boundary.
- A crash before manifest publication leaves the previous retained floor and
  records usable. A crash after publication but before deletion leaves an
  authoritative floor plus reclaimable orphans.
- A failed manifest, checkpoint, or directory sync never reports the logical
  retention advance or acknowledgement as complete.
- A consumer checkpoint remains monotonic. Under the accepted protected
  policy, a record that was not durably acknowledged remains behind the
  protected cursor fence and is never silently skipped. A future expiry mode
  would need a separate decision and an explicit unavailable-history result.
- A request-aware publish with a stable identity can be resolved from durable
  request identity after an ambiguous response. A request without identity
  remains unknown to the broker's caller after a possible durable write.

Startup should validate every referenced segment's format, offset continuity,
timestamp bounds, length bounds, and checksum before serving it. It should
rebuild bounded lookup metadata without reading the entire history into memory.
If a manifest references missing or corrupt retained data, startup must fail
closed with a diagnostic that identifies the logical stream and generation,
not start with an apparently empty stream. Recovery and orphan cleanup need
separate bounded time and byte budgets so a large backlog cannot make the
process appear healthy while it performs unbounded work.

### Cluster restart, leader failure, and replacement

Retention configuration, policy changes, and logical floors must be part of
the durable replicated state covered by the data group's applied log and
snapshot. The first contract has no replay/session state; if a later decision
adds state that affects eligibility, it must also be replicated. Leader-
selected time cutoffs and retention decisions must be carried in commands;
followers must not independently choose wall-clock values.

After a leader crash:

- a new leader may continue only from committed retention state;
- an uncommitted floor advance cannot make history unavailable;
- a committed floor remains valid even if physical deletion was incomplete;
- an in-flight publish, cleanup, or policy response may be retried using its
  stable identity, with `unknown` preserved when commit cannot be determined;
- consumer delivery tokens and lease fencing retain their existing semantics;
  cleanup must not turn a stale acknowledgement into a valid one.

Snapshots must include the retained floor, policy version, segment/extent
manifest metadata, durable consumer progress, and producer deduplication state
needed to interpret the retained prefix. The first contract has no replay
session state to snapshot. Snapshot installation remains staged and validated.
An interrupted transfer may restart
from byte zero under the current mechanism; partial receiver state must never
be exposed as a serving stream. An empty or inconsistent replica is not a
normal replacement path, as recorded in [Raft recovery and replacement research](../research/raft-recovery-and-replacement.md).

### Current runtime versus accepted contract versus implementation hypotheses

| Concern | Current runtime at the supplied baseline | Accepted target contract | Remaining implementation hypothesis |
| --- | --- | --- |
| Retention | All complete broker history remains; no automatic time/size deletion. | Unlimited by default; explicit age/size targets are eligibility rules, with a monotonic logical floor and protected durable progress. | Whether cleanup uses segments, extents, or another recoverable representation. |
| Lagging consumers | Durable contiguous progress and out-of-order acknowledgements are persisted; no retention inventory exists. | Lagging progress or an active delivery may hold the floor above target; incomplete inventory blocks deletion. No expiry, implicit reset, or cursor clamp. | A complete bounded source for local and clustered durable consumer state. |
| Replay | The bounded offset read is explicit, read-only, and returns `history_unavailable` for a missing offset; time selection is not implemented. | An offset below the floor returns `history_unavailable`; time selection follows ADR 0038 and proves deleted-prefix completeness before returning a match or `no_match`. Neither selector pins history or alters ordinary progress. | A bounded timestamp lookup and complete deleted-prefix maximum; replay sessions and multi-record operations remain separate choices. |
| Dead-letter history | Local movement appends or reconciles the target record before source progress; clustered movement is atomic in its data group. | Broker-managed dead-letter output remains unlimited until move reconciliation no longer depends on target history. | A durable move ledger or other bounded representation, if finite dead-letter retention is later needed. |
| Disk admission | Bounded protocol/executor admission exists, but no physical reserve or capacity provider. | Logical retention cannot overrule protected history. Configured physical enforcement reserves required headroom and refuses before mutation when safe admission cannot be established; stage uncertainty remains explicit. | Capacity provider, measured reserve, pressure thresholds, and cluster-local reporting. |
| Cleanup | No cleanup operation exists. | The logical floor becomes authoritative before physical deletion can remove history it declares unavailable; cleanup lag remains visible. | Manifests/segments, directory-sync sequence, orphan cleanup, and scheduling. |
| Observability | `storage_bytes` and general request/snapshot metrics; no complete consumer catalogue or capacity measurement. | Distinguish logical retained/reclaimable/protected overage from physical availability/reserve. Unknown freshness or coverage is not zero; existing `storage_bytes` keeps its meaning. | Bounded administrative descriptions, metric names, freshness deadlines, and scrape-cost evidence under ADR 0028. |

The accepted target-contract column records ADR 0036, not current runtime
behavior. The implementation column is not a required file layout or module
plan. Nothing in this design authorizes a runtime change by itself.

## Outcome and evidence gates

The following gates describe the outcomes and evidence needed before a future
implementation is accepted. They are deliberately not a task list: an
implementation may choose different storage, scheduling, or protocol
mechanisms if it preserves the stated invariants and evidence boundary. Each
gate should leave the repository runnable and should stop if its exit evidence
is incomplete.

### Gate 0: establish implementation evidence boundaries

- ADR 0036 accepts unlimited defaults, protected consumer progress, logical
  size accounting, replay-floor outcomes, and stage-aware physical admission.
- Implementation architecture still needs a bounded capacity provider and a
  complete durable-consumer inventory without changing the public contract.
- Deterministic unit evidence has a test-clock and capacity-provider boundary,
  while real filesystem/process tests cover actual failure behavior.
- A baseline artifact captures local and clustered startup, replay, cleanup
  (not present), publish, poll, acknowledgement, memory, and storage behavior.

Exit evidence: bounded evidence interfaces and a baseline artifact whose
workload and durability boundaries are explicit.

### Gate 1: introduce segmented retained storage without deleting history

- A versioned segment/manifest abstraction exists behind `runnel-core`, while
  current record readers and request identities remain readable.
- New appends roll at bounded size/time boundaries and old segments remain
  read-only during migration.
- Startup recovery validates manifest generations, segment checksums, offset
  continuity, and incomplete tails without loading all records.
- A clustered retained-data abstraction remains distinct from the consensus
  log and is not exposed through `runnel-engine`.

Exit evidence: mixed old/new read and restart tests, no history loss, bounded
startup memory, and a recovery benchmark over history larger than the current
tail index.

### Gate 2: implement local logical retention and crash-safe cleanup

- Retention policy and a monotonic retained floor are persisted per stream.
- Time and size candidate selection obeys the accepted protection rule; a
  complete inventory is mandatory before the floor can advance.
- Rotation precedes deletion; manifests publish atomically; old segments are
  deleted in bounded, restartable batches.
- Unavailable-history errors distinguish logical floor advancement from
  physical bytes still awaiting deletion.

Exit evidence: local time/size, lag, active-delivery, restart, interrupted
cleanup, and no-silent-gap tests pass with real process coverage where a
filesystem or socket boundary is involved.

### Gate 3: validate retained-floor replay and consumer lifecycle semantics

- The accepted bounded offset read returns the requested record or an explicit
  `history_unavailable` result from one authoritative view; it does not create
  delivery state or pin history. Existing poll/ack semantics remain unchanged.
- No replay progress or session state is introduced by this gate. If a durable
  replay session or multi-record operation is proposed later, its lifecycle
  and any retention pin require a separate decision.
- No expiry mode is implemented by this contract. Any future destructive
  expiry requires a separate decision and tests for lagging/unacknowledged
  work and acknowledgement fencing.

Exit evidence: conformance tests cover local replay, normal delivery, grouped
delivery, key ordering, concurrent acknowledgement, restart, and retention
policy changes.

### Gate 4: replicate retention facts and clean clustered data safely

- Versioned retention policy and floor state live in the appropriate
  metadata/data-group state, with leader-selected cutoffs carried in
  deterministic commands.
- Snapshot serialization includes the retention floor and policy metadata
  needed to interpret retained state; there is no replay-session state in the
  first contract.
- Replica cleanup occurs locally only after the committed floor is applied;
  failed deletion is reported without diverging logical eligibility.
- Capacity behavior is tested for a full follower, loss of quorum, leader
  change during cleanup, and controlled future replacement without weakening
  the selected commit boundary.

Exit evidence: three real broker processes demonstrate time/size retention,
  protected lag, follower/leader failure, snapshot recovery, interrupted cleanup,
  and no observation of an uncommitted or logically expired record.

### Gate 5: add reserved-capacity admission and operator surfaces

- Capacity detection, configured limits, reserve validation, pressure
  hysteresis, and bounded cleanup scheduling are observable.
- Connection/request/storage execution limits remain separate from disk reserve;
  configuration and runtime status stay bounded.
- Topology-free configuration inspection, metrics, logs, and readiness
  semantics preserve liveness and metrics availability at critical pressure.
- Versioned publish outcomes distinguish confirmed rejection, retryable failure,
  and unknown durable result while keeping request identity resolution explicit.

Exit evidence: real server tests cover low space, full disk, external capacity
changes, stalled cleanup, acknowledgement under pressure, ambiguous publish,
and recovery after capacity returns.

### Gate 6: migration, hardening, and performance acceptance

- Upgrade, downgrade, retention-policy transition, and local-to-clustered
  migration boundaries are documented before finite retention is enabled in
  production.
- The full failure and process-level matrix below covers fault injection at
  every manifest and durable-write boundary.
- The retention benchmark matrix follows the repository's authoritative
  benchmark policy and publishes raw measurements and resource limits.
- ADRs record accepted consequences and deferred `expire`, replay, or
  replacement behavior; implementation-facing docs change only after runtime
  verification.

Exit evidence: compatibility, failure, resource, and stable benchmark reports
support a recommendation to enable the selected defaults.

## Invariants

These properties should be executable in state-machine, storage, engine
conformance, and process-level tests.

### Safety and semantics

- A committed durable publish remains readable until the selected retention
  policy makes it ineligible. The accepted protected policy never deletes a
  record at or after durable contiguous progress or an active delivery.
- Retention floor offsets and policy generations are monotonic. No restart,
  leader change, or cleanup retry may lower a floor or reinterpret a policy
  generation.
- Only complete, validated history no longer referenced by the authoritative
  floor can be physically reclaimed. A specific segment or extent format is
  not part of the accepted contract.
- A poll or replay request below the floor receives an explicit
  `history_unavailable` result; the broker never converts a gap into `Empty`,
  silently advances a checkpoint, or delivers a later offset as a substitute.
- Acknowledgement persistence precedes an acknowledgement success. A failed
  checkpoint leaves the previous durable progress authoritative.
- An acknowledgement token from an expired, deleted, or superseded delivery
  cannot acknowledge a later delivery of the same record.
- A published-time retention decision cannot delete early because a wall clock
  moved backwards or because replicas applied a command at different times.
- A publish is accepted only after its selected local or quorum durability
  point. An I/O error after a possible durable write is never presented as a
  confirmed rejection without a resolution path.
- Retention cannot be inferred from Raft-log compaction. A consensus snapshot
  or log purge does not by itself make a broker record unavailable.

### Resource and operational safety

- Physical writes, cleanup work, manifest temporary space, recovery scans, and
  in-flight requests have explicit bounds. Future replay sessions, if accepted,
  need their own bound.
- The reserved capacity remains available for the largest configured legal
  durable operation and its required metadata/sync work; a cleanup attempt
  cannot consume the entire foreground execution budget.
- Pressure transitions have hysteresis and remain observable without writing
  to the full data volume.
- Cleanup is idempotent. Every crash point leaves either the old valid
  manifest or a newer valid manifest plus reclaimable orphans.
- Startup fails closed on missing, corrupt, ambiguous, or identity-mismatched
  retained state. It never serves an empty stream as a substitute for failed
  recovery.
- Local and clustered implementations expose the same application outcomes;
  physical capacity, replica count, and cleanup implementation stay inside
  their engine/operational boundaries.

## Compatibility and migration boundaries

### Record and state formats

The current `RNL1`, versioned, and request-aware readers remain readable during
the first segmented-storage migration. Existing one-file logs should be
opened read-only and rolled into new segments at an explicit boundary; an old
writer must never truncate or append to a format it does not understand. A
segment format version, checksum coverage, record offset range, timestamp
range, and manifest generation must be self-describing.

Existing consumer checkpoints map to a retained floor of zero and retain their
committed offset and attempt state. Existing request-identity mappings must be
rebuilt or migrated before a publish can be acknowledged as idempotently
resolved, and recovery must exclude IDs whose records are below the committed
retained floor. A missing or invalid checkpoint is a startup/storage error, not
a new consumer at offset zero.

The clustered state-machine, journal, and snapshot formats need an explicit
version bump or additive migration for policy, floor, and replay fields.
Snapshots produced before retention fields exist mean unlimited retention and
must not be interpreted as an implicit finite policy. A snapshot with a
retention floor must not be installed over data whose manifest identity or
stream identity does not match.

### Policy changes

- Unlimited to finite retention is potentially destructive and requires an
  explicit operator acknowledgement. It cannot be an automatic upgrade step.
- Tightening a policy may delete eligible history but never restores it when
  the policy is later relaxed. A relaxed policy applies only to future data.
- Any future expiry policy, deleting a consumer, or expiring a replay session
  can remove an application's replay entitlement and needs a separate
  explicit, auditable decision and operation.
- The broker must reject an unknown policy version or field combination rather
  than choosing a more destructive fallback.

### Clients, engines, and deployment

The JSON-lines listener remains provisional v1 without a cross-release
compatibility promise. ADR 0031 accepts the negotiated v2 framing and outcome
contract; its runtime negotiation and encoding remain unimplemented. Any new
replay or retention-configuration operation must be defined under that
version/capability contract. An unavailable-history result must never be
presented as an ordinary empty poll.

Local-to-cluster migration remains unsupported until a separate, versioned
migration workflow fences writers, transfers retained data and consumer state,
resolves producer identities, and establishes the destination durability
boundary. Changing `--engine`, reusing a clustered data directory, or changing
cluster/node identity is not a migration procedure. Kubernetes remains a
packaging/deployment surface; it cannot substitute for process-level recovery
or storage compatibility tests.

## Observability and operator behavior

The existing metrics should keep their current meanings while any new signals
remain bounded and documented. Default Prometheus metrics must not label stream,
consumer, member, key, offset, request ID, or delivery token. Use fixed reason
labels for bounded dimensions; expose identity-specific detail only through a
separate bounded administrative description if that interface is later
accepted.

Consumer-lag signals must follow the accepted observation semantics in
[ADR 0028](../decisions/0028-consumer-lag-observation-semantics.md): cursor lag
is `H - C` only when head and contiguous durable cursor share a source revision,
meet an explicit freshness deadline, and `F <= C <= H`, where `F` is the
retained floor. Report `retention_expired` and no numeric lag when `F > C`; a
cursor above `H` is unknown. Keep freshness and coverage separate, and never
map unknown, stale, incomplete, absent, or expired state to zero or caught-up.
Count a shared-consumer group once, keep in-flight deliveries separate, and
publish an aggregate only for its complete declared consumer scope. ADR 0028
accepts these semantics, not a consumer catalogue, runtime query, metric family,
freshness deadline, or coverage promise; those remain design and implementation
gates.

### Gauges and status

At minimum expose:

- physical broker storage bytes and logical retained record bytes, with their
  measurement definitions;
- detected available capacity, configured capacity limit, reserved bytes,
  required headroom, and current pressure state;
- retained bytes, reclaimable bytes, retention overage, logical floor offset,
  logical floor time, and the number of durable consumers constraining each
  stream through a bounded inspection that can establish complete coverage.
  Do not encode stream identity in default metric labels;
- cursor lag for a complete declared consumer scope, with separate freshness
  and coverage signals; any byte-lag or per-stream constrained-consumer count
  needs a bounded source and explicit measurement definition;
- cleanup in progress, selected cleanup budget, last successful cleanup time,
  and pending orphan bytes;
- clustered redundancy that is below reserve, unable to clean, or not caught
  up sufficiently to satisfy the selected durability mode.

Names should follow the current `runnel_*` convention. Exact names and allowed
fixed labels belong in the implementation ADR and metrics tests. The existing
`runnel_storage_bytes` definition must not silently change.

### Counters, histograms, and diagnostics

Add counters for cleanup attempts, successful segment/extent deletion, bytes
reclaimed, cleanup failures by fixed reason, pressure transitions, publish
rejections by fixed reason, ambiguous durable writes, unavailable-history
responses, policy changes, and recovery failures. Add bounded
histograms for cleanup duration, bytes reclaimed per cycle, recovery duration,
time spent in low/critical pressure, and durable append/checkpoint failures.

Log messages should include stream identity, policy generation, logical floor,
pressure state, capacity class, and retry guidance. They should not require a
client to parse a physical path or topology identifier. A topology-free
administrative description should show configured policy, current eligibility
boundary, lag constraint, pressure state, and last cleanup result.

Liveness should answer whether the process is running. Readiness should remain
false for failed initialization or a cluster that cannot satisfy its selected
durability boundary; it should not flap solely because a protected consumer is
slow. Critical local storage pressure should be a distinct degraded condition
that operators can alert on even if reads and acknowledgements still work.
Metrics and health checks must have bounded execution and must not require a
new durable write.

## Failure and process-level test matrix

Unit and state-machine tests should establish deterministic policy decisions,
while real broker processes and persistent temporary storage prove filesystem,
server, and cluster behavior. Network cases must use the real server, as in
[testing.md](../testing.md).

| Scenario | Engine/scope | Fault or setup | Required result |
| --- | --- | --- | --- |
| Time-only retention | Local, then clustered | Controlled broker time; records before/after the cutoff; active segment present. | Only complete segments strictly older than the cutoff are candidates; boundary records remain; floor and metrics are correct. |
| Size-only retention | Local, then clustered | Small logical-byte target with records of different key and payload sizes. | Oldest eligible complete segments are removed until within target or a documented granularity/fence prevents it; no partial record disappears. |
| Durable publish versus future visibility | Local, then clustered | In separate size-only and age-only cases with no consumer, publish one record larger than `max_bytes` or explicitly set `max_age=0`; advance the deterministic broker clock past the publish time in the age case. | Publish first reports confirmed at the durable point. Once eligible, cleanup may advance the floor past the record; replay then returns `history_unavailable`, and a new consumer starts at the new floor. There is no newest-record grace or promise that the response means a future consumer can read it. |
| Request-ID retention horizon | Local, then clustered | Publish with a stable ID; retry before retention removes the record, then expire its record and retry the same ID. | Before expiry, retry returns the original receipt without append. Floor advancement retires the ID mapping with the record; after expiry, reusing the ID creates a new record at a new offset, consistent with ADR 0031's end of the deduplication horizon. |
| Both limits | Local | One limit violated before the other and then both violated. | Either trigger can select deletion; cleanup converges toward both targets and reports overage when it cannot. |
| Protected lagger and new consumer | Local process | Consumer remains below the age/size target while publishes continue; a new consumer and replay request start below the floor. | Floor does not pass durable progress; overage is visible; a new consumer starts at the floor; replay or an inconsistent old cursor receives `history_unavailable`; publish refuses only at reserve pressure and never by deleting protected history. |
| Active delivery fence | Local process | Retention becomes due while a delivery lease is unexpired. | Its history remains protected; old acknowledgement retains current stale-token behavior after lease expiry; durable progress still fences the floor. |
| Dead-letter move and cleanup race | Local process, then clustered | A target append completes while source acknowledgement or retention cleanup is interrupted; repeat after restart. | A local move identity reconciles the same-content target before source progress advances; retention does not delete target evidence needed for reconciliation. Clustered same-group movement remains one replicated transition; a future split-group target needs separate evidence. |
| Grouped out-of-order acknowledgement | Local and cluster | A later grouped offset is acknowledged while an earlier one is in flight. | Deletion uses contiguous progress only; the earlier message/key cannot be reclaimed prematurely. |
| Read-only replay during floor advance | Local process | Race one offset read with a committed floor advance. | The read returns either the requested record or `history_unavailable` from one authoritative view; it never returns a later offset or pins history. |
| Preflight low space | Local process | Constrain free space or use a capacity-provider test double, then publish. | Cleanup is bounded; a publish that cannot preserve reserve is refused before append; read-only health/replay work continues. |
| Full disk between checks | Local process | Consume headroom after preflight, fail append or sync with `ENOSPC`. | No false success; response is `unknown` if the write may have crossed its durable stage, otherwise a proven pre-write refusal is retryable or rejected according to configuration. |
| Full disk during ack | Local process | Fail the consumer-state replacement or directory sync. | No acknowledgement success is reported before persistence; if application is uncertain, expose `unknown` and resolve from durable state instead of assuming that progress stayed unchanged. |
| Cleanup crash before manifest | Local process | Kill the real broker after temporary manifest write and before publish. | Old manifest and all referenced history recover; temporary state is discarded or retried. |
| Cleanup crash after manifest | Local process | Kill after manifest publication but during segment deletion. | New floor remains; old unreferenced segments are reclaimable orphans; restart does not resurrect the floor. |
| Cleanup deletion failure | Local process | Make one segment undeletable or return an I/O error. | Logical policy remains deterministic; physical reclaimable bytes and failure are visible; later cleanup retries. |
| Torn/corrupt segment | Local process | Kill during append; separately corrupt a complete frame or manifest. | Torn tail follows the accepted recovery rule; complete corruption fails closed with a diagnostic; no empty-stream recovery. |
| Restart after retention | Local process | Close/restart with retained and deleted prefixes plus old consumers. | Retained records and consumer progress match the policy; deleted prefixes return explicit unavailable history. |
| Leader failure during floor command | Three processes | Kill leader before response and during local cleanup. | Only committed floor affects eligibility; a retry/new leader is idempotent; no uncommitted history is hidden. |
| Full follower with writable quorum | Three processes | Exhaust one replica's capacity while the existing commit boundary may still be writable. | The existing commit boundary remains authoritative; cluster reports local pressure and does not call the follower repaired. This test does not authorize a smaller quorum. |
| Loss of writable quorum | Three processes | Exhaust or stop enough replicas to prevent durable commit. | New publishes reject/retry without becoming visible; reads/health behavior follows the selected degraded policy. |
| Cluster snapshot after retention | Three processes | Advance floor, compact, restart, and install a snapshot on a test replacement. | Snapshot contains policy/floor/consumer state; no expired record is resurrected; replacement remains within the accepted recovery boundary. |
| Interrupted snapshot transfer | Three processes | Interrupt transfer at multiple chunks. | Receiver never serves partial state; retry from the current supported boundary is safe and metrics count the interruption. |
| Policy migration | Local and cluster | Upgrade old state; apply unlimited-to-finite changes. | Old state means unlimited retention; the finite transition is explicit and durable/auditable. |
| Process shutdown under pressure | Real server | SIGTERM during cleanup, publish, ack, and full-disk handling. | Admission stops, bounded work drains within the existing shutdown contract, and restart recovers a valid state. |

The process tests must assert child-process liveness and preserve broker logs
when a child exits unexpectedly. A passing client assertion is not evidence
that all clustered nodes remained alive, as documented by the recovery
research.

## Benchmark and resource plan

This documentation-only change does not require a runtime benchmark. The
eventual implementation changes storage and admission hot paths, so it must
use the repository's authoritative benchmark policy rather than relying on a
microbenchmark or a successful smoke test.

### Workloads

Run local and real three-node clustered workloads for 100-byte and 1-KiB
payloads, with and without ordering keys, using:

- retention disabled (compatibility baseline), time-only, size-only, and both;
- protected retention with a fully caught-up consumer, a slow consumer, and a
  stopped consumer;
- continuous publish while cleanup catches up, including a cleanup backlog;
- replay at, above, and below the retained offset boundary;
- low-space headroom, cleanup failure, restart, leader failure, and recovery;
- local durable publish and publish/poll/ack; clustered quorum publish and
  follower-forwarded delivery; and
- segment sizes and cleanup budgets large enough to expose sequential I/O,
  fsync, manifest, snapshot, and deletion amplification.

The existing [Criterion suite](../../crates/runnel-core/benches/broker.rs),
`just bench-container`, and `just bench-cluster` establish useful starting
workloads. `just bench-pr-local` is required after committing any implementation
change whose hot-path cost plausibly changes; it must report the exact revision,
workload, durability boundary, repetitions, stability result, raw ranges,
matched medians, and outlier diagnostics. Diagnostic or inconclusive runs are
not performance evidence under [ADR 0020](../decisions/0020-stable-optimization-evidence.md).

### Measurements and acceptance evidence

Record, per scenario and revision:

- publish, poll, acknowledge, replay, and cleanup throughput;
- p50, p99, and p99.9 foreground latency, plus cleanup latency and duration;
- time to reclaim a known byte backlog and maximum observed overage;
- physical bytes written/read, encoded retained bytes, logical payload bytes,
  filesystem free space, fsync latency, and storage amplification;
- startup and restart recovery duration, bytes scanned, bytes transferred,
  snapshot size, and time to resume durable traffic;
- broker CPU time, CPU efficiency, resident memory peak/average, allocation
  rate where available, open files, background-worker count, and queue depth;
- number and size of in-flight/replay/cleanup items and the effect of a
  protected lagger on memory; and
- publish rejection, retry, unknown-outcome, cleanup-failure, and recovery
  counts under pressure.

Acceptance should demonstrate that retained history no longer causes linear
startup memory or unbounded cleanup queues, and that cleanup can keep up with
the intended sustained workload or drives explicit admission before reserve
violation. The retention-enabled foreground p99/p99.9 tradeoff must be
reported against retention-disabled baseline; no claim of improvement is
valid without stable same-host evidence. Cluster measurements must identify
whether they exercise the changed data path, and must separately report the
case where a follower is full or cleanup is blocked.

The first implementation should not set a universal numeric recovery or
throughput target before these measurements. It should set hard correctness
gates, a bounded-resource budget, and a documented p99/p99.9 regression policy
in the ADR, then use the existing repeated-range rules to distinguish stable
direction from host noise.

## Unvalidated operational hypotheses

No numeric reserve, pressure watermark, cleanup interval, or cleanup budget is
supported by current measurements. Earlier example values are removed from
this plan rather than treated as launch defaults. The evidence needed to set
them is described in [Retention and disk-pressure semantics](../research/retention-disk-pressure-semantics.md)
and the benchmark plan below.

The remaining hypotheses are implementation prompts, not alternate defaults:

- New and existing streams default to unlimited history; a finite age or size
  target must be explicitly configured. Determine how the durable consumer
  inventory is made complete and bounded before enabling cleanup. Durable
  replay sessions are not part of the selected first policy.
- The selected protected policy permits target overage. Measure how often
  protected lag prevents reclamation and how physical admission should expose
  the resulting write refusals. Any destructive expiry mode remains a separate
  product decision, not a fallback.
- Derive reserve and cleanup budgets from the largest legal durable mutation,
  bounded concurrency, manifest/checkpoint/snapshot work, filesystem behavior,
  and actual recovery needs. Measure them on supported deployment storage.
- Choose pressure hysteresis and cleanup cadence from measured reclaim rate,
  workload burst, segment granularity, and foreground tail latency.
- Preserve capacity for health, metrics, acknowledgement/recovery work, and
  shutdown only where those operations have an explicit durable-write budget;
  do not imply they can succeed after the filesystem has refused the required
  write.

The current illustrative Kubernetes values (10 GiB claims, 1 GiB memory,
five-minute startup probe, and 30-second termination grace) should remain
unchanged by this design document. Once implementation exists, the deployment
documentation must explain the relationship between those values, detected
capacity, reserve, snapshot working space, and the broker's startup recovery
budget. Kubernetes must not be required for the policy to be safe.

## Unresolved implementation and future-policy decisions

The semantic policy is accepted; the following choices still require evidence
or a separate decision before implementation is considered complete. The
[source review](../research/retention-disk-pressure-semantics.md) organizes
their alternatives and evidence needs:

1. What exact versioned operation configures finite retention, and how is the
   policy change authenticated, inspected, and made durable?
2. Should the public contract ever add a durable replay session or an
   explicitly destructive expiry policy, and what evidence would justify one?
3. Can dead-letter move reconciliation be made durable independently of the
   target record before a future policy permits finite dead-letter retention?
4. How should a clustered engine report local pressure on a follower while
   preserving the existing selected commit boundary and avoiding a new quorum
   promise?
5. Which capacity provider can report usable bytes and relevant quotas for the
   supported filesystems, and how should platform-specific unknown coverage be
   surfaced?
6. Which storage representation and snapshot format can provide bounded
   recovery and idempotent cleanup without exposing physical layout?
7. What downgrade boundary applies after a floor or finite retention policy
   has been persisted?
8. Which explicit operator operation, if any, may remove an abandoned durable
   consumer's protection and expose an unavailable-history boundary?
9. Which readiness state and alerts distinguish a protected-lag write refusal
   from an inability to accept the selected durable writes?
10. What measured reserve, hysteresis, cleanup budget, and p99/p99.9 limits
    fit the supported workloads and deployment storage?

## References

The proposal is grounded in the current code and the following accepted or
exploratory records:

- [Current architecture](../architecture.md)
- [Product backlog](../backlog.md)
- [Testing and local operation](../testing.md)
- [Technical debt register](../tech-debt.md)
- [ADR 0001: single-node durable log](../decisions/0001-single-node-durable-log.md)
- [ADR 0007: snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [ADR 0013: local shared consumer delivery](../decisions/0013-local-shared-consumer-delivery.md)
- [ADR 0014: local retry and dead-letter policy](../decisions/0014-local-retry-and-dead-letter-policy.md)
- [ADR 0015: clustered shared-consumer ownership](../decisions/0015-clustered-shared-consumer-ownership.md)
- [ADR 0016: clustered retry and dead-letter policy](../decisions/0016-clustered-retry-and-dead-letter-policy.md)
- [ADR 0018: safe replica recovery boundary](../decisions/0018-safe-replica-recovery-boundary.md)
- [ADR 0019: clustered storage identity](../decisions/0019-clustered-storage-identity.md)
- [ADR 0020: stable optimization evidence](../decisions/0020-stable-optimization-evidence.md)
- [ADR 0023: independent retained storage and placement](../decisions/0023-independent-retained-storage-and-placement.md)
- [ADR 0024: explicit offset replay](../decisions/0024-explicit-offset-replay-read.md)
- [ADR 0026: semantic engine error classification](../decisions/0026-semantic-engine-error-classification.md)
- [ADR 0027: consumer-scoped retry policy](../decisions/0027-consumer-scoped-retry-policy.md)
- [ADR 0028: consumer-lag observation semantics](../decisions/0028-consumer-lag-observation-semantics.md)
- [ADR 0036: retained-history and disk-pressure contract](../decisions/0036-retained-history-and-disk-pressure-contract.md)
- [Local engine storage and delivery implementation](../../crates/runnel-core/src/lib.rs)
- [Clustered state-machine and group manager](../../crates/runnel-raft/src/lib.rs)
- [Server admission and metrics](../../crates/runnel-server/src/main.rs)
- [Illustrative Kubernetes deployment](../../deploy/kubernetes/runnel.yaml)

The existing research notes contain additional primary references for
consensus recovery, replicated-log behavior, and storage alternatives. The
direct retention comparison above records the competitor and storage-research
evidence used for this plan. Implementation tradeoffs not yet measured remain
hypotheses or unresolved decisions above; no competitor behavior is itself an
accepted Runnel policy.
