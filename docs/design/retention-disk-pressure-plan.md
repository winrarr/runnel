# Retention and disk-pressure implementation plan

- Status: exploratory implementation plan
- Scope: safe retained-history policy, bounded cleanup, and durable-write admission
- Related outcome: [Make retention and disk-pressure behavior safe](../backlog.md)
- Related research: [Distributed architecture exploration](../research/distributed-architecture-options.md), [Raft follower recovery and replacement](../research/raft-recovery-and-replacement.md), and [Message encoding and compression study](../research/message-encoding-and-compression.md)

This is a design proposal, not an accepted ADR. It turns the retention and
disk-pressure backlog outcome into an implementation boundary while preserving
the current stream, record, consumer, acknowledgement, replay, and ordering
model. It does not change runtime code, the backlog, deployment files, or the
current compatibility policy.

## Outcome and boundaries

Runnel should be able to retain a bounded amount of stream history without
silently losing a message that a configured policy still promises to deliver,
and without accepting a publish that cannot reach its selected durable point.
The implementation must make the following visible to an operator and, where
it affects an application operation, to the client:

- which time and size rules are configured;
- whether a consumer or replay session is pinning history;
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
| Local storage | `runnel-core` uses one append-only log per stream. It accepts the current `RNL1` and versioned record families, calls `sync_data` before reporting a publish success, scans the complete log at open, keeps a bounded tail index and sparse lookup window, and truncates an incomplete trailing frame on recovery. The legacy parser's malformed-UTF-8 edge case still needs an explicit compatibility test. | Retention cannot safely delete a prefix of one mutable file. Segmentation, format metadata, and a durable retained-history floor are prerequisites. |
| Local consumers | Consumer state is a JSON checkpoint with a contiguous committed offset, out-of-order acknowledgements, and persisted delivery attempts. Active deliveries and their deadlines are in memory; a restart may redeliver an unacknowledged message. | A safe deletion watermark must use contiguous committed progress, not the highest acknowledged offset, and must fence active deliveries. |
| Local replay | A new consumer implicitly starts at offset zero and polling follows its checkpoint. There is no explicit replay operation or retention policy today. | A future replay cursor must be distinct from ordinary consumer progress and must return an explicit unavailable-history outcome rather than turning a gap into `Empty`. |
| Local dead letters | The source record is appended to a derived dead-letter stream before the source checkpoint advances. A crash between those writes may duplicate the dead-letter record but cannot silently skip the source record. | Retention must preserve this at-least-once ordering and define whether derived streams inherit or override source retention. |
| Clustered storage | `runnel-raft` keeps complete message vectors in the replicated stream state, writes a state-machine journal, and creates complete materialized snapshots. Raft log compaction is independent from broker history. | The clustered path needs replicated logical retention state but local, interruptible physical cleanup. A snapshot must not resurrect history below the committed retention floor. |
| Clustered consumers | Progress, out-of-order acknowledgements, attempts, in-flight ownership, deadlines, and fencing state are in the stream data-group state. Grouped polls and acknowledgements are leader-authorized writes. | Retention decisions affecting a cluster must be deterministic state-machine facts; local filesystem inspection cannot itself decide a replicated watermark. |
| Admission | The server bounds connections, request frames, in-flight requests, and request duration. Local storage work has a bounded executor. Storage errors map to a generic `storage_error`; the provisional protocol has no explicit unknown publish outcome. | Disk admission must be checked before append, but races and `ENOSPC` still require an explicit future retry/unknown outcome contract. |
| Metrics and health | `/metrics` exposes request/admission counters, request latency, process-lifetime delivery counters, storage bytes, health failures, and clustered snapshot activity. Local `storage_bytes` is physical log-file length; clustered `storage_bytes` is a logical sum of stored keys and payloads. | Existing storage bytes are not a disk budget. Add retained, reclaimable, reserved, pressure, lag, cleanup, and write-rejection signals without pretending the current gauge is comparable across engines. |
| Deployment | The illustrative Kubernetes deployment gives each of three static-cluster pods an independent 10 GiB claim, 1 GiB memory limit, 1 CPU limit, five-minute startup-probe window, and 30-second termination grace period. It has no broker retention or capacity settings. | A broker capacity policy must work without Kubernetes, use detected available capacity conservatively, and document that PVC size is not by itself free space available to the broker. |

These facts are also tracked as [TD-002](../tech-debt.md), [TD-005],
[TD-006], [TD-009], [TD-010], [TD-017], [TD-019], [TD-022], and [TD-023].
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
- local dead-letter movement remains at least once across its two durable
  writes, including its documented duplicate caveat;
- storage identity checks and the conservative replacement boundary from
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) and [ADR 0019](../decisions/0019-clustered-storage-identity.md) remain in force.

## Proposed policy

The following policy is the recommended starting point. It is deliberately
not accepted until the implementation, failure tests, and measurements below
exist and a dedicated ADR records the compatibility consequences.

### Configuration vocabulary

Each stream should have a durable, versioned retention policy with these
intent-level fields:

```text
RetentionPolicy {
    max_age: optional duration,
    max_bytes: optional bytes,
    lag_policy: protect | expire
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

Absent `max_age` and `max_bytes` mean unlimited automatic retention. This is
the compatibility-safe default for existing streams. Enabling finite
retention is an explicit data-lifecycle choice, not an incidental consequence
of a new storage format.

### Time and size retention

Time and size are independent triggers. A complete immutable segment becomes a
retention candidate when either condition is true:

- its newest record is older than the age cutoff; or
- retained record bytes exceed `max_bytes`.

The broker deletes the oldest eligible complete segments until neither limit
is exceeded, or until no safe segment remains. Thus, when both limits are set,
the stricter trigger wins. A record at the exact time cutoff remains eligible
until it is strictly older than the cutoff. Size overage caused by the active
segment, segment granularity, an active delivery, or a protected consumer is
reported rather than hidden.

The age cutoff is based on the broker-assigned `published_at_ms` already
present in the logical message. A cleanup decision carries a monotonic cutoff
or an equivalent committed timestamp; a wall-clock jump backwards may delay
deletion but must never make a previously protected record disappear early.
The implementation must reject or clearly handle timestamps that would make a
segment's metadata inconsistent rather than using an unbounded per-record
timer.

`max_bytes` should count encoded record bytes owned by the stream, including
record framing and key bytes, but not temporary cleanup files or consensus
logs. The metrics must also expose logical payload bytes and total physical
broker usage because an encoded-byte retention budget is not a filesystem
quota. The exact treatment of compression and metadata is an ADR decision
shared with the [encoding and compression plan](encoding-compression-day1-plan.md).

Retention is segment-granular. Cleanup first seals or rolls the active segment,
then considers old segments. It never removes individual records from a
segment in place. A segment may remain above a target because deleting it would
cross the retention boundary, violate a safety fence, or consume the reserved
cleanup workspace.

### Precedence

When a retention cycle and a publish happen together, apply decisions in this
order:

1. Validate the request and preserve durable-write, checkpoint, active-lease,
   corruption, and identity safety. Disk pressure never authorizes an
   unconfigured destructive policy.
2. Apply the stream's `lag_policy` and replay fences. `protect` can block
   deletion; `expire` can make an explicitly permitted history boundary
   unavailable. Neither policy silently advances a checkpoint.
3. Evaluate `max_age` and `max_bytes` as independent triggers, selecting the
   oldest complete segments that are allowed by the first two steps.
4. Publish a logical floor only through the durable local manifest or
   replicated state-machine boundary, then reclaim physical bytes.
5. Check reserved capacity for the proposed durable write. If cleanup did not
   leave the required headroom, reject or return a retryable/unknown outcome;
   do not delete protected data merely to make the write fit.

This ordering means a configured `expire` policy may trade replay eligibility
for bounded history, but low space cannot silently change `protect` into
`expire`. If no policy permits safe reclamation, admission is the safety valve.

### Lagging consumers and replay eligibility

The safe prefix for a normal consumer is its contiguous committed offset. A
group consumer's highest out-of-order acknowledgement does not free earlier
history until the gap is closed. A consumer with no persisted state does not
pin history merely by potentially existing; it can replay only the history
that remains when it explicitly starts.

`lag_policy` determines how an existing consumer interacts with retention:

| Policy | Retention behavior | Lagging-consumer outcome | Disk-pressure consequence |
| --- | --- | --- | --- |
| `protect` | A durable consumer's committed offset, an active delivery, and an active protected replay session constrain the deletion watermark. A lagging consumer may keep the stream above its time or size target. | The consumer retains its normal at-least-once/replay eligibility while the history is retained. No implicit skip or checkpoint advance is allowed. | If no safe history can be reclaimed before the reserve is reached, new publishes are explicitly rejected or made retryable; polls, acknowledgements, health, and cleanup remain schedulable. |
| `expire` | Time/size retention may advance the logical floor past a lagging consumer once the record is not protected by an active delivery. It is an explicit opt-in loss of replay eligibility. | Polling below the floor returns `history_unavailable` with the earliest eligible boundary. It must not return `Empty`, redeliver a different offset, or silently move the checkpoint. The client must explicitly reset/skip or use a new replay scope. | The stream can converge toward its configured bound, subject to segment granularity and active leases. Pressure is handled by admission if physical cleanup still cannot keep up. |

An unexpired active delivery always protects its segment until the lease
expires, even under `expire`; deleting it while its acknowledgement could
still arrive would make the delivery outcome ambiguous in a way the policy did
not disclose. Once the lease expires, the configured `expire` policy may end
the retry opportunity at the retention floor. An acknowledgement after that
boundary is stale or already unavailable, never a successful acknowledgement
of a missing record.

`protect` must not have an accidental infinite pin from an abandoned consumer.
The first implementation should retain consumer state indefinitely unless the
operator explicitly deletes it or selects an inactivity-expiry policy. An
inactivity expiry is destructive to replay eligibility and therefore must be
visible, durable, and separately confirmed. The exact consumer lifecycle API
is unresolved.

### Replay

Replay should be an explicit operation over an existing consumer identity or a
bounded replay session, not a request to invent a special consumer name. A
future public operation should support at least:

- an inclusive offset selector;
- a published-time selector resolved to the first retained record at or after
  that time; and
- the earliest currently retained boundary.

Starting before the retained floor must return `history_unavailable` and the
available offset/time boundary. A time or offset request that spans a deleted
gap must fail as a whole; silently replaying only the suffix would make the
requested scope unknowable to the caller.

A replay cursor is separate from the ordinary durable consumer checkpoint.
Replay acknowledgements do not move ordinary progress unless a later explicit
operation asks to replace that progress. Replay delivery uses the same
at-least-once, key-ordering, attempt, and stale-token rules as ordinary
delivery. The cursor/session, its generation, and its last delivered position
must be durable before a success response if a client expects replay to resume
after restart.

Under `protect`, an active replay session contributes its earliest unread
position to the deletion fence. It needs a bounded lease, renewal, or explicit
end operation so a crashed client cannot hold a stream forever; expiry must
produce a durable session outcome and a subsequent `history_unavailable` rather
than a silent reset. Under `expire`, the session remains valid only while its
requested records remain retained and reports the same explicit unavailable
boundary when cleanup wins the race.

The exact replay wire schema, session lifetime, and whether replay is read-only
or acknowledgement-driven require a later protocol decision. These
uncertainties must not be resolved by exposing offsets as physical locations or
by overloading the existing poll checkpoint.

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

The proposed pressure states are operational states, not public topology:

| State | Entry condition | New publish behavior | Other behavior |
| --- | --- | --- | --- |
| `normal` | Available space is above reserve plus cleanup/write hysteresis. | Accept only if the projected durable write stays above reserve. | Run cleanup at its normal interval. |
| `low` | Available space is below the low-water mark or reclaimable backlog is growing. | Run one bounded cleanup attempt, then accept only when headroom remains. Otherwise return confirmed `storage_pressure` or a retryable pressure outcome. | Keep health, metrics, polls, and acknowledgements responsive; bound cleanup work so it cannot occupy all foreground capacity. |
| `critical` | Available space is at or below reserve, capacity inspection fails conservatively, or cleanup cannot make progress. | Reject new publishes and stream-creating writes before append. Do not silently wait for space. | Continue reads and health where possible. Acknowledgements may proceed only if their durable state update fits the reserve; otherwise return an explicit retryable storage outcome without advancing progress. |
| `full` | The operating system reports `ENOSPC`, quota exhaustion, or an equivalent durable-write failure. | No new publish is accepted until a fresh capacity check succeeds. A write that may have reached the durable boundary is an ambiguous outcome, not a confirmed rejection. | Preserve the old manifest/checkpoint, surface the failure, and retry cleanup/recovery with bounded work. |

The pressure state should have hysteresis so a broker does not oscillate around
one block boundary. It must not make liveness depend on a successful write.
Metrics and a topology-free administrative description should remain scrapeable
while the data volume is full.

For clustered durability, the leader may commit a publish only when the
selected durability quorum can persist it. If one follower is full while a
quorum remains writable, the cluster may continue committing under the
selected one-failure guarantee but must expose degraded redundancy and prevent
that replica from being treated as repaired. If the quorum cannot persist, the
publish is rejected or retryable before commit. A full follower must never make
an already committed publish appear uncommitted or permit a replacement to
serve an unvalidated empty state. The current static replacement boundary in
[ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) remains in
force.

A client request with a stable request identity can resolve an ambiguous
publish after restart or a retry. A request without such identity cannot
reliably distinguish “not appended” from “appended but response lost”; the
future protocol must expose `unknown` rather than encourage an unsafe blind
retry. Existing provisional clients may continue to see a generic storage
error until the versioned outcome contract is accepted.

### Interrupted cleanup

The storage layer should use immutable, format-tagged segments and a durable
manifest or equivalent metadata describing the retained logical floor. A
cleanup transaction has this shape:

1. stop or rotate the active append target at a record boundary;
2. select a monotonic candidate floor using time, size, consumer/replay fences,
   and the stream policy;
3. write and sync a new manifest/state record that makes the candidate floor
   authoritative;
4. atomically publish that manifest and sync its parent directory;
5. delete or quarantine only segments no longer referenced by the published
   manifest, in bounded batches; and
6. sync the directory and record completion or failure metrics.

The logical floor becomes authoritative only after step 4. If the process
stops before then, the old manifest and all old records remain valid. If it
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
- A consumer checkpoint remains monotonic. A record that was not durably
  acknowledged is either redelivered while retained or reported as explicitly
  unavailable under `expire`; it is never silently skipped.
- A request-aware publish can be resolved from durable request identity after
  an ambiguous response. A request without identity remains unknown to the
  broker's caller after a possible durable write.

Startup should validate every referenced segment's format, offset continuity,
timestamp bounds, length bounds, and checksum before serving it. It should
rebuild bounded lookup metadata without reading the entire history into memory.
If a manifest references missing or corrupt retained data, startup must fail
closed with a diagnostic that identifies the logical stream and generation,
not start with an apparently empty stream. Recovery and orphan cleanup need
separate bounded time and byte budgets so a large backlog cannot make the
process appear healthy while it performs unbounded work.

### Cluster restart, leader failure, and replacement

Retention configuration, policy changes, logical floors, and any replay/session
state that affects eligibility must be part of the durable replicated state
covered by the data group's applied log and snapshot. Leader-selected time
cutoffs and retention decisions must be carried in commands; followers must
not independently choose wall-clock values.

After a leader crash:

- a new leader may continue only from committed retention state;
- an uncommitted floor advance cannot make history unavailable;
- a committed floor remains valid even if physical deletion was incomplete;
- an in-flight publish, cleanup, or policy response may be retried using its
  stable identity, with `unknown` preserved when commit cannot be determined;
- consumer delivery tokens and lease fencing retain their existing semantics;
  cleanup must not turn a stale acknowledgement into a valid one.

Snapshots must include the retained floor, policy version, segment/extent
manifest metadata, durable consumer progress, replay state, and producer
deduplication state needed to interpret the retained prefix. Snapshot
installation remains staged and validated. An interrupted transfer may restart
from byte zero under the current mechanism; partial receiver state must never
be exposed as a serving stream. An empty or inconsistent replica is not a
normal replacement path, as recorded in [Raft recovery and replacement research](../research/raft-recovery-and-replacement.md).

### Accepted behavior versus proposed behavior

| Concern | Accepted at this baseline | Proposed in this plan |
| --- | --- | --- |
| Retention | All complete broker history remains; no automatic time/size deletion. | Explicit per-stream time/size policy, segment-granular cleanup, and a durable logical floor. |
| Lagging consumers | They can replay from their durable offset as long as the one-file history exists. | `protect` pins history; opt-in `expire` reports `history_unavailable` after an explicit floor advance. |
| Replay | Forward polling only; no replay request or eligibility response. | Separate bounded replay cursor/session with offset/time selectors and explicit unavailable boundaries. |
| Disk admission | Bounded protocol admission exists, but no disk reserve or preflight. | Reserved capacity, pressure states, bounded cleanup, and explicit pre-append rejection/unknown outcomes. |
| Cleanup | No cleanup operation exists. | Crash-safe manifest publication followed by idempotent orphan deletion. |
| Local recovery | Incomplete tail recovery; complete history is scanned at open. | Versioned segments, bounded recovery, manifest validation, and cleanup recovery. |
| Cluster recovery | Raft snapshots compact consensus history; empty-replica replacement is test-only. | Replicated retention facts with local cleanup; no weakening of the replacement boundary. |
| Observability | Storage bytes and general request/snapshot metrics. | Retention, lag, pressure, admission, cleanup, recovery, and unavailable-history signals. |

Nothing in the proposed column is a current guarantee or an authorization to
change the runtime in this documentation-only change.

## Implementation sequence

Each stage should leave the repository runnable and should stop if its exit
evidence is incomplete.

### Stage 0: accept policy and measurement boundaries

- Review this plan with the encoding/segmentation work and resolve the
  retention, replay, size-accounting, and unknown-outcome choices.
- Record an ADR for the public policy, retention-floor semantics, capacity
  accounting, and compatibility/version boundaries.
- Define a test clock and capacity provider seam for deterministic unit tests,
  while keeping real filesystem/process tests for actual failure behavior.
- Capture current local and clustered startup, replay, cleanup (not present),
  publish, poll, acknowledge, memory, and storage baselines.

Exit evidence: accepted policy ADR, documented public errors/outcomes, and a
baseline artifact whose workload and durability boundaries are explicit.

### Stage 1: introduce segmented retained storage without deleting history

- Add a versioned segment/manifest abstraction behind `runnel-core`; keep
  current record readers and request identities readable.
- Roll new appends at bounded size/time boundaries and retain old segments
  read-only during migration.
- Make startup recovery validate manifest generations, segment checksums,
  offset continuity, and incomplete tails without loading all records.
- Add a clustered retained-data abstraction distinct from the consensus log;
  do not expose its implementation to `runnel-engine`.

Exit evidence: mixed old/new read and restart tests, no history loss, bounded
startup memory, and a recovery benchmark over history larger than the current
tail index.

### Stage 2: implement local logical retention and crash-safe cleanup

- Persist retention policy and a monotonic retained floor per stream.
- Implement time and size candidate selection with `protect` as the only first
  safe policy if the `expire` semantics are not yet fully tested.
- Rotate before deletion; publish manifests atomically; delete old segments in
  bounded, restartable batches.
- Add explicit errors for unavailable history and distinguish logical floor
  advancement from physical bytes still awaiting deletion.

Exit evidence: local time/size, lag, active-delivery, restart, interrupted
cleanup, and no-silent-gap tests pass with real process coverage where a
filesystem or socket boundary is involved.

### Stage 3: add replay and consumer lifecycle semantics

- Add a versioned replay operation or cursor generation without changing the
  meaning of existing poll/ack requests.
- Persist replay progress and fencing state at the selected durability point.
- Implement protected replay pins, bounded session lifetime, explicit reset or
  skip behavior, and `history_unavailable` boundaries.
- Add opt-in `expire` only after tests prove that lagging and unacknowledged
  work receive explicit outcomes and cannot acknowledge a later generation.

Exit evidence: conformance tests cover local replay, normal delivery, grouped
delivery, key ordering, concurrent acknowledgement, restart, and retention
policy changes.

### Stage 4: replicate retention facts and clean clustered data safely

- Add versioned retention policy and floor state to metadata/data-group state as
  appropriate; carry leader-selected cutoffs in deterministic commands.
- Make snapshot serialization include all retention and replay metadata needed
  to interpret the retained state.
- Let each replica clean locally only after the committed floor is applied;
  report failed deletion without diverging logical eligibility.
- Define quorum capacity behavior for a full follower, loss of quorum, leader
  change during cleanup, and controlled future replacement.

Exit evidence: three real broker processes demonstrate time/size retention,
lag policy, follower/leader failure, snapshot recovery, interrupted cleanup,
and no observation of an uncommitted or logically expired record.

### Stage 5: add reserved-capacity admission and operator surfaces

- Add capacity detection, configured limits, reserve validation, pressure
  hysteresis, and bounded cleanup scheduling.
- Keep connection/request/storage execution limits separate from disk reserve;
  expose useful configuration and runtime status without unbounded queues.
- Add topology-free configuration inspection, metrics, logs, and readiness
  semantics. Preserve liveness and metrics availability at critical pressure.
- Define versioned publish outcomes for confirmed rejection, retryable failure,
  and unknown durable result; keep request identity resolution explicit.

Exit evidence: real server tests cover low space, full disk, external capacity
changes, stalled cleanup, acknowledgement under pressure, ambiguous publish,
and recovery after capacity returns.

### Stage 6: migration, hardening, and performance acceptance

- Document upgrade, downgrade, retention-policy transition, and local-to-
  clustered migration boundaries before enabling finite retention by default.
- Run the full failure and process-level matrix below, including fault
  injection at every manifest and durable-write boundary.
- Run the retention benchmark matrix under the repository's authoritative
  benchmark policy and publish raw measurements and resource limits.
- Record the accepted consequence and any deferred `expire`, replay, or
  replacement behavior in ADRs; update implementation-facing docs only after
  the runtime is verified.

Exit evidence: compatibility, failure, resource, and stable benchmark reports
support a recommendation to enable the selected defaults.

## Invariants

These properties should be executable in state-machine, storage, engine
conformance, and process-level tests.

### Safety and semantics

- A committed durable publish remains readable until the selected retention
  policy makes it ineligible; `protect` never deletes a record still entitled
  to replay by a consumer or protected replay session.
- Retention floor offsets and policy generations are monotonic. No restart,
  leader change, or cleanup retry may lower a floor or reinterpret a policy
  generation.
- Only complete, validated, unreferenced segments are deleted. An active
  segment and any segment containing an unexpired delivery remain available.
- A lagging `expire` consumer receives an explicit unavailable-history result;
  the broker never converts a gap into `Empty`, silently advances its
  checkpoint, or delivers a later offset as a substitute.
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

- Physical writes, cleanup work, manifest temporary space, replay sessions,
  recovery scans, and in-flight requests have explicit bounds.
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
resolved. A missing or invalid checkpoint is a startup/storage error, not a new
consumer at offset zero.

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
- Changing `protect` to `expire`, deleting a consumer, or expiring a replay
  session can remove an application's replay entitlement and needs an
  explicit, auditable operation.
- The broker must reject an unknown policy version or field combination rather
  than choosing a more destructive fallback.

### Clients, engines, and deployment

Existing provisional publish, poll, grouped poll, and acknowledgement requests
retain their current meaning when no new retention/replay feature is used.
New errors and replay/configuration operations require an additive protocol
version or capability negotiation. Older clients may receive a generic error
for a new operation, but they must not see an unavailable-history result as an
ordinary empty poll.

Local-to-cluster migration remains unsupported until a separate, versioned
migration workflow fences writers, transfers retained data and consumer state,
resolves producer identities, and establishes the destination durability
boundary. Changing `--engine`, reusing a clustered data directory, or changing
cluster/node identity is not a migration procedure. Kubernetes remains a
packaging/deployment surface; it cannot substitute for process-level recovery
or storage compatibility tests.

## Observability and operator behavior

The existing metrics should remain backward-compatible while adding bounded,
documented signals. Labels must be limited to a stream or fixed reason set;
consumer identity should be opt-in or exposed through a bounded administrative
description rather than creating unbounded Prometheus cardinality.

### Gauges and status

At minimum expose:

- physical broker storage bytes and logical retained record bytes, with their
  measurement definitions;
- detected available capacity, configured capacity limit, reserved bytes,
  required headroom, and current pressure state;
- retained bytes, reclaimable bytes, retention overage, logical floor offset,
  logical floor time, and the number of consumers/replay sessions constraining
  each stream;
- oldest contiguous consumer lag in records and bytes, plus the number of
  lagging consumers in `protect` mode;
- cleanup in progress, selected cleanup budget, last successful cleanup time,
  and pending orphan bytes;
- replay sessions in progress and their bounded resource usage; and
- clustered redundancy that is below reserve, unable to clean, or not caught
  up sufficiently to satisfy the selected durability mode.

Names should follow the current `runnel_*` convention. Exact names and label
sets belong in the implementation ADR and metrics tests. The existing
`runnel_storage_bytes` definition must not silently change.

### Counters, histograms, and diagnostics

Add counters for cleanup attempts, successful segment/extent deletion, bytes
reclaimed, cleanup failures by fixed reason, pressure transitions, publish
rejections by fixed reason, ambiguous durable writes, unavailable-history
responses, policy changes, replay expiry, and recovery failures. Add bounded
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
| Size-only retention | Local, then clustered | Small segment/byte budget with records of different encoded sizes. | Oldest eligible complete segments are removed until within budget or a documented granularity/fence prevents it; no partial record disappears. |
| Both limits | Local | One limit violated before the other and then both violated. | Either trigger can select deletion; cleanup converges toward both targets and reports overage when it cannot. |
| Protected lagger | Local process | Consumer remains at an old contiguous offset while publishes continue. | Consumer pins the floor; reclaimable/pinned/overage bytes are visible; publish rejects only at reserve pressure, never by deleting its history. |
| Expiring lagger | Local process | Opt-in `expire`, consumer below the floor, then poll/replay. | History is deleted only under policy; the client gets `history_unavailable` with the boundary; checkpoint is not silently advanced. |
| Active delivery fence | Local process | Retention becomes due while a delivery lease is unexpired. | Its segment remains; old acknowledgement retains current stale-token behavior; after expiry the selected policy determines the explicit outcome. |
| Grouped out-of-order acknowledgement | Local and cluster | A later grouped offset is acknowledged while an earlier one is in flight. | Deletion uses contiguous progress only; the earlier message/key cannot be reclaimed prematurely. |
| Replay pin and expiry | Local process | Start replay, block progress, restart/expire the session while cleanup runs. | `protect` pins within its bounded lifetime; session expiry is durable and subsequent access is explicit, never an accidental reset. |
| Preflight low space | Local process | Constrain free space or use a capacity-provider test double, then publish. | Cleanup is bounded; a publish that cannot preserve reserve is rejected before append; unrelated health/poll work continues. |
| Full disk between checks | Local process | Consume headroom after preflight, fail append or sync with `ENOSPC`. | No false success; old manifest/checkpoint remains authoritative; response is confirmed rejection only when known, otherwise unknown/retryable with metrics. |
| Full disk during ack | Local process | Fail the consumer-state replacement or directory sync. | Acknowledgement is not reported; durable progress remains at the prior state; retry can succeed after capacity returns. |
| Cleanup crash before manifest | Local process | Kill the real broker after temporary manifest write and before publish. | Old manifest and all referenced history recover; temporary state is discarded or retried. |
| Cleanup crash after manifest | Local process | Kill after manifest publication but during segment deletion. | New floor remains; old unreferenced segments are reclaimable orphans; restart does not resurrect the floor. |
| Cleanup deletion failure | Local process | Make one segment undeletable or return an I/O error. | Logical policy remains deterministic; physical reclaimable bytes and failure are visible; later cleanup retries. |
| Torn/corrupt segment | Local process | Kill during append; separately corrupt a complete frame or manifest. | Torn tail follows the accepted recovery rule; complete corruption fails closed with a diagnostic; no empty-stream recovery. |
| Restart after retention | Local process | Close/restart with retained and deleted prefixes plus old consumers. | Retained records and consumer progress match the policy; deleted prefixes return explicit unavailable history. |
| Leader failure during floor command | Three processes | Kill leader before response and during local cleanup. | Only committed floor affects eligibility; a retry/new leader is idempotent; no uncommitted history is hidden. |
| Full follower with writable quorum | Three processes | Exhaust one replica's capacity while two can persist. | Quorum durability behavior is explicit; cluster exposes degraded redundancy and does not call the follower repaired. |
| Loss of writable quorum | Three processes | Exhaust or stop enough replicas to prevent durable commit. | New publishes reject/retry without becoming visible; reads/health behavior follows the selected degraded policy. |
| Cluster snapshot after retention | Three processes | Advance floor, compact, restart, and install a snapshot on a test replacement. | Snapshot contains policy/floor/consumer state; no expired record is resurrected; replacement remains within the accepted recovery boundary. |
| Interrupted snapshot transfer | Three processes | Interrupt transfer at multiple chunks. | Receiver never serves partial state; retry from the current supported boundary is safe and metrics count the interruption. |
| Policy migration | Local and cluster | Upgrade old state; apply unlimited-to-finite and protect-to-expire changes. | Old state means unlimited retention; destructive transitions require explicit confirmation and are durable/auditable. |
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
- `protect` with a fully caught-up consumer, a slow consumer, and a stopped
  consumer;
- `expire` with lagging independent and grouped consumers;
- continuous publish while cleanup catches up, including a cleanup backlog;
- replay from earliest retained, offset, and time boundaries;
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

## Recommended initial operational defaults

These are rollout hypotheses, not accepted defaults:

- Existing streams retain unlimited history and use `protect` semantics until
  an operator explicitly selects finite retention.
- A newly created stream should also default to unlimited retention in the
  first compatibility release. A production deployment should be encouraged
  to set a finite `max_bytes` or `max_age` after measuring its replay needs.
- Reserve defaults to the greater of 10% of effective capacity, 256 MiB, and
  four maximum publish batches, with validation that one maximum legal durable
  operation still fits. Operators can set an explicit reserve for known
  snapshot/cleanup amplification.
- Low pressure begins below reserve plus two maximum publish batches; critical
  pressure begins at reserve. Recovery of normal admission uses hysteresis at
  the low-water mark plus the same bounded margin.
- Cleanup runs periodically (a starting hypothesis is 30 seconds), runs
  immediately on low pressure, and deletes no more than one segment or a
  bounded byte/time budget per turn. The segment target and budget must be
  measured against the deployment's storage device.
- `protect` is the default lag policy. `expire` requires explicit per-stream
  configuration and an auditable destructive transition.
- Cleanup, health, metrics, and shutdown retain reserved execution capacity;
  no background queue is allowed to grow with publish rate or retained
  history.

The current illustrative Kubernetes values (10 GiB claims, 1 GiB memory,
five-minute startup probe, and 30-second termination grace) should remain
unchanged by this design document. Once implementation exists, the deployment
documentation must explain the relationship between those values, detected
capacity, reserve, snapshot working space, and the broker's startup recovery
budget. Kubernetes must not be required for the policy to be safe.

## Unresolved decisions

The following require explicit resolution before implementation is treated as
an accepted product behavior:

1. What exact versioned public operation configures retention and starts,
   polls, acknowledges, resets, and ends a replay session?
2. Should a replay session pin history by default, and what durable lease,
   renewal, or operator-expiry semantics bound a crashed session?
3. Is deleting an unacknowledged but no-longer-active message under `expire`
   acceptable, or must such messages always remain until a retry/dead-letter
   outcome completes?
4. Should dead-letter streams inherit the source policy, have an independent
   policy, or default to protected unlimited retention?
5. Should `max_bytes` count encoded record bytes, logical payload/key bytes, or
   both as separate configurable limits, especially when compression arrives?
6. How should a clustered leader determine quorum headroom when a follower is
   full, slow, or unable to report capacity, while preserving the selected
   durability guarantee without exposing replica topology?
7. What filesystem capacity source and failure policy apply when the data
   volume is shared, quota-limited, or does not provide reliable statfs data?
8. Should the first local implementation use rename-and-delete manifests,
   immutable extent manifests, or a transactional embedded store, and what
   snapshot representation avoids copying all retained history?
9. What is the supported downgrade boundary after a new segment, floor,
   replay, or policy version has been written?
10. What operator action explicitly releases a protected abandoned consumer,
    and how is its lost replay entitlement reported to applications?
11. Which readiness status and alerts distinguish safe degraded reads from an
    inability to accept the selected durable writes?
12. What stable numeric resource and p99/p99.9 thresholds are appropriate for
    each supported workload after the first measurements exist?

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
- [Local engine storage and delivery implementation](../../crates/runnel-core/src/lib.rs)
- [Clustered state-machine and group manager](../../crates/runnel-raft/src/lib.rs)
- [Server admission and metrics](../../crates/runnel-server/src/main.rs)
- [Illustrative Kubernetes deployment](../../deploy/kubernetes/runnel.yaml)

The existing research notes contain the primary external references for
consensus recovery, replicated-log behavior, and storage alternatives. This
plan makes no additional external product or standards claims; implementation
tradeoffs not yet measured are identified as hypotheses or unresolved
decisions above.
