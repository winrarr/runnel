# Safe durable storage upgrades

- Status: exploratory design proposal; not an accepted compatibility decision
- Last reviewed: 2026-09-03
- Baseline: 5bed1e052fcf907d2ab8ce3aa22da961b38540f1
- Scope: backlog outcome “Make durable storage upgrades safe” and TD-007
- Related policy: [Durable storage upgrade policy](storage-upgrade-policy.md)

## Purpose and non-claims

This document is the detailed, implementation-ready contract for the storage
upgrade backlog outcome. It records proposed invariants, compatibility
relations, migration phases, failure oracles, and evidence gates. The words
MUST, MUST NOT, and MAY describe requirements for a future implementation;
they do not describe behavior that exists today.

This proposal does not accept a public administration API, a final storage
schema, an online migration protocol, local-to-cluster movement, or general
downgrade support. No migration, generation selector, writer epoch, rollback
command, or rolling-upgrade test is implemented at this baseline. A later ADR
is required before any proposed contract becomes a release compatibility
promise.

The safety objective is:

> A supported upgrade preserves the same acknowledged logical state, or fails
> closed with enough identity and progress information to recover. It MUST NOT
> make a valid store appear empty, serve partial target state, or allow a
> stale writer to append or acknowledge after activation.

## Terminology and invariants

These terms keep binary replacement, format conversion, and engine migration
separate:

| Term | Meaning in this proposal |
| --- | --- |
| Source generation | The validated durable image currently selected for serving. It remains authoritative until activation commits. |
| Target generation | An immutable, side-by-side image being built from one source boundary. It is never served while it is incomplete or merely staged. |
| Active selector | A small durable record that identifies exactly one generation and its identity/checksum. A directory name or first parsable file is not a selector. |
| Migration record | Durable state for one migration ID, its source/target descriptors, phase, fence epoch, progress, validation result, and rollback/cleanup state. |
| Activation | The durable transition that changes the selected generation after target validation. It is not the same as writing target bytes. |
| Writer fence | A durable generation/epoch check on every mutating operation that prevents a stale process or migration owner from committing after the barrier. |
| Rollback | Returning to a source generation before target state is active, or using a tested inverse/recovery artifact after activation. |
| Downgrade | Starting software that cannot read the active representation. It is unsupported after target-only active state unless a reverse conversion or recovery procedure explicitly proves safety. |
| Source boundary | The exact logical record/checkpoint/Raft apply boundary copied into the target. It MUST be recorded and validated rather than inferred from a file length. |

The implementation MUST preserve these invariants:

1. **One authority:** recovery selects exactly one valid generation or refuses
   to serve; it never falls back to an empty directory.
2. **No partial serving:** target bytes and progress are private until the
   target is complete, validated, and activated.
3. **Logical equality:** conversion preserves stream/group identity, logical
   offsets, timestamps, keys, opaque payload bytes, consumer progress,
   attempts, and producer request identity.
4. **Monotonic progress:** a checkpoint, applied log boundary, or generation
   cannot move backward as a side effect of upgrade or rollback.
5. **Fence before mutation:** every publish, acknowledgement, state transition,
   and migration-owner action checks the active generation and epoch at its
   durable commit point.
6. **Evidence before cleanup:** source and diagnostic evidence remain until the
   documented rollback/recovery condition has passed. Cleanup failure is an
   observable space condition, not permission to delete the active target.

## Current observed boundary

Runnel has no global storage schema. The [local engine](../../crates/runnel-core/src/lib.rs)
and [clustered engine](../../crates/runnel-raft/src/lib.rs) own different
artifacts and use different recovery rules. The current implementation and
tests are evidence for the rows below, not a cross-release promise.

### Local engine

| Artifact | Current observed representation and recovery | Consequence for migration |
| --- | --- | --- |
| Stream history | \`streams/<stream>.log\` contains \`RNL1\` legacy frames, checksummed version-1 \`RNL2\` frames, and request-aware version-1 \`RNL3\` frames. Versioned/request-aware keys are bounded at 128 bytes and bodies at 64 MiB; request IDs are bounded at 1 KiB. The reader dispatches by magic. \`StreamLog::open\` scans complete frames and truncates only an incomplete suffix. | A successful parse does not prove offset continuity, unique request IDs, or semantic equivalence. The converter must validate those properties before activation. |
| Consumer checkpoint | \`consumers/<stream>/<consumer>.json\` stores the contiguous committed offset, out-of-order acknowledged offsets, and persisted delivery attempts. | Copy the logical state, not a highest-seen offset. Reject impossible offsets, attempts of zero, and state whose stream/consumer identity does not match its path. |
| Consumer journal | The historical \`<consumer>.json.tmp\` path is a bounded JSON-lines event journal. A partial final line is truncated during recovery; complete malformed events fail. Checkpoint compaction writes a temporary checkpoint and renames it into place. | Journal replay and checkpoint replacement are file-level crash boundaries, not a resumable directory migration. Their sync and rename behavior needs focused fault evidence. |
| Volatile delivery state | Local member ownership, delivery tokens, deadlines, indexes, and process-lifetime counters are in memory. Attempts are persisted before delivery is returned; tokens do not survive restart. | Do not copy tokens or Instant deadlines. The barrier must define which acknowledgements finish before the fence and which deliveries are redelivered after it. |

Relevant current tests include local [versioned frame recovery](../../crates/runnel-core/src/lib.rs),
[mixed legacy/versioned frame reading](../../crates/runnel-core/src/lib.rs),
[checksum refusal](../../crates/runnel-core/src/lib.rs), and
[consumer-state persistence tests](../../crates/runnel-core/src/consumer_state.rs).
They do not implement migration.

### Clustered engine

| Artifact | Current observed representation and recovery | Consequence for migration |
| --- | --- | --- |
| Root identity | \`storage.json\` is a denied-unknown-field JSON object at metadata version 1 with \`cluster_name\` and \`node_id\`. Existing mismatches, unknown versions, malformed metadata, and unmarked grouped state fail closed. | Identity is an ownership guard, not a generation selector. A staged image MUST bind cluster/node identity and cannot acquire authority by matching configuration alone. |
| Group layout/manifest | \`groups/metadata\` is the metadata group. Stream data groups are under \`groups/data/<hex-stream>/\` and use \`group.json\` for stream, stream ID, and group ID. Startup validates paths, manifests, and group files before opening groups. | The manifest has no migration phase or generation field. It MUST NOT be overloaded without a compatibility revision. |
| Raft log | \`raft-log.json\` has a separate denied-unknown-field format version 1, purge boundary, entries, committed ID, and vote. | Consensus-log representation is independent from retained stream data. A conversion MUST preserve committed and purge/applied boundaries and cannot use a state-machine version as a Raft-log version. |
| State-machine checkpoint | \`state-machine/state-machine.json\` is emitted at version 2 and reads version 1 forward into current stream identity/lifecycle state. | Read-forward is a parser behavior; it does not prove old writers can operate beside new writers or that all semantic fields are preserved. |
| Snapshot | \`snapshot.json\` wraps OpenRaft metadata and a payload that accepts version 1, including an omitted legacy version, and version 2. Installation validates the payload before replacing state. | Snapshot metadata carries a committed/applied boundary and membership. Snapshot install is a recovery primitive, not a general format converter. |
| State-machine journal | \`state-machine/state-machine.log\` is a length-prefixed JSON journal with record version 1 and a bounded record size. Recovery truncates only an incomplete final frame; complete malformed or unsupported entries fail. | Journal replay, checkpoint, snapshot, and Raft-log boundaries must agree before a target can serve. |
| Peer/snapshot transport | Peer RPCs are bounded length-prefixed JSON frames without a version handshake and have a 64 MiB frame limit. Snapshot chunks are bounded at 64 KiB, and the current in-memory receiver retries an interrupted transfer from byte zero. | Parseability does not establish mixed-version safety. Existing snapshot retry behavior MUST NOT be described as resumable migration. |

The clustered validation path is exercised by tests for [identity mismatch and
reopen](../../crates/runnel-raft/src/lib.rs), [legacy and partial layout
refusal](../../crates/runnel-raft/src/lib.rs), [unsupported versions without
mutation](../../crates/runnel-raft/src/lib.rs), and [snapshot/journal
recovery](../../crates/runnel-raft/src/lib.rs). These tests establish current
refusal and recovery boundaries only.

## Compatibility matrix

Compatibility is a relation between a specific artifact, operation, and
release pair. A single global format number is insufficient. Every future
format or binary change MUST fill this matrix with explicit versions, fields,
limits, identities, and test fixtures.

| Relation | Contract |
| --- | --- |
| read | The reader decodes every required field, bounds allocation and decompression work, validates identity/checksums, and preserves logical meaning. |
| write | A writer emits only representations that all binaries permitted to serve the active generation can safely read and interpret. |
| mixed | Old and new binaries can operate together without violating ordering, acknowledgement progress, recovery, deduplication, or writer fencing. Successful JSON parsing is not sufficient. |
| migrate | An explicit source-to-target conversion is required, with a recorded boundary, validation oracle, activation protocol, and rollback/recovery path. |

### Current artifact matrix

| Artifact | Current read | Current write | Current mixed | Current migrate / downgrade |
| --- | --- | --- | --- | --- |
| Local RNL1/RNL2/RNL3 stream frames | Recognized magic and version/length/checksum rules; mixed frame families can be read by the current reader. | Default local appends use RNL1; opt-in versioned appends use RNL2; request-aware appends use RNL3 with bounded IDs. | Same current reader can read known mixed frames. Cross-release mixed writers and semantics are not promised. | No root generation marker or converter. Older binaries must not be assumed to understand target-only frames. |
| Local consumer checkpoint and journal | Current JSON checkpoint and event forms are read; documented partial journal tails are recovered. | Current code writes the current checkpoint/event forms. | No cross-release writer contract; no versioned migration manifest. | No converter. A future converter must preserve contiguous progress, out-of-order acknowledgements, attempts, and identity. |
| Cluster storage.json | Metadata version 1 with exact cluster/node identity. | Current version 1 only. | No rolling compatibility level. | No generation selection, migration, or downgrade. |
| Cluster group.json | Current stream/stream-ID/group-ID/path agreement. | Current unversioned shape. | No mixed-generation group contract. | No converter. |
| Cluster Raft log | Format version 1 only. | Format version 1 only. | No cross-release log-writer guarantee. | No log converter. |
| State-machine checkpoint | Versions 1 and 2, with version 1 converted in memory. | Version 2. | New reader over old bytes is observed; old/new writers and command semantics are not proven. | No general migration or downgrade. |
| Snapshot payload | Versions 1 and 2, including legacy omitted version. | Version 2. | No rolling snapshot-writer guarantee. | Snapshot replacement is separate from format conversion. |
| State-machine journal | Record version 1; incomplete final frame is a recovery exception. | Record version 1. | No mixed-version journal contract. | No converter. |
| Peer frames | Current bounded JSON shape. | Current shape only. | No explicit version negotiation or rolling guarantee. | No protocol migration. |

The current supported opening behavior is therefore limited to the current
layouts and the tested read-forward checkpoint/snapshot cases. It does not
include a binary-to-binary rolling upgrade, a directory rewrite, or a
local-to-cluster move.

### Proposed compatibility classes

The following vocabulary is a proposal for future matrix entries:

| Class | Reader/writer rule | Serving and rollback rule |
| --- | --- | --- |
| Patch-preserving | Durable and peer representations are unchanged. | Binary rollback is allowed while the same representation remains active. |
| Additive read-forward | New readers accept old bytes, but writers emit old bytes until all serving writers pass the compatibility gate. | Mixed serving requires operation-level tests; a new-only field or semantic change closes the old-binary rollback path. |
| Converted layout/encoding | Old and new readers are distinct; target is built side-by-side and activated only after validation. | Source remains authoritative before activation. After activation, old software fails closed unless a tested inverse exists. |
| Semantic | Offset, acknowledgement, replay, retry, retention, identity, or ordering meaning changes, even if the serialized shape is additive. | No automatic downgrade. Require explicit compatibility proof and a verified recovery artifact. |

Unknown versions, unknown required fields, invalid identity, impossible
boundaries, and contradictory version combinations MUST fail before serving or
mutating existing state. Serde defaults and parseability do not establish
semantic compatibility.

## Proposed first migration contract

The first supported physical rewrite should be deliberately narrow:

- one local stream or one independently addressable clustered data group;
- offline conversion with a short per-stream maintenance fence;
- side-by-side immutable target units and bounded copy batches;
- a durable migration record and active selector;
- full logical-image validation before activation; and
- retained source state until explicit cleanup eligibility.

This slice does not include local-to-cluster movement, live dual writes,
dynamic placement, automatic downgrade, or a public operation that exposes
physical paths and offsets. Unrelated local streams MAY continue only if their
writer ownership cannot observe or bypass the affected stream’s fence; if that
cannot be proven, the implementation MUST fence the whole broker for the
operation.

### Durable migration record

The exact serialization is open, but the record MUST bind:

- migration ID, source and target generation IDs, and format/layout/schema
  descriptors;
- engine, cluster, node, stream, and data-group identity;
- source boundary, target progress, batch/segment checksums, record/byte
  counts, and validation result;
- writer/activation epoch and migration-owner identity;
- phase, outcome, start/last-progress timestamps, and last failure reason; and
- rollback eligibility, recovery-artifact identity, cleanup state, and orphan
  accounting.

Use these phases or an equivalent state machine:

planned → copying → validating → ready → activating → activated → complete

failed and aborted are durable terminal outcomes. Each transition MUST be
idempotent and must identify the migration and source/target generations. A
restart MUST resume only from a checkpoint whose target prefix checksum and
source boundary still match; otherwise it quarantines or discards only
unreferenced staging and retries from a known boundary.

### Validation contract

Read-only preflight MUST happen before conversion mutation and MUST:

1. identify the engine, cluster/node, stream/group, source generation, and
   every artifact expected for that layout;
2. reject missing, extra, malformed, unknown, contradictory, or identity-
   mismatched metadata before opening an empty replacement;
3. parse every source artifact through a bounded compatibility reader;
4. validate checksums, frame lengths, record limits, offset continuity,
   duplicate offsets, and source-boundary agreement;
5. compare logical records exactly: stream/group identity, offset order,
   published timestamp, key, opaque payload bytes, and request ID/fingerprint;
6. validate ordinary and grouped consumer state: contiguous committed offset,
   out-of-order acknowledgements, attempts, in-flight ownership semantics,
   and consumer identity. Volatile local tokens/deadlines are redelivery
   inputs, not portable state;
7. for clustered state, validate stream lifecycle/identity, consumer state,
   request deduplication, lease-clock floor, last-applied log, membership,
   snapshot boundary, Raft committed/purge boundary, and journal replay
   agreement;
8. check temporary-space headroom, bounded batch memory, and the largest
   legal durable write before starting; and
9. record a stable source boundary and a verified backup/recovery artifact.

The implementation MUST distinguish an incomplete crash tail from corruption
and from an unsupported version. A supported tail-recovery rule may be applied
only at the documented source boundary; complete malformed or unsupported data
MUST fail closed without truncating authoritative state.

### Local transfer and activation

The proposed local sequence is:

1. **Preflight:** validate source, compatibility, identity, backup, free
   space, and absence of an ambiguous active migration. Do not rewrite source.
2. **Fence:** acquire exclusive migration ownership, advance the durable epoch,
   and stop new publish, acknowledge, and other mutating operations for the
   affected stream. An operation already at its durable commit point either
   completes before the barrier or receives an explicit retryable/fenced
   outcome; it must not be reported as successful after the epoch changes.
3. **Resolve deliveries:** let durable consumer-state writes cross the barrier
   or reject them. In-flight local tokens and deadlines are invalidated at
   cutover and may redeliver; acknowledged progress MUST NOT move backward.
4. **Copy:** read only the validated source boundary and write immutable target
   units in bounded batches. Sync target data before syncing the progress
   record; a progress record whose target bytes are not durable is invalid.
   Do not serve target reads.
5. **Validate:** reopen or independently read the target, compare the complete
   logical image, verify identities/checksums/counts/offsets, and mark ready
   only after all checks pass.
6. **Activate:** sync target files and their parent, atomically replace the
   active selector through a filesystem primitive whose crash behavior is
   tested, sync the selector parent, and persist activation completion. Keep
   the fence until the target is reopened through its normal recovery path.
7. **Reopen and release:** verify diagnostics and serving health, then release
   the fence. Keep source and migration evidence until the recorded rollback
   condition permits cleanup.

The selector recovery rule MUST be deterministic:

- source selector plus ready/activating: source remains authoritative; target
  is staged and may be retried or abandoned after validation;
- target selector plus a valid matching target: target is authoritative;
  recovery completes activation before serving;
- selector and migration record disagree, either selected image is invalid, or
  both images claim authority: refuse to serve and report the identities; and
- no selector or an unmarked directory: do not choose by directory order,
  filename, or “first file that parses.”

The exact use of temporary files, rename, and directory synchronization needs
an implementation experiment on each supported filesystem. The invariant is
that recovery sees a valid old or valid new image, never a half-written
selector and empty fallback.

### Clustered rolling and physical migration

Rolling binary replacement and physical format conversion are separate gates:

1. Replace nodes one at a time while every node reads/writes the old
   compatible peer, command, snapshot, and state representation.
2. Keep the lowest common protocol/format capability as the cluster serving
   level. A node version is not sufficient evidence of capability.
3. Before enabling a target representation, verify every voter and the
   controlled replacement path supports it. Record a cluster-wide compatibility
   level as a committed metadata fact.
4. For a physical rewrite, build each group target from a committed source
   boundary. Every replica independently validates target identity and content
   and reports readiness keyed by migration ID and group ID.
5. Commit an activation epoch only after the required replicas are ready. A
   replica with no validated target MUST not serve the group or become leader
   for the target representation; it must finish activation or rejoin through
   the controlled replacement workflow.
6. After activation, every command and snapshot written during the declared
   compatibility window MUST remain applicable by every serving binary.
7. Retain source generations and recovery artifacts until cluster-wide
   rollback eligibility has passed and restart/failover evidence is complete.

OpenRaft snapshot transfer remains a separate recovery workflow. Its current
bounded chunks and retry-from-zero behavior do not provide resumable migration.
An interrupted target transfer MUST leave the receiver non-serving and either
resume from a verified migration checkpoint or restart from a verified source
boundary.

## Interruption and rollback contract

| Interruption or fault | Required result after restart |
| --- | --- |
| Preflight, missing space, or before copy | Source remains active and unchanged. Migration is failed or aborted with a diagnostic; no empty target is created as authority. |
| During copy/checkpoint | Source remains active. Resume only from a checksummed bounded checkpoint, or discard unreferenced target bytes and restart from the recorded source boundary. |
| During validation | Source remains active. Target is not served; a mismatch is durable failed evidence, not a reason to guess or repair source bytes. |
| After ready, before activation | Source remains active. The validated target can be retried after identity/checksum checks or explicitly aborted. |
| During selector replacement/sync | Recovery validates the selector and both generation descriptors. It serves a valid old or new generation deterministically; disagreement or invalidity fails closed. |
| After target activation, before fence release | Target remains authoritative. Recovery completes target activation, redelivers volatile in-flight work as required, and does not accept stale source writes. |
| During cleanup | Target remains authoritative. Source/orphan bytes remain for a bounded cleanup pass and are visible in diagnostics; cleanup never removes the selected generation. |
| Process restart at any phase | The durable migration record and selector are the only authority. Directory order, timestamps, and parseability alone are not recovery decisions. |
| Corrupt source/target or mismatched identity | Refuse to serve the affected scope, preserve bytes, and report the observed and expected identities. Do not silently rebuild empty state. |

Rollback and downgrade are intentionally different:

| State | Supported action in the first proposal |
| --- | --- |
| Binary rollout, old representation still active | Stop the new process and restore the previous binary after normal identity/preflight checks. |
| planned through ready, source selected | Abort migration or discard unreferenced staging after stopping its owner. This is migration abort, not downgrade. |
| activating with an unambiguous source selector | Keep source selected and resolve/abort the migration; do not start an old binary against an ambiguous directory. |
| Target activated | Keep target selected. An old binary fails closed unless it has a tested target reader. Use a target-aware binary, reverse converter, or verified recovery artifact. |
| Source retained after target writes | Retention alone does not make pointer rollback safe. All acknowledged writes, progress, attempts, request IDs, retention effects, and semantic changes must be representable by the rollback target. |

Automatic downgrade is unsupported. Editing version fields, removing selectors,
renaming directories, or starting an older binary against target-only active
state is forbidden because it can hide acknowledged writes or move progress
backward. A verified backup must include enough metadata and logical state to
restore identity, offsets, consumer state, attempts, and producer retry
identity, not just message bytes.

## Writer fencing and ownership

The first local implementation MUST use both an exclusive migration owner and a
durable writer epoch; an in-process mutex alone is insufficient. Every writer
and acknowledgement path must:

- read the expected active generation/epoch before waiting on storage;
- re-check it at the durable commit point;
- reject a stale owner with an explicit retryable/fenced outcome; and
- record the rejection without appending or advancing consumer state.

The migration owner must hold the fence through target validation, selector
activation, target reopen, and the point at which the new generation is ready
to serve. An old process that retains an open file descriptor must not gain
authority from that descriptor. Cleanup ownership must also be epoch-bound so
it cannot delete a generation selected by a later migration.

For the clustered path, the activation epoch and compatibility level must be
committed facts in the metadata/data-group protocol. A stale leader, delayed
forwarded request, duplicate migration owner, or unready replica must receive
a fencing/retryable result before it can append or acknowledge. Replica
replacement is separate: matching node identity alone does not make an empty
or copied directory eligible to vote or serve.

## Observability and operator contract

The exact command and metric names remain open, but a usable implementation
must expose these facts without requiring file inspection:

- selected engine, generation, layout/schema/record/peer versions, supported
  reader/writer ranges, and bounded identity summary;
- migration ID, source/target generations, phase/outcome, source boundary,
  start/last-progress times, records/bytes copied, validated, and remaining;
- validation result, last failure reason, backup/recovery-artifact identity,
  rollback eligibility and its expiry/retention condition;
- writer-fence owner/epoch, stale-owner rejections, activation attempts/result,
  serving/recovering/blocked state, and cleanup/orphan bytes; and
- for clusters, per-group target readiness, lagging replicas, snapshot source
  boundary, compatibility level, and leader/serving eligibility.

Candidate metrics may use bounded labels such as engine, phase, outcome, and
reason. Stream, group, consumer, and migration identifiers belong in
structured logs or an explicitly bounded diagnostic response, not unbounded
Prometheus labels. Counters must state whether they reset on process restart.
Progress gauges must not be mistaken for a durability acknowledgement.

Readiness MUST be false, or the affected scope must refuse service, when
required metadata is corrupt, a migration is ambiguous, a target is not
validated, a replica lacks the active representation, or recovery has not
established a unique authoritative generation. “Started successfully” is not
a recovery result.

## Acceptance matrix

The following matrix is the merge gate for a future implementation. “Current”
means existing evidence at the baseline; “future” means required work and is
not implemented by this document.

| ID | Scenario | Setup/fault | Required oracle | Evidence status |
| --- | --- | --- | --- | --- |
| COMP-01 | Artifact inventory | Fixture each local, clustered, snapshot, journal, and peer artifact with version/identity descriptors. | Matrix records read, write, mixed, and migrate behavior, limits, and downgrade boundary for every artifact. | Future; current inventory is documented above. |
| VAL-01 | Read-only preflight | Unknown version, malformed JSON, unknown required field, identity mismatch, missing/partial layout, and contradictory selector/manifest. | Fails before serving or mutating authoritative bytes; never opens an empty replacement; diagnostics identify artifact and expected/observed identity. | Cluster refusal tests exist; migration fixtures future. |
| VAL-02 | Logical state image | Old fixture with records, mixed frame families, keys, opaque bytes, timestamps, offsets, ordinary/grouped consumer progress, attempts, and request IDs. | Exact state comparison passes; no renumbering, dropped bytes, progress rollback, duplicate request identity, or unacknowledged-state loss. | Future. |
| VAL-03 | Bounds and corruption | Oversized lengths, checksums, gaps, duplicate offsets, impossible checkpoints, malformed complete tail, and supported incomplete tail. | Bounded allocation; only the documented incomplete tail is recoverable; complete corruption/unsupported data remains intact and fails closed. | Current parser tests cover subsets; conversion gate future. |
| XFER-01 | Bounded copy/resume | Interrupt after each bounded copy checkpoint and restart with matching and mismatching target prefixes. | Resume is idempotent from verified progress or discards only unreferenced target; source remains usable and memory/temporary work is bounded. | Future. |
| XFER-02 | Interrupted transfer | Kill at preflight, copy, validation, ready, selector replacement, target reopen, and cleanup; include multiple snapshot chunks for clustered path. | Receiver/target never serves partial state; exactly one valid generation is authoritative after restart; migration phase/outcome explains the result. | Existing snapshot retry-from-zero is separate; migration gate future. |
| ACT-01 | Activation crash | Fault file sync, rename, selector sync, and migration-record completion at each activation boundary. | Recovery chooses a valid old or new image from durable identity/protocol state, never directory order or empty fallback. | Future; filesystem-specific evidence required. |
| FENCE-01 | Local stale writer | Keep old process/owner delayed across fence, copy, activation, and cleanup; issue publish and ack. | Stale operations get explicit retryable/fenced outcomes before durable mutation; acknowledged progress and source/target identity remain correct. | Future. |
| FENCE-02 | Cluster stale owner | Delay old leader, forwarded request, duplicate migration owner, and unready replica across committed activation. | Only the committed active epoch can append/ack or lead; unready replica cannot serve; no split-brain generation. | Existing leader/follower tests are not migration evidence; future. |
| ROLL-01 | Pre-activation rollback | Abort or restart a migration in planned through ready; retain staged target. | Source remains readable and authoritative; abort is idempotent; target can be safely discarded without source mutation. | Future. |
| ROLL-02 | Post-activation downgrade | Start an old binary against target-only active state; test version edit, selector removal, and path rename attempts. | Old binary fails closed; no acknowledged target state is hidden; documented reverse conversion or recovery-artifact path is required. | Future. |
| OBS-01 | Diagnostics and metrics | Exercise every phase, failure reason, fence rejection, cleanup orphan, restart, and process-counter reset. | Versions, identities, progress, outcome, rollback eligibility, readiness, and orphan state are visible with bounded labels and no secret/path leakage. | Future; current startup and snapshot metrics are partial evidence. |
| E2E-01 | Local real process | Publish/consume/ack representative records, migrate, kill/restart at each phase, then use the public protocol. | Same logical records and acknowledged state are observable before/after; redeliveries are allowed only where the contract says; health/readiness recover. | Future; just smoke does not exercise migration. |
| E2E-02 | Cluster rolling binary | Three real broker processes, old-compatible representation, one-node-at-a-time restart, then compatibility gate. | Mixed phase serves with old format; gate is durable; unsupported node/binary refuses rather than serving incompatible state. | Future; just cluster-test is current recovery evidence only. |
| E2E-03 | Cluster physical activation | Three nodes with leader loss, follower restart, snapshot install, target readiness, and replacement identity checks. | Activation epoch survives failover; every serving replica validates target; acknowledged records/progress and request identities remain intact. | Future. |
| RES-01 | Migration headroom | Large retained stream, bounded batch sizes, temporary-space reserve, and no-reserve condition. | Admission pauses/throttles/rejects explicitly; memory, temporary bytes, and recovery work remain bounded; no false durable success. | Future targeted resource test. |

The implementation must not be called complete until all future rows have
tests or operational evidence at the appropriate layer. A later ADR may
reduce or extend the matrix only by recording the evidence and consequence.

## Implementation sequence and exit gates

1. **Compatibility descriptors and fixtures:** define Runnel-owned artifact
   descriptors, version ranges, identities, limits, and state-image equality.
   Exit when unsupported and contradictory layouts fail before mutation.
2. **Manifest and preflight:** add migration record parsing, source-boundary
   capture, space checks, and deterministic selector recovery. Exit when
   ambiguous state cannot serve.
3. **Local side-by-side conversion:** implement one representative format or
   layout conversion with bounded checkpoints and exact logical comparison.
   Exit when interruption/resume and pre-activation abort are idempotent.
4. **Fence and activation hardening:** add stale publish/ack/owner tests,
   filesystem sync/rename fault injection, target reopen, and diagnostics.
   Exit when no stale operation can commit across cutover.
5. **Cluster compatibility gate:** add negotiated/committed capability,
   per-replica readiness, old-binary refusal, leader/follower/replacement
   tests, and explicit separation from snapshot recovery.
6. **Operational and resource acceptance:** expose bounded diagnostics/metrics,
   run large-stream headroom tests, and document backup/cleanup workflow.
7. **Decision review:** only after all required rows pass should an ADR accept
   a named format, command, rollback window, or release guarantee.

## References and design evidence

The sources below are direct primary or project-maintained references. Their
behavior is evidence for design constraints, not a compatibility target for
Runnel.

| Source | Relevant fact | Difference and Runnel implication |
| --- | --- | --- |
| [Apache Kafka rolling upgrades](https://kafka.apache.org/32/getting-started/upgrade/) and [protocol design](https://kafka.apache.org/38/design/protocol/) | Kafka upgrades binaries while holding the old inter-broker/message representation, verifies behavior, then advances an explicit protocol version gate; clients negotiate API versions. | Runnel needs a binary-versus-format gate, but its state includes consumer progress and producer request identity. The current static cluster has no negotiated compatibility level, so this remains proposed work. |
| [PostgreSQL pg_upgrade](https://www.postgresql.org/docs/17/pgupgrade.html) | Preflight runs before mutation. Copy/clone modes keep a separate old cluster; link mode saves space but moves or removes the old-cluster rollback property earlier. | Side-by-side conversion is the safer first Runnel model. In-place or shared-file conversion would need a separate decision and filesystem evidence. |
| [etcd 3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) and [downgrade procedure](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) | etcd documents mixed-version operation, snapshots, cluster-wide downgrade state, schema-aware handling, and status reporting; replacing one binary is not a downgrade procedure. | Runnel should require a verified recovery artifact and target validation, but must not imply etcd-like online migration until protocol and failure evidence exist. |
| [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/) and [storage traits](https://docs.rs/openraft/latest/openraft/storage/) | Snapshot metadata and storage interfaces carry committed/applied boundaries, membership, log persistence, state-machine persistence, and installation as separate concerns. | Runnel must validate application-state schema, group identity, consumer state, attempts, and deduplication in addition to consensus boundaries. Snapshot replacement is not format migration. |
| [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST) | A transactional version-edit log and CURRENT pointer select complete referenced file sets, while obsolete files may remain until safe cleanup. | This motivates a small active selector and retained source generations, but Runnel needs explicit delivery semantics, identity, bounded validation, and writer fencing. |
| [Online asynchronous schema change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | Online readers/writers require compatibility between transition states; asynchronous schema assumptions can corrupt data even when parsing succeeds. | A future live-tail migration requires operation-level proofs for publish, ack, replay, and recovery. The first slice avoids that risk with a maintenance fence. |
| [Linux rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html) and [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html) | Rename and file synchronization have distinct durability and filesystem semantics; syncing a file does not automatically establish directory-entry durability. | The selector protocol and supported-filesystem crash evidence must be specified before claiming atomic recovery. |
| [Runnel Raft recovery research](../research/raft-recovery-and-replacement.md), [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md), and [ADR 0019](../decisions/0019-clustered-storage-identity.md) | Current accepted decisions separate snapshot-based replica recovery from retained state and require clustered storage identity checks. | This proposal extends neither decision into migration or downgrade; it uses them as boundaries and keeps replica replacement separate. |

### Alternatives considered

- **Rewrite in place:** lower temporary space, but an interruption can destroy
  the only source and makes rollback indistinguishable from recovery. Reject
  for the first implementation.
- **Live dual-write with a tail:** reduces maintenance time, but two physical
  stores cannot be made atomic by writing both in sequence. Keep as a
  hypothesis until duplicate publish/ack, ordering, and stale-owner faults are
  proven.
- **Use an OpenRaft snapshot as the universal migration format:** useful for
  committed replica replacement, but it does not represent the local consumer
  journal or local/clustered producer identity semantics. Keep workflows
  separate.
- **Use a single global format number:** easy to inspect, but it cannot express
  peer, log, state, record, option, and semantic compatibility independently.
  Reject in favor of the per-artifact matrix.
- **Automatic downgrade by pointer rollback:** operationally convenient, but
  unsafe after target-only state is active. Require a tested inverse or
  verified recovery artifact.

### Hypotheses to test

- A short per-stream fence is sufficient for the first conversion and keeps
  the correctness proof smaller than live copy; otherwise the implementation
  must widen the fence rather than run an unproven mixed path.
- A generation selector plus durable migration record makes process-kill
  recovery deterministic when file and directory sync boundaries are tested
  on every supported filesystem.
- Bounded, checksummed batches keep migration memory and recovery work bounded,
  but side-by-side space amplification may require explicit admission policy.
- A committed cluster compatibility level can protect rolling upgrade without
  exposing Raft placement in the public engine contract.

### Unresolved risks and evidence required

- **Filesystem durability:** process-kill tests do not model power loss.
  Establish the supported filesystem/deployment matrix and backup requirement;
  do not infer crash safety from JSON parsing or rename success.
- **Space amplification:** side-by-side conversion can require nearly two
  retained copies plus journals, checkpoints, and headroom. Measure and define
  pause, throttle, or explicit rejection when reserve is unavailable.
- **Source boundary:** local offsets are encoded in frames while clustered
  state uses materialized vector positions and Raft boundaries. Reject gaps,
  duplicates, truncation ambiguity, and mismatched applied state.
- **In-flight delivery:** local tokens/deadlines are volatile while clustered
  grouped state is replicated. Test barrier races so stale acknowledgements
  cannot commit and permissible redelivery is explicit.
- **Cluster divergence:** metadata activation and per-replica installation may
  diverge on a crash. Model recovery and prove an unready replica cannot serve
  or lead with an incompatible target.
- **Peer compatibility:** current peer frames have no version handshake.
  Rolling tests may require protocol negotiation before physical migration can
  be enabled.
- **Backup freshness:** a backup is useful only if it includes all logical
  state and its identity can be verified. Test restore, not merely backup
  creation.
- **Cleanup/retention:** current Runnel has no general retention policy, so a
  source-generation rollback window cannot yet be automatic. Keep cleanup
  explicit or blocked until source-retention semantics are accepted.
- **Scope boundary:** local-to-cluster migration changes topology and producer
  identity; it remains unsupported until its separate cutover protocol and
  failure matrix pass.

## Evidence classification and benchmark applicability

Primary evidence class: design/research. Secondary tags: storage/recovery,
compatibility/migration, operability, and resource safety.

No runtime benchmark is required for this documentation-only change. It changes
no code, serialization, lock scope, I/O path, scheduling, resource limit, or
benchmark workload. The current publish/cluster benchmarks do not exercise
migration and cannot establish upgrade safety. The future RES-01 gate must
use a targeted migration/resource workload, with source/target sizes, batch
limits, temporary-space budget, recovery work, and failure state recorded.

## Repository evidence and handoff boundary

Current startup refusal, legacy read-forward, journal, snapshot, identity, and
real-process recovery evidence remains in the linked source/tests. This design
change does not alter those behaviors, add a compatibility promise, or add an
ADR. The backlog should describe this as a completed design milestone with
migration implementation and end-to-end gates still open.
