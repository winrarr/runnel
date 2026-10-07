# Safe durable storage upgrades

- Status: accepted behavioral contract; implementation deferred by [ADR 0037](../decisions/0037-offline-side-by-side-storage-upgrades.md)
- Last reviewed: 2026-10-07
- Baseline: `9f64146169bb525221f99b231b0c3deee784cc59`
- Reading guide: [design-note conventions](README.md)
- Scope: backlog outcome “Make durable storage upgrades safe” and TD-007
- Related policy: [Durable storage upgrade policy](storage-upgrade-policy.md)
- Current evidence: [TD-007 storage compatibility evidence](td-007-storage-compatibility-evidence.md)
- Related current boundaries: [current architecture](../architecture.md),
  [protocol compatibility](protocol-compatibility.md), [message encoding and
  compression research](../research/message-encoding-and-compression.md),
  [retention and disk pressure](retention-disk-pressure-plan.md), and
  [Raft recovery research](../research/raft-recovery-and-replacement.md)

## Purpose and non-claims

This document is the detailed implementation contract accepted by [ADR 0037](../decisions/0037-offline-side-by-side-storage-upgrades.md).
Its MUST, MUST NOT, and MAY statements govern a future supported conversion;
they do not describe runtime behavior that exists today. The records,
selectors, phase names, and procedures are illustrative mechanisms rather
than a required API, module layout, or file layout.

ADR 0037 does not accept a public administration API, a final storage schema,
an online migration protocol, local-to-cluster movement, or general downgrade
support. No migration, generation selector, writer epoch, rollback command,
or rolling-upgrade test is implemented at this baseline. Implementation must
pass the applicable acceptance gates before any release compatibility promise
is made. A later ADR is needed to change the accepted behavioral boundary,
not to authorize implementation of this contract.

The safety objective is:

> A supported upgrade preserves the same acknowledged logical state, or fails
> closed with enough identity and progress information to recover. It MUST NOT
> make a valid store appear empty, serve partial target state, or allow a
> stale writer to append or acknowledge after activation.

## Terminology and invariants

These terms keep binary replacement, format conversion, and engine migration
separate:

| Term | Meaning in this accepted contract |
| --- | --- |
| Source generation | The validated durable image currently selected for serving. It remains authoritative until activation commits. |
| Target generation | An immutable, side-by-side image being built from one source boundary. It is never served while it is incomplete or merely staged. |
| Active selector | A small durable record that identifies exactly one generation and its identity/checksum. A directory name or first parsable file is not a selector. |
| Migration record | Durable state for one migration ID, its source/target descriptors, phase, fence epoch, progress, validation result, and rollback/cleanup state. |
| Activation | The durable transition that changes the selected generation after target validation. It is not the same as writing target bytes. |
| Writer fence | A durable generation/epoch check on every mutating operation that prevents a stale process or migration owner from committing after the barrier. |
| Rollback | Returning to the source only before any target-only durable mutation is accepted; after that boundary, recovery uses a target-aware binary, tested inverse, or verified recovery artifact. |
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
tests are evidence for the rows below, not a cross-release promise. Existing
clustered storage is parsed and structurally validated before groups open; the
current preflight does not prove semantic equality across every checkpoint,
snapshot, journal, and Raft-log boundary. An empty directory is different
because startup intentionally initializes its identity metadata.

### Local engine

| Artifact | Current observed representation and recovery | Consequence for migration |
| --- | --- | --- |
| Stream history | streams/<stream>.log contains checksummed RNL3 version-2 frames for ordinary records, public request IDs, and dead-letter moves. Keys, payloads, and identities are bounded; recovery validates checksums and contiguous offsets, then repairs an incomplete suffix only after all streams pass validation. RNL1, RNL2, and RNL3 version 1 are explicitly refused without mutation. | A current-format parse establishes neither release-pair compatibility nor a conversion contract. A local-to-cluster migration may use current RNL3 data as its local source; old local frame families are ineligible. Preserve logical fields and current request-ID comparison semantics. |
| Consumer checkpoint | \`consumers/<stream>/<consumer>.json\` stores the contiguous committed offset, out-of-order acknowledged offsets, and persisted delivery attempts. The JSON shape has no explicit format version, and the current loader does not independently validate serialized stream/consumer identity against its path. | Copy the logical state, not a highest-seen offset. Reject impossible offsets, attempts of zero, and state whose stream/consumer identity does not match its path. Preserve current out-of-order progress and define how a target handles invalid or unknown fields. |
| Consumer journal | The historical \`<consumer>.json.tmp\` path is a bounded 64 KiB JSON-lines event journal. A partial final line is truncated during recovery; complete malformed events fail. Checkpoint compaction writes a separate \`.checkpoint.tmp\` file, syncs it, and renames it into place. | Journal replay and checkpoint replacement are file-level crash boundaries, not a resumable directory migration. Their cross-file ordering, parent-directory durability, and sync/rename behavior need focused fault evidence. |
| Volatile delivery state | Local member ownership, delivery tokens, \`Instant\` deadlines, active-delivery indexes, and process-lifetime counters are in memory. The tail record cache and sparse index are bounded, the consumer-state cache is capped at 1,024 entries, and attempts are persisted before delivery is returned; request-ID and active-delivery maps can still grow with retained identities/work. Tokens do not survive restart. | Do not copy tokens or \`Instant\` deadlines. The barrier must define which acknowledgements finish before the fence and which deliveries are redelivered after it. Resource accounting must distinguish bounded lookup structures from durable identity/state that can grow. |
| Replay | The current replay operation reads one inclusive logical offset without creating delivery state or changing ordinary consumer progress. Current retention keeps history from offset zero, and an unavailable offset is an explicit \`history_unavailable\` outcome. | Preserve the logical \`[earliest, next)\` range and replay semantics during conversion. Replay is not a bulk-export format and future retention floors, sessions, and pins need their own compatibility rules. |

Relevant current tests include local [versioned frame recovery](../../crates/runnel-core/src/lib.rs),
[mixed legacy/versioned frame reading](../../crates/runnel-core/src/lib.rs),
[checksum refusal](../../crates/runnel-core/src/lib.rs), [replay behavior](../../crates/runnel-core/src/lib.rs),
and [consumer-state persistence tests](../../crates/runnel-core/src/consumer_state.rs).
They establish current recovery behavior only; they do not implement migration.

### Clustered engine

| Artifact | Current observed representation and recovery | Consequence for migration |
| --- | --- | --- |
| Root identity | \`storage.json\` is a denied-unknown-field JSON object at metadata version 1 with \`cluster_name\` and \`node_id\`. Existing mismatches, unknown versions, malformed metadata, and unmarked grouped state fail closed. | Identity is an ownership guard, not a generation selector. A staged image MUST bind cluster/node identity and cannot acquire authority by matching configuration alone. |
| Group layout/manifest | \`groups/metadata\` is the metadata group. Stream data groups are under \`groups/data/<hex-stream>/\` and use \`group.json\` for stream, stream ID, and group ID. Startup validates paths, manifests, and group files before opening groups. | The manifest has no migration phase or generation field. It MUST NOT be overloaded without a compatibility revision. |
| Raft log | \`raft-log.json\` selects a version-3 checksummed segment family with a bounded control record; versions 1 and 2 fail without mutation or empty-store fallback. Command payloads use compact text/base64 JSON, and the 96 MiB peer-frame cap admits the encoded form of a maximum public payload. | Consensus-log representation is independent from retained stream data. A conversion MUST preserve committed and purge/applied boundaries and cannot use a state-machine version as a Raft-log version. |
| State-machine checkpoint | \`state-machine/state-machine.json\` is emitted and accepted at version 3; older and unknown versions fail closed without mutation. The persisted image carries ordinary and grouped consumer progress, attempts, in-flight member/token/deadline state, lease-clock floor, request-ID deduplication, redelivery/dead-letter counters, last-applied log, and membership. | A future migration must distinguish portable durable state from process-local state and validate stream/group identity rather than trusting JSON shape. |
| Snapshot | \`snapshot.json\` wraps OpenRaft metadata and a version-3 payload; older and unknown payload versions fail closed. The payload carries the materialized stream, consumer, grouped-delivery, lease-clock, deduplication, and counters state; installation validates payload syntax/version before replacing state. | Snapshot metadata carries a committed/applied boundary and membership. Snapshot install is a recovery primitive, not a general format converter, and current validation does not by itself prove payload identity or agreement with the checkpoint/journal/Raft log. |
| State-machine journal | \`state-machine/state-machine.log\` is a length-prefixed JSON journal with record version 3 and a 96 MiB record bound. Recovery reads the journal into memory, truncates only an incomplete final frame, and fails on complete malformed or unsupported entries, including older versions. Publish command payloads use compact text/base64 JSON and older journal versions fail before command decoding. | Journal replay, checkpoint, snapshot, and Raft-log boundaries must agree before a target can serve; current preflight validates these artifacts independently rather than proving that agreement. |
| Peer/snapshot transport | Peer RPCs use persistent or pooled TCP connections and bounded big-endian length-prefixed JSON frames without a version handshake; the outer body limit is 96 MiB. This admits the compact text/base64 command form of a 64 MiB public payload. Snapshot chunks are bounded at 64 KiB, and the current receiver buffers a complete transfer in memory and retries an interruption from byte zero. | Parseability does not establish mixed-version safety. Existing snapshot retry behavior MUST NOT be described as resumable migration, and the complete in-memory receiver is a recovery/resource boundary that future transfer work must measure. |

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
| Local RNL3 v2 stream frames | The current reader accepts only checksummed RNL3 version-2 frames and checks field bounds and logical offset continuity. Recovery inspects every stream before repairing incomplete suffixes. RNL1, RNL2, and RNL3 version 1 fail explicitly without mutation. | Every local append uses RNL3 v2; ordinary records carry no identity, while public and dead-letter move identities are typed. The request-ID map remains unbounded by retained identity count. | No cross-release mixed-writer or release-pair contract is established. | No old-format conversion path or local format selector exists; old frame families are not eligible current local sources. |
| Local consumer checkpoint and journal | Current JSON checkpoint and event forms are read; the 64 KiB journal bound and documented partial-tail recovery are enforced. The checkpoint/event forms have no explicit compatibility version, and path identity is not independently checked by the current loader. | Current code writes the current checkpoint/event forms. | No cross-release writer contract; no versioned migration manifest. | No converter. A future converter must preserve contiguous progress, out-of-order acknowledgements, attempts, and identity. |
| Cluster storage.json | Metadata version 1 with exact cluster/node identity. | Current version 1 only. | No rolling compatibility level. | No generation selection, migration, or downgrade. |
| Cluster group.json | Current stream/stream-ID/group-ID/path agreement. | Current unversioned shape. | No mixed-generation group contract. | No converter. |
| Cluster Raft log | Format version 3 only; versions 1 and 2 are refused unchanged. | Format version 3 only. | No cross-release log-writer guarantee. | No log converter. |
| State-machine checkpoint | Version 3 only; all current persisted fields and stream shapes are required. | Version 3. | No mixed-version writer contract. | No migration or downgrade. |
| Snapshot payload | Version 3 only; all current persisted fields and stream shapes are required. | Version 3. | No rolling snapshot-writer guarantee. | Snapshot replacement is separate from format conversion. |
| State-machine journal | Record version 3 only; incomplete final frame is a recovery exception, and each record is bounded at 96 MiB. | Record version 3. | No mixed-version journal contract. | No converter. |
| Peer frames | Current persistent/pooled transport with bounded big-endian length-prefixed JSON frames. | Current shape only. | No explicit version negotiation or rolling guarantee. | No protocol migration. |

The current supported opening behavior is therefore limited to the current
layouts and tested current-format checkpoint, snapshot, and journal recovery. It does not
include a binary-to-binary rolling upgrade, a directory rewrite, or a
local-to-cluster move.

### Accepted compatibility classes

Use this vocabulary for future per-artifact matrix entries:

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

## Accepted first migration contract

The first supported physical rewrite is deliberately narrow:

- one local stream and its durable consumer state when tested isolation proves
  that scope is complete; otherwise the entire local broker;
- a whole-cluster maintenance window for any clustered physical conversion;
- offline conversion with writes fenced for the selected scope;
- side-by-side immutable target units and bounded copy batches;
- a durable migration record and active selector;
- full logical-image validation before activation; and
- retained source state until explicit cleanup eligibility.

This slice does not include local-to-cluster movement, live dual writes,
rolling mixed-binary upgrades, dynamic placement, automatic downgrade, or a
public operation that exposes physical paths and offsets. Unrelated local
streams MAY continue only if tests prove their writer ownership cannot observe
or bypass the affected stream's fence; otherwise the implementation MUST stop
the whole broker. A clustered conversion MUST stop every node and must not
permit service until all nodes select the same validated target.

### Durable migration record

The exact serialization is open, but the migration record MUST bind:

- migration ID, source and target generation IDs, and format/layout/schema
  descriptors;
- engine identity plus the applicable cluster, node, stream, and data-group
  identities. Local migrations must represent cluster/node as absent rather
  than inventing clustered identity;
- source boundary, target progress, batch/segment checksums, record/byte
  counts, and validation result;
- writer/activation epoch and migration-owner identity;
- phase, outcome, start/last-progress timestamps, and last failure reason; and
- rollback eligibility, recovery-artifact identity, cleanup state, and orphan
  accounting.

Use these phases or an equivalent state machine:

planned → copying → validating → ready → activating → active-reversible →
write-pending → active-committed → complete

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
3. parse every eligible source artifact through its current bounded reader; refuse unsupported local frame versions without mutation;
4. validate checksums, frame lengths, record limits, offset continuity,
   duplicate offsets, and source-boundary agreement;
5. compare logical records exactly: stream/group identity, offset order,
   published timestamp, key, opaque payload bytes, and request ID where
   present. A converter may compute a target fingerprint, but the current
   local RNL3 representation does not store a key/payload fingerprint; runtime
   comparison reads the original record and rejects representable mismatches;
6. validate ordinary and grouped consumer state: contiguous committed offset,
   out-of-order acknowledgements, attempts, consumer identity, and, where the
   source persists it, in-flight ownership semantics. Volatile local
   tokens/deadlines are redelivery inputs, not portable state; the current
   local replay range and unavailable-history outcome must also remain
   equivalent;
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
MUST fail closed without truncating authoritative state. Local migration sources
are current RNL3 v2 files only. RNL1, RNL2, and RNL3 v1 files are rejected
before recovery mutation; no legacy audit or conversion route is provided.

### Local transfer and activation

The accepted local sequence is:

1. **Preflight:** validate source, compatibility, identity, backup, free
   space, and absence of an ambiguous active migration. Do not rewrite source.
2. **Fence:** acquire exclusive migration ownership, advance the durable epoch,
   and stop service for the affected scope. Per-stream downtime is allowed only
   after tests prove every mutating path observes that fence; otherwise stop
   the whole broker. An operation already at its durable commit point either
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
   the migration fence but keep target mutation disabled while source rollback
   remains eligible. Resume service only with a binary that declares and proves
   read, write, and semantic support for the target; an incompatible binary
   fails closed. Before the first target-only mutation can commit, durably
   enter `write-pending` or record it atomically with the mutation. A successful
   commit becomes `active-committed`; a proven no-effect failure returns to
   `active-reversible`; an ambiguous result remains pending and blocks rollback.
   Keep source and migration evidence.

The selector recovery rule MUST be deterministic:

- source selector plus ready/activating: source remains authoritative; target
  is staged and may be retried or abandoned after validation;
- target selector plus a valid matching target: target is authoritative;
  recovery completes activation and serves only after normal target reopen;
- selector and migration record disagree, either selected image is invalid, or
  both images claim authority: refuse to serve and report the identities; and
- no selector or an unmarked directory: do not choose by directory order,
  filename, or “first file that parses.”

The exact use of temporary files, rename, and directory synchronization needs
an implementation experiment on each supported filesystem. The invariant is
that recovery sees a valid old or valid new image, never a half-written
selector and empty fallback. An ambiguous selector or disagreement with the
migration record blocks service until an operator completes activation or
proves rollback is still eligible.

### Clustered physical migration

The accepted first clustered conversion requires a whole-cluster maintenance
window; rolling binary replacement and mixed-version serving are outside ADR
0037. The clustered path remains unsupported until its all-node activation and
failure-recovery gates pass:

1. Stop every broker and verify that no node can accept client or peer
   mutations. Keep every source generation selected and unchanged.
2. Validate common cluster identity, membership, committed source boundaries,
   and a restorable recovery artifact. Build side-by-side targets for all
   required nodes and groups under one migration identity.
3. Independently validate every target and cross-artifact agreement. Keep all
   nodes stopped while any target is missing, invalid, or selected differently.
4. Activate only after every target is ready. If selector updates are
   interrupted, keep all nodes offline until each selects the same validated
   target, or each is explicitly restored to the source while rollback remains
   eligible.
5. Start only binaries that declare read/write support for the selected
   generation. A stale binary or a node with a mismatched generation or
   identity fails closed and cannot lead or serve.
6. The first target-only durable mutation enters `write-pending` cluster-wide.
   A successful commit closes source rollback for every node. A proven no-effect
   failure may restore eligibility; an ambiguous result keeps the cluster
   stopped and blocks rollback until reconciled.
7. Retain source generations and recovery artifacts until the documented
   rollback/recovery condition permits cleanup.

OpenRaft snapshot transfer remains a separate recovery workflow. Its current
bounded chunks and retry-from-zero behavior do not provide resumable migration.
An interrupted target transfer MUST leave the receiver non-serving and either
resume from a verified migration checkpoint or restart from a verified source
boundary.

The public JSON-lines v1 declaration is source-level alignment between the
protocol, client, and server crates; it is not a runtime handshake or a
cluster-wide capability gate. The peer transport has no preface or capability
negotiation at all. This is why ADR 0037 rejects rolling mixed-version
conversion. Any later decision to permit that behavior must establish an
explicit committed compatibility gate. See [protocol
compatibility](protocol-compatibility.md) for the separate proposed public-v2
boundary.

## Interruption and rollback contract

| Interruption or fault | Required result after restart |
| --- | --- |
| Preflight, missing space, or before copy | Source remains selected and unchanged. Record a refusal/failure; never create an empty target as authority. |
| During copy/checkpoint | Source remains selected. Resume only from checksummed bounded progress whose source boundary and target prefix still match; otherwise quarantine/discard only unreferenced staging. |
| During validation or after a mismatch | Source remains selected. Do not serve the target, repair source bytes, or infer validity from counts or successful parsing alone. |
| Ready, before activation | Source remains authoritative. The target may be revalidated and activated explicitly or the migration may be aborted. |
| During selector replacement/sync | Keep service stopped. Resolve to one valid source or target from durable selector and migration identity; disagreement or invalidity fails closed. |
| Activated, before a target write | Target is selected but read-only; the source remains eligible for explicit offline rollback. |
| Before a target mutation can commit | Durably enter `write-pending`, or atomically record it with the mutation. Block source rollback while the result is unresolved. |
| Target mutation succeeds durably | Persist `active-committed`; source rollback is permanently closed. |
| Target mutation is proven not to have committed | Durably return to `active-reversible`; explicit offline source rollback remains available. |
| Target mutation outcome is ambiguous | Keep `write-pending`, refuse source rollback, and fail closed until target state is reconciled. |
| During cleanup | Target remains authoritative. Source and orphan bytes remain visible; cleanup cannot delete the selected target or required recovery artifact. |
| Process restart at any phase | The durable migration record and selector determine recovery. Directory order, timestamps, and parseability are not authority. |
| Corrupt source/target or mismatched identity | Refuse service for the affected scope, preserve bytes, and report expected/observed identity. Never silently rebuild empty state. |

Rollback and downgrade are different operations:

| State | Supported action |
| --- | --- |
| Before activation, source selected | Abort migration; source remains active. Remove target bytes only after proving they are unreferenced staging. |
| Selector activation is ambiguous | Do not serve either generation or start an old binary. Resolve the durable selector/record disagreement or restore a verified source state. |
| Target active, no target-only mutation, reversible state | Stop every target process, verify the migration record proves no target-only mutation, explicitly select the unchanged source, reopen it with its compatible binary, and check identity and logical state before serving. |
| Write pending | Keep service stopped and target selected; reconcile the operation. Return to reversible only with durable proof of no mutation, otherwise close rollback or remain fail-closed if uncertain. |
| Target mutation committed | Keep target authoritative. Older binaries that do not declare support fail closed. Restore from a verified recovery artifact or use a separately tested reverse conversion. |
| Source retained after target-only writes | Retention is not rollback: selecting the stale source would hide acknowledged or otherwise durable state. |

Before a target-only mutation can commit, durably record `write-pending`, or
atomically record it with that mutation. If the mutation commits, durably mark
the migration `active-committed`. If it is proven not to have committed,
durably restore `active-reversible`. A crash or ambiguous outcome leaves the
operation pending and blocks source rollback until recovery resolves it. Every
mutation counts, including message publishes, acknowledgements, delivery
attempts, consumer-policy changes, deduplication state, and clustered Raft or
state-machine writes. Recovery must never infer no mutation from a missing
client response or a retained source directory.

Automatic downgrade is unsupported. Editing version fields, removing selectors,
renaming directories, or starting an older binary against target-only active
state can hide acknowledged writes or move progress backward. A verified
recovery artifact must include identity, offsets, consumer state, attempts,
and producer retry identity, not just message bytes.

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

Before any target-only durable mutation is attempted, persist `write-pending`
or atomically commit it with the mutation. If that state cannot be confirmed,
do not accept the mutation. After a successful commit, persist
`active-committed`; after a proven no-effect failure, persist
`active-reversible`. An unresolved `write-pending` state keeps service stopped
and blocks rollback. No process may infer rollback eligibility from the absence
of a client response or from a retained source directory.

For the clustered path, the active migration identity and generation must be
consistently recorded by the metadata and data groups. A stale leader, delayed
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
  first-write state and whether source selection is still eligible;
- writer-fence owner/epoch, stale-owner rejections, activation attempts/result,
  serving/recovering/blocked state, and cleanup/orphan bytes; and
- for clusters, per-group target readiness, lagging replicas, snapshot source
  boundary, generation agreement, and leader/serving eligibility.

Metrics may use bounded labels such as engine, phase, outcome, and
reason. Stream, group, consumer, and migration identifiers belong in
structured logs or an explicitly bounded diagnostic response, not unbounded
Prometheus labels. Counters must state whether they reset on process restart.
Progress gauges must not be mistaken for a durability acknowledgement.

Readiness MUST be false, or the affected scope must refuse service, when
required metadata is corrupt, a migration is ambiguous, a target is not
validated, a replica lacks the active representation, or recovery has not
established a unique authoritative generation. “Started successfully” is not
a recovery result.

## Implementation acceptance matrix

The following matrix is the merge gate for a future implementation. “Current”
means existing evidence at the baseline; “future” means required work and is
not implemented by this document.

| ID | Scenario | Setup/fault | Required oracle | Evidence status |
| --- | --- | --- | --- | --- |
| COMP-01 | Artifact inventory | Fixture each local, clustered, snapshot, journal, and peer artifact with version/identity descriptors. | Matrix records read, write, mixed, and migrate behavior, limits, and downgrade boundary for every artifact. | Future; current inventory is documented above. |
| VAL-01 | Read-only preflight | Unknown version, malformed JSON, unknown required field, identity mismatch, missing/partial layout, and contradictory selector/manifest. | Fails before serving or mutating authoritative bytes; never opens an empty replacement; diagnostics identify artifact and expected/observed identity. | Cluster refusal tests cover identity/layout/version subsets; current preflight does not prove cross-artifact semantic agreement, and migration fixtures are future. |
| VAL-02 | Logical state image | Old fixture with records, mixed frame families, keys, opaque bytes, timestamps, offsets, ordinary/grouped consumer progress, attempts, and request IDs. | Exact state comparison passes; no renumbering, dropped bytes, progress rollback, duplicate request identity, or unacknowledged-state loss. | Future. |
| VAL-03 | Bounds and corruption | Oversized lengths, checksums, gaps, duplicate offsets, impossible checkpoints, malformed complete tail, and supported incomplete tail. | Bounded allocation; only the documented incomplete tail is recoverable; complete corruption/unsupported data remains intact and fails closed. | Current parser tests cover subsets; conversion gate future. |
| XFER-01 | Bounded copy/resume | Interrupt after each bounded copy checkpoint and restart with matching and mismatching target prefixes. | Resume is idempotent from verified progress or discards only unreferenced target; source remains usable and memory/temporary work is bounded. | Future. |
| XFER-02 | Interrupted transfer | Kill at preflight, copy, validation, ready, selector replacement, target reopen, and cleanup; include multiple snapshot chunks for clustered path. | Receiver/target never serves partial state; exactly one valid generation is authoritative after restart; migration phase/outcome explains the result. | Existing snapshot retry-from-zero is separate; migration gate future. |
| ACT-01 | Activation crash | Fault file sync, rename, selector sync, and migration-record completion at each activation boundary. | Recovery chooses a valid old or new image from durable identity/protocol state, never directory order or empty fallback. | Future; filesystem-specific evidence required. |
| FENCE-01 | Local stale writer | Keep old process/owner delayed across fence, copy, activation, and cleanup; issue publish and ack. | Stale operations get explicit retryable/fenced outcomes before durable mutation; acknowledged progress and source/target identity remain correct. | Future. |
| FENCE-02 | Cluster stale owner | Delay old leader, forwarded request, duplicate migration owner, and unready replica across committed activation. | Only the committed active epoch can append/ack or lead; unready replica cannot serve; no split-brain generation. | Existing leader/follower tests are not migration evidence; future. |
| ROLL-01 | Pre-activation rollback | Abort or restart a migration in planned through ready; retain staged target. | Source remains readable and authoritative; abort is idempotent; target can be safely discarded without source mutation. | Future. |
| ROLL-02 | Post-activation downgrade | Start an old binary against target-only active state; test version edit, selector removal, and path rename attempts. | Old binary fails closed; no acknowledged target state is hidden; documented reverse conversion or recovery-artifact path is required. | Future. |
| ROLL-03 | First target mutation boundary | Interrupt before, during, and after the first mutation; inject proven pre-commit failure and ambiguous outcomes. | `write-pending` blocks rollback; committed mutation closes it; only durable proof of no effect restores reversibility. | Future. |
| OBS-01 | Diagnostics and metrics | Exercise every phase, failure reason, fence rejection, cleanup orphan, restart, and process-counter reset. | Versions, identities, progress, outcome, rollback eligibility, readiness, and orphan state are visible with bounded labels and no secret/path leakage. | Future; current startup and snapshot metrics are partial evidence. |
| E2E-01 | Local real process | Publish/consume/ack representative records, migrate, kill/restart at each phase, then use the public protocol. | Same logical records and acknowledged state are observable before/after; redeliveries are allowed only where the contract says; health/readiness recover. | Future; just smoke does not exercise migration. |
| E2E-02 | No mixed clustered conversion | Three real broker processes; stop all nodes, stage and validate all targets, interrupt activation, and attempt stale-binary and mismatched-generation restarts. | No node serves with a mismatched generation; all nodes resume only after uniform target activation, or all return to source while rollback remains eligible. | Future; current cluster tests do not exercise storage conversion. |
| E2E-03 | Cluster physical activation | Three nodes with leader/follower failures, snapshot install, target readiness, and replacement identity checks after whole-cluster outage. | Activation and first-write rollback state survive restart; every node validates the same target before serving; acknowledged records/progress and request identities remain intact. | Future. |
| RES-01 | Migration headroom | Large retained stream, bounded batch sizes, temporary-space reserve, and no-reserve condition. | Admission pauses/throttles/rejects explicitly; memory, temporary bytes, and recovery work remain bounded; no false durable success. | Future targeted resource test. |

The implementation must not be called supported until the applicable future
rows have tests or operational evidence at the appropriate layer. A later ADR
is required for online conversion, rolling mixed-version compatibility,
downgrade, or any change to the accepted rollback and activation boundary.

## Implementation evidence sequence and exit gates

The following order is a risk-reduction guide, not a prescribed module, API,
or file-layout decomposition. An implementation may satisfy a gate with
different mechanisms while preserving the accepted outcome.

1. **Compatibility descriptors and fixtures:** establish Runnel-owned artifact
   descriptors, version ranges, identities, limits, and state-image equality.
   Exit when unsupported and contradictory layouts fail before mutation.
2. **Manifest and preflight:** establish migration-record parsing,
   source-boundary capture, space checks, and deterministic selector recovery.
   Exit when ambiguous state cannot serve.
3. **Local side-by-side conversion:** demonstrate one representative format or
   layout conversion with bounded checkpoints and exact logical comparison.
   Exit when interruption/resume and pre-activation abort are idempotent.
4. **Fence and activation hardening:** demonstrate stale publish/ack/owner
   rejection, filesystem sync/rename fault behavior, target reopen, and
   diagnostics. Exit when no stale operation can commit across cutover.
5. **Cluster maintenance conversion:** demonstrate all-node shutdown,
   per-node target readiness, uniform activation recovery, stale-binary
   refusal, leader/follower/replacement behavior, and separation from snapshot
   recovery. Do not permit mixed-version serving.
6. **Operational and resource acceptance:** expose bounded diagnostics/metrics,
   run large-stream headroom tests, and document backup/cleanup workflow.
7. **Operational support review:** expose bounded diagnostics and recovery
   instructions, name tested filesystems and the verified recovery artifact,
   and keep any unsupported artifact or cluster path explicitly unavailable.

## References and design evidence

The sources below are direct primary or project-maintained references. Their
behavior is evidence for design constraints, not a compatibility target for
Runnel.

| Source | Relevant fact | Difference and Runnel implication |
| --- | --- | --- |
| [Apache Kafka 4.3 upgrade](https://kafka.apache.org/43/getting-started/upgrade/) and [protocol design](https://kafka.apache.org/43/design/protocol/) | Kafka separates a rolling binary phase from finalizing a feature/metadata version; its upgrade guide disallows metadata downgrade when metadata changes. | The distinction between binary rollout and durable-format activation is relevant. Runnel's peer protocol has no negotiated compatibility level, and its consumer and producer identities add state Kafka's procedure does not define; ADR 0037 therefore rejects rolling conversion. |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html) | `--check` performs preflight; copy is the default. Copy/clone retain the old cluster, while link/swap can make it unusable or destructive once the new cluster starts or transfer begins. Both servers are stopped for upgrade. | This supports the accepted offline, side-by-side source boundary. Unlike PostgreSQL copy mode, Runnel must close rollback once any target-only durable state is accepted, because reselecting a stale source would hide acknowledged writes. |
| [etcd 3.5→3.6 upgrade](https://etcd.io/docs/v3.6/upgrades/upgrade_3_6/) and [downgrade procedure](https://etcd.io/docs/v3.7/downgrades/downgrading-etcd/) | etcd requires a snapshot, operates during a mixed-version phase at the lowest common version, and permits binary rollback only during that phase; after all members upgrade, recovery requires snapshot restore or the formal downgrade procedure. | This supports visible phases and verified recovery evidence. Runnel lacks the protocol gate and cluster failure evidence needed for rolling upgrades, so its first clustered conversion requires whole-cluster maintenance. |
| [OpenRaft snapshot replication](https://docs.rs/openraft/0.9.25/openraft/docs/protocol/replication/snapshot_replication/) and [storage traits](https://docs.rs/openraft/0.9.25/openraft/storage/) | Snapshot metadata and storage interfaces carry committed/applied boundaries, membership, log persistence, state-machine persistence, and installation as separate concerns. | Runnel must validate application-state schema, group identity, consumer state, attempts, and deduplication in addition to consensus boundaries. Snapshot replacement is not format migration. The repository currently pins OpenRaft 0.9.25. |
| [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST) | A transactional version-edit log and `CURRENT` pointer select complete referenced file sets, while obsolete files may remain until no live version references them. | This supports a single explicit active-generation selector and delayed cleanup, but does not supply Runnel's identity, delivery semantics, source/target validation, or first-write rollback fence. |
| [Online asynchronous schema change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | Online readers/writers require compatibility between transition states; asynchronous schema assumptions can corrupt data even when parsing succeeds. | A future live-tail migration would need operation-level proofs for publish, acknowledgement, replay, and recovery. ADR 0037 avoids that transition with a write fence and maintenance window. |
| [Linux `rename(2)`](https://man7.org/linux/man-pages/man2/rename.2.html) and [`fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html) | Rename and file synchronization have distinct durability and filesystem semantics; syncing a file does not automatically establish directory-entry durability. | Selector replacement must be tested on each supported filesystem; a successful rename alone is not accepted evidence of crash-safe activation. |
| [Runnel Raft recovery research](../research/raft-recovery-and-replacement.md), [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md), [ADR 0019](../decisions/0019-clustered-storage-identity.md), [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md), [ADR 0024](../decisions/0024-explicit-offset-replay-read.md), and [ADR 0026](../decisions/0026-semantic-engine-error-classification.md) | Current accepted decisions separate snapshot-based replica recovery from retained state, keep public replay topology-free, establish a hidden future placement identity, and classify engine outcomes without exposing backend details. | ADR 0037 adds offline physical-format upgrade semantics without redefining replica replacement, local-to-cluster movement, or public engine outcomes. |

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
- A whole-cluster maintenance gate and one durable migration identity can make
  activation deterministic without changing the public engine contract.

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
- **Peer compatibility:** current peer frames have no version handshake. This
  supports the decision to prohibit rolling mixed-version conversion; any
  future exception requires a separate compatibility decision and evidence.
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

Primary evidence class: design/research. Secondary tags: correctness/recovery,
storage/recovery, compatibility/migration, operability, and resource safety.

No runtime benchmark is required for this documentation-only change. It changes
no code, serialization, lock scope, I/O path, scheduling, resource limit, or
benchmark workload. The current publish/cluster benchmarks do not exercise
migration and cannot establish upgrade safety. The future RES-01 gate must
use a targeted migration/resource workload, with source/target sizes, batch
limits, temporary-space budget, recovery work, and failure state recorded.

## Repository evidence and handoff boundary

Current startup refusal, local-stream read-forward, journal, snapshot,
identity, and real-process recovery evidence remains in the linked
source/tests. Earlier state-machine checkpoint, snapshot, and journal formats
fail closed; no reader or conversion path is provided. ADR 0037 accepts an
operational behavior contract but adds no runtime compatibility promise. The
backlog records that accepted design milestone; migration implementation and
end-to-end gates remain open.
