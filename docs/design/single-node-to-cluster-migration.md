# Single-node to clustered migration boundary

- Status: accepted migration boundary; runtime implementation deferred by [ADR 0043](../decisions/0043-offline-local-to-cluster-migration.md)
- Last reviewed: 2026-10-06
- Baseline: `66cacafc8545f010dc710b49a1947c65e8dad53d`
- Reading guide: [design-note conventions](README.md)
- Scope: backlog outcome [Make growth from one node to a cluster non-disruptive](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive)

## Summary

The accepted first local-to-cluster migration is an offline, all-stream,
side-by-side logical export/import from one supported local deployment into a
fresh three-voter static cluster. It preserves logical stream offsets, order,
timestamps, key and payload bytes, request identities, durable consumer
progress, attempts, configured policies, and pinned policy snapshots. The
source is stopped and durably fenced before export, and stays fenced throughout
copy, import, validation, activation, and endpoint cutover. Outage duration is
therefore proportional to transfer and validation; the first slice does not
promise a short fence or zero downtime.

The target remains a non-serving staging generation until every imported
stream, consumer state, request identity, and configured voter is validated.
After explicit activation and endpoint cutover, it is read-only until a
durable `write-pending` boundary makes the first target-only mutation
forward-only. The frozen source is retained as a rollback candidate only until
that boundary and as a stale recovery artifact afterward. See [ADR 0043](../decisions/0043-offline-local-to-cluster-migration.md)
for the accepted authority and preservation contract.

The migration is an engine boundary, not a durable-format upgrade. The
supported source is the migration-aware current broker using the current
`RNL3` writer/reader format and consumer-state schema. `RNL1`, `RNL2`, mixed
histories, old binaries, and obsolete state schemas are refused without source
mutation, even if an observed historical reader can decode them. Copying a
local file into a clustered directory,
republishing through the public API, or installing local files as an OpenRaft
snapshot is not a supported migration.

This note records the evidence and implementation gates behind the accepted
behavior; it is not a runtime-support claim or a file/API implementation plan.
Current behavior remains defined by [architecture](../architecture.md), code
and tests, and accepted ADRs. The authority, preservation, and rollback
outcomes in ADR 0043 are binding; proposed bundle fields, phase names, schemas,
and command shapes below remain illustrative mechanisms unless that ADR
requires their outcome.

## Boundary at the recorded baseline

The following distinction is important: this note records an accepted future
boundary; it does not turn the current engines into a migration service.

| Classification | Evidence in the current repository | Consequence for this note |
| --- | --- | --- |
| Observed local behavior | At the recorded baseline, the local broker selects one durable writer format at startup, scans known `RNL1`, `RNL2`, and `RNL3` frame magics, truncates an incomplete trailing frame during normal recovery, and persists consumer checkpoints/journal events. Consumer state includes the configured versioned policy and policy snapshots pinned to attempted offsets; process-local delivery members, tokens, and `Instant` deadlines are not durable. See [`BrokerState::open`](../../crates/runnel-core/src/broker.rs), [`ConsumerState`](../../crates/runnel-core/src/consumer_state.rs), [`StreamLog::open`](../../crates/runnel-core/src/stream_log.rs), and the recovery tests in [`runnel-core`](../../crates/runnel-core/src/lib.rs). | This is baseline reader evidence, not the migration eligibility rule. The accepted source is the current migration-aware broker and `RNL3` store; `RNL1` and `RNL2` are refused unchanged, including mixed histories. Preserve durable policies, snapshots, progress, and attempts, not volatile ownership. |
| Observed clustered behavior | The clustered engine selects the Raft backend at process startup. Startup validates clustered storage identity and persisted artifacts before opening groups; stream creation reconciles metadata `Creating`/`Active` state with one data group per stream and the configured peer set. The current layout uses `storage.json`, `groups/metadata`, and `groups/data/<hex-stream>` with an identity-bearing `group.json`. Grouped consumer state includes configured policy and per-offset policy snapshots along with durable progress and attempts; delivery ownership and deadlines are replicated. See [`PersistentEngine::open_with_config`](../../crates/runnel-raft/src/engine.rs), [`GroupConsumerState`](../../crates/runnel-raft/src/delivery.rs), [`GroupManager`](../../crates/runnel-raft/src/group_manager.rs), [`StateMachineStore`](../../crates/runnel-raft/src/state_machine_store.rs), and [`SnapshotState`](../../crates/runnel-raft/src/state_machine.rs). The detailed current artifact/version evidence is in the [TD-007 compatibility note](td-007-storage-compatibility-evidence.md) and [TD-009 snapshot note](td-009-snapshot-evidence.md). | A fresh target can be populated only through a future logical import path. The existing public `Publish`, `CreateStream`, and snapshot-recovery paths are not a local-to-cluster interchange format. |
| Observed absence | There is no migration command, import/export schema, durable migration phase, writer-fence epoch, endpoint-generation owner, or migration-specific status/metric in the current code. ADR 0031 accepts negotiated v2 outcomes, but its runtime remains incomplete. Existing clustered identity checks intentionally reject ambiguous state; they do not convert it. Current snapshot and peer metrics describe recovery activity only. Existing tests cover local recovery and clustered restart/failure, not cross-engine migration. | Fence, import, activation, rollback, and migration-status behavior are accepted for future implementation under ADR 0043; none is current support. |
| Accepted first supported boundary | Offline, all-stream logical export/import into an empty, fresh three-voter target. The whole source deployment is durably fenced before export and remains unavailable through validation and explicit endpoint cutover; target is read-only until its first-write gate. | [ADR 0043](../decisions/0043-offline-local-to-cluster-migration.md) accepts the behavior but does not implement it. It preserves the messaging model, not availability during transfer, zero downtime, or automatic downgrade. |

The current evidence is useful but deliberately weaker than migration evidence.
Local tests cover request-ID recovery, mixed legacy/versioned frame replay,
consumer journal recovery, durable attempts, configured consumer-policy
isolation and persistence, per-offset policy pinning, dead-letter retry
identity, and incomplete/corrupt input. Cluster tests cover Raft state-machine
recovery, stream lifecycle, request-ID deduplication, grouped delivery fencing,
durable restart, consumer-policy and pinned-attempt transfer across leader
failure, and the test-only interrupted snapshot replacement experiment. The
real-process coverage is in
[`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs). None
of these tests proves a local export, cross-engine state conversion, writer
fence, endpoint switch, or rollback boundary; those remain explicit gaps in
the verification plan below. Separate real-server coverage now exercises an
application-shaped typed-client restart flow and ambiguous dead-letter
reconciliation, but those tests still keep one engine and one durable
representation throughout ([`client_path.rs`](../../crates/runnel-server/tests/client_path.rs),
[`server_smoke.rs`](../../crates/runnel-server/tests/server_smoke.rs)).

The later storage and engine evidence notes refine this boundary without
changing it: [TD-007](td-007-storage-compatibility-evidence.md) records the
tested read-forward and fail-closed storage cases, [TD-008](td-008-static-cluster-evidence.md)
separates static-cluster evidence from replacement support, [TD-009](td-009-snapshot-evidence.md)
and [TD-010](td-010-retained-state-evidence.md) document snapshot and retained-state
cost boundaries, and the [clustered outcome contract](clustered-outcome-contract.md)
keeps safe attempt outcomes separate from operation-stage evidence. [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)
also accepts durable versioned consumer policies and per-offset policy
pinning; this state is included in the transfer boundary here. None of these
decisions or notes implements or authorizes migration.

The test-to-claim mapping is:

| Current claim | Existing evidence | Not established by that evidence |
| --- | --- | --- |
| Local logical history can be recovered and scanned safely | `versioned_reader_replays_mixed_legacy_and_versioned_frames`, `incomplete_trailing_frame_is_discarded_on_recovery`, `complete_legacy_record_with_malformed_key_fails_closed_on_recovery`, and `versioned_checksum_corruption_fails_recovery` in [`runnel-core/src/lib.rs`](../../crates/runnel-core/src/lib.rs). | A stable export schema, source-generation marker, or cross-engine digest. |
| Local durable consumer progress/attempts and configured-policy/pinning behavior | `consumer_delivery_journal_recovers_committed_events_and_discards_partial_tail`, `acknowledged_group_progress_and_retry_state_survive_restart`, `consumer_policy_is_isolated_durable_and_pinned_per_delivery`, `attempted_delivery_keeps_its_policy_snapshot_after_restart_and_update` in [`consumer_policy_recovery.rs`](../../crates/runnel-core/tests/consumer_policy_recovery.rs), and `request_id_deduplication_survives_restart` in [`runnel-core/src/lib.rs`](../../crates/runnel-core/src/lib.rs). | Conversion into clustered state, export of configured and per-offset policy snapshots, or behavior for a fence racing with an acknowledgement. The integration test confirms local journal recovery only; it does not establish cross-engine conversion or migration fencing. |
| Cluster state recovers and rejects ambiguous storage | `persistent_engine_recovers_committed_state_after_reopen`, `persisted_storage_rejects_cluster_identity_mismatch_without_rewriting_data`, `partial_cluster_layout_is_rejected_without_opening_as_empty`, and `rejected_snapshot_install_preserves_existing_state` in [`runnel-raft/src/lib.rs`](../../crates/runnel-raft/src/lib.rs). | Local-to-cluster import, migration authority, endpoint ownership, or production replica replacement. |
| Cluster delivery and process failures have a correctness baseline | `three_process_cluster_preserves_group_delivery_through_replica_restart`, `three_process_cluster_reassigns_group_delivery_after_node_failure`, and `three_process_cluster_replicates_and_recovers_after_failures` in [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs). | Any cross-engine cutover, stale local writer rejection, or rollback after target writes. |
| Cluster consumer policy and an attempted record's pinned policy survive leadership change | `persistent_raft_consumer_policy_is_durable_and_pins_attempts` in [`runnel-raft/src/lib.rs`](../../crates/runnel-raft/src/lib.rs) and `three_process_cluster_transfers_consumer_policy_and_delivery_snapshot_after_leader_failure` in [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs). | Importing local configured policies/snapshots into clustered state or migrating a policy update that races with the source fence. |
| Engine failures have a backend-independent retry boundary | `classifies_failures_without_exposing_backend_details` and `retains_diagnostic_sources_for_backend_failures` in [`runnel-engine/src/lib.rs`](../../crates/runnel-engine/src/lib.rs), with shared local and clustered assertions in [`engine_contract.rs`](../../crates/runnel-core/tests/engine_contract.rs) and [`runnel-raft/src/lib.rs`](../../crates/runnel-raft/src/lib.rs). | The classification does not identify a migration phase, writer-fence epoch, commit/apply stage, or endpoint authority; a future migration surface still needs explicit evidence for those boundaries. |
| Provisional protocol support is declared consistently | `protocol_support_stays_aligned_across_wire_client_and_server` in [`runnel-server/src/protocol.rs`](../../crates/runnel-server/src/protocol.rs) checks the shared `runnel-json-lines` v1 declarations. | This source-level declaration is not the target protocol for migration; ADR 0031 accepts negotiated v2, but runtime negotiation and cross-version compatibility remain unimplemented. |

## Current evidence and the boundary it creates

The current implementation provides two implementations of the same intended
messaging contract, but not two compatible durable representations. The public
boundary is deliberately topology-free: [`Engine`](../../crates/runnel-engine/src/lib.rs) exposes streams,
publishes, polls, replay, acknowledgements, and health, while engine selection
is made when the process starts. [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md)
explicitly defers mixed engines and live engine migration.

### Local state

`runnel-core` currently owns the following state:

| State | Current representation | Migration consequence |
| --- | --- | --- |
| Stream history | At the recorded baseline, `streams/<stream>.log` may contain `RNL1`, `RNL2`, and request-aware `RNL3` record families. Each frame carries a logical offset, publish timestamp, optional UTF-8 key, and payload; `RNL3` also carries typed request identity. Current request-aware writers bound keys to 128 bytes, payloads to 64 MiB, and request IDs to 1 KiB. | The migration-aware source must contain only current `RNL3` frames. `RNL1`, `RNL2`, and mixed histories are rejected before source mutation regardless of historical parser support. Preserve fields and bytes in an explicitly versioned import representation, not local frame layout or file name. |
| Recovery/index state | The local log scans complete frames on open, truncates only an incomplete trailing frame, retains a bounded recent index, and uses a bounded sparse index for older reads. The async engine dispatches this synchronous work through bounded per-stream storage lanes; those lanes are execution isolation, not a migration boundary. | Export only after normal recovery has established a complete source boundary. A malformed complete frame is a validation failure; it must not be skipped or turned into a gap. |
| Producer retry identity | The local `request_ids` map is rebuilt from request-aware frames. Under [ADR 0034](../decisions/0034-publish-request-id-content-contract.md), an exact public retry returns its first offset and changed representable key or payload is a confirmed conflict. | Import each identity kind, original offset, and comparison content. Reject a conflicting mapping and preserve exact-retry/conflict semantics. Records without an ID remain non-deduplicated; internal dead-letter move identities remain distinct under ADR 0029. |
| Ordinary and grouped consumer state | Local `consumers/<stream>/<consumer>.json` stores `committed_offset`, out-of-order `acknowledged_offsets`, `delivery_attempts`, optional versioned `policy`, and per-offset `delivery_policies` pinned on first assignment and reused on retries. The adjacent `.json.tmp` path is an append-only event journal with a bounded size; checkpoint compaction writes a separate `.checkpoint.tmp` file and renames it into place. Older checkpoints/journal events default absent policy fields. | Convert this logical state into the clustered consumer-state schema. Preserve the configured policy version and values plus every persisted per-offset policy snapshot with its attempt; these snapshots keep an in-progress record's retry budget stable across a later policy update. Do not copy the JSON file or either temporary path as if it were a clustered snapshot. Validate every offset and policy against the imported stream and accepted policy limits. |
| Active deliveries | Local in-flight ownership, deadlines, and delivery tokens are process memory. Attempts are persisted before a delivery is returned, but local tokens do not survive restart. | Do not transfer local tokens, members, or `Instant` deadlines. At the fence, outstanding deliveries become eligible redeliveries on the target; an acknowledgement that races after the fence is rejected and must be retried against the target. |
| Stream and consumer names/paths | Stream, consumer, and member names are restricted to 1–128 ASCII letters, digits, `.`, `_`, and `-`; stream and consumer names are later used below the local `streams` and `consumers` directories. | Validate names before export and again before import. A migration tool must never accept an arbitrary source path or infer a name from an unsafe filename. |

The local durable log retains all history in the current slice. Its bounded
in-memory indexes do not mean that old records are unavailable: old replay and
delivery lookups can scan from a sparse checkpoint. This distinction matters
when estimating migration work and memory.

### Clustered state

`runnel-raft` uses a metadata Raft group and one data group per stream. The
metadata group records stream identity and the `Creating` to `Active` lifecycle;
the data group contains the stream records and consumer state. The first
clustered topology statically replicates every group to the configured voter
set, which is three voters in the initial deployment. The current data-group
identity is derived deterministically from the stream name rather than
allocated as an arbitrary migration identity. [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md),
[ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md), and [ADR 0023](../decisions/0023-independent-retained-storage-and-placement.md)
make those group, snapshot, and retained-storage boundaries explicit.

The clustered state machine currently materializes complete retained messages.
`state-machine.json` and snapshot payloads emit format version 2 and read the
tested version-1 forms; the OpenRaft `snapshot.json` wrapper also carries
snapshot metadata. `state-machine.log` is a separate length-prefixed JSON
journal with record format version 1 and a 64 MiB record bound. These are
separate persistence and recovery boundaries, as documented in the [TD-009
snapshot evidence note](td-009-snapshot-evidence.md) and [TD-010 retained-state
evidence note](td-010-retained-state-evidence.md). Its state includes:

- stream metadata, lifecycle, and `StoredMessage` values, with offsets implied
  by their position in the stream vector;
- ordinary consumer offsets;
- grouped consumer `committed_offset`, out-of-order acknowledged offsets,
  delivery attempts, optional versioned consumer policy, per-offset policy
  snapshots, in-flight member/token/deadline state, and the replicated
  lease-clock floor;
- per-stream request-ID-to-offset deduplication; and
- redelivery and dead-letter counters.

The counters are operational observations rather than message semantics. The
local counters are process-lifetime values and are not recoverable source
state; the migration may reset them while reporting the reset in diagnostics.
Existing dead-letter streams and their records are ordinary streams and must be
copied. Historical duplicate dead-letter records must not be silently merged.

Cluster startup validates existing `storage.json`, group directories,
data-group manifests, Raft logs, state-machine checkpoints, snapshots, and
journals before opening groups. It refuses legacy single-group layouts,
unmarked clustered state, partial layouts, and cluster/node identity
mismatches. An actually empty directory is intentionally initialized with
`storage.json`; that initialization is not conversion or migration. These
checks are important safety boundaries, but they are not a local-to-cluster
converter:
[ADR 0019](../decisions/0019-clustered-storage-identity.md) says that storage
identity must not be guessed, and [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md)
keeps empty-replica recovery test-only.

### Public outcomes during and after migration

The provisional client makes transport failures after a request may have been
written `Unknown`. A publish with a stable request ID can be retried explicitly
and resolve to the original offset; a publish without one remains ambiguous.
At the engine boundary, `BrokerError::kind()` and `BrokerError::outcome()` now
provide a backend-independent reason and conservative rejected/retryable/
unknown attempt classification, as accepted by [ADR 0026](../decisions/0026-semantic-engine-error-classification.md).
That classification does not expose a commit/apply stage or migration
authority, and the v1 server still maps existing variants to its provisional
codes. The server also treats request timeouts conservatively because engine
work may already have committed. The migration cannot turn an unknown
pre-fence publish without a request ID into a confirmed or deduplicated result.

After activation, the same public protocol and engine contract remain in use.
Clients reconnect to the target endpoint; they do not learn Raft groups,
stream placement, storage paths, or node identities.

## Goals and non-goals

The accepted first boundary has these goals:

- move a complete supported local deployment, including all streams and
  consumer state, to a fresh supported static cluster;
- preserve logical offsets, record ordering, timestamps, keys, exact payload
  bytes, offset and timestamp replay eligibility, request-ID retry identity,
  acknowledged progress, persisted attempts and retry schedules, configured
  consumer policies, and pinned policy snapshots including retry delay;
- make the authoritative writer and serving deployment unambiguous after every
  interruption or restart;
- make copy, validation, fencing, cutover, and cleanup progress visible and
  bounded by configured resources; and
- allow the application to continue using the existing stream, consumer,
  poll, replay, acknowledgement, and publish intent after reconnecting.

The first boundary does not include:

- zero-downtime online copying, live tail replication, or dual writes;
- an in-place conversion of a local directory into a clustered directory;
- republishing through `publish` as the migration transport;
- migration into an already populated target or merging two deployments;
- dynamic membership, automatic placement, changing the static three-voter
  topology, or production empty-replica replacement;
- changing retention, replay, retry, dead-letter, or ordering semantics during
  migration;
- automatic downgrade or pointer rollback after target-side state changes; or
- exposing migration paths, Raft terms, group IDs, offsets as physical file
  positions, or placement as normal application concepts.

“Supported” here means a documented, versioned workflow with recovery tests.
It does not mean that an old software generation, a historical format merely
readable for compatibility, arbitrary target version, or future distributed
engine can be migrated automatically.

## Accepted authority outcome and illustrative record

ADR 0043 requires one source generation to remain immutable and explicitly
fenced while one target generation is staged. An implementation needs durable
authority evidence bound to at least:

- a unique migration ID;
- source generation and target generation identifiers;
- source engine and target engine/schema descriptors;
- the source deployment identity and the new target cluster identity;
- a writer-fence epoch;
- a per-stream source next offset, retained-history floor, record count, and
  content digest;
- target group identities and imported-state digests; and
- phase, bounded progress, last-progress time, validation result, and failure
  reason.

The controller writes a durable migration record and source-generation fence
before export. Every supported source-broker startup must honor the marker and
refuse ordinary service until an authorized migration resume or abort resolves
it; an unsupported old binary cannot write the marked source. The source's
stream and consumer artifacts remain immutable after the final inventory, with
authority metadata stored separately. Exact paths and administration
interfaces remain implementation choices. Target selection must use an
explicit durable generation record, never directory order or whichever
partial file parses. RocksDB's [`CURRENT` and `MANIFEST` design](https://github.com/facebook/rocksdb/wiki/MANIFEST)
is a selector/recovery reference, not a format or procedure Runnel adopts.

The phase names below are illustrative. The source/target authority and
rollback outcomes are accepted by ADR 0043:

| Phase | Authoritative deployment | Allowed actions | Restart result |
| --- | --- | --- | --- |
| `planned` / read-only `preflight` | Source | Normal application traffic; target is absent or empty. Planning cannot mutate source state. | Source starts normally. Failed preflight leaves it unchanged. |
| `fenced` / `copying` / `validating` | Source is the frozen logical authority; neither generation serves application traffic | Source ordinary startup is blocked. Target accepts only migration import and read-only validation, never application writes. | Resume only the same migration from a verified checkpoint, or explicitly abort after proving target is unactivated and source inventory still matches. |
| `ready` | Source remains the rollback generation; target is complete but not selected for writes | All source/target inventories and every target voter validate. The migration controller can commit activation. | Keep both generations non-writable until activation or explicit abort. |
| `active-reversible` | Target is selected; source remains fenced and rollback-eligible | Target serves reads only. Target writes remain blocked by the global first-write gate. Endpoint owner must match the selected target generation. | Reconcile endpoint and target activation; explicit offline rollback remains possible only after proving no target-only mutation was accepted. |
| `write-pending` | Target is the only possible authority; source rollback is forbidden | First target mutation outcome is unresolved. Further mutations fail closed pending reconciliation. | Resolve against target durable state. Never select source while the outcome is ambiguous. |
| `active-committed` / `complete` | Target | Normal application traffic through target; source is stale retained recovery material. Cleanup follows a separate explicit policy. | Recover forward from target. A source pointer rollback would hide acknowledged target state. |

The authority order is: durably fence source, freeze and inventory it, import
and validate target on all configured voters, durably activate the target as
read-only, record/switch the external endpoint, then admit target writes only
through the durable first-write gate. A crash may extend downtime. If endpoint
state is ambiguous, the source remains fenced and target writes remain
disabled until the generation record and endpoint owner agree.

## Supported input and target boundary

The future implementation must accept only an explicit compatibility matrix;
ADR 0043 does not claim current migration support or automatic coverage of
formats merely readable for historical compatibility.

### Source

- A cleanly recoverable store produced by the currently supported,
  migration-aware source-broker binary, using only current `RNL3` stream
  frames and its current consumer-state schema. `RNL1`, `RNL2`, mixed histories,
  old software generations, and obsolete state schemas fail preflight without
  mutation; reader support alone never makes them eligible.
- Valid stream and consumer names, complete logical offsets, and consumer
  states whose offsets, attempt entries, configured policy, and per-offset
  policy snapshots can be checked against their stream and supported bounds.
- A source deployment that can durably fence every writer and acknowledgement
  path. The entire export/import/validation interval starts after that fence;
  this first slice does not use a live prefix copy or a final short tail fence.
- A source configuration whose delivery and retention behavior is either equal
  to the target or explicitly covered by a compatibility rule. The first
  implementation should preserve each configured consumer policy and its
  version, plus the policy snapshot pinned to each outstanding attempt. For
  consumers without an explicit policy, and attempts whose old journal event
  has no policy snapshot, require equivalent source and target broker-wide
  acknowledgement-timeout, attempt-limit, and retry-delay fallbacks unless the importer can
  preserve their effective behavior another verified way.

### Target

- A freshly initialized current clustered deployment with a new, explicit
  cluster identity and the configured static voter membership (three voters in
  the initial deployment).
- Empty metadata and data-group state, or an explicitly marked migration
  staging generation that contains no unrelated streams. Existing non-empty or
  identity-mismatched target state is rejected.
- A target binary that supports the migration schema, current public protocol,
  imported record limits, consumer-state conversion, request-ID deduplication,
  and the activation/recovery phases.
- Enough per-node disk and memory reserve for the target’s replicated retained
  state, staging metadata, journals/snapshots, and bounded transfer buffers.

### Refused cases

The workflow must fail before copying when it sees a corrupt complete frame,
unrecognized local format, invalid name, missing or inconsistent consumer
state, an active source writer that cannot be fenced, a target with unrelated
state, an unknown migration phase, insufficient resource reserve, or a
retention/configuration combination without a declared policy. A local store
with an incomplete final frame must first be recovered by the normal local
startup path; the migration must not guess whether that tail was an accepted
publish.

Earlier clustered single-group layouts, unmarked clustered directories, and
empty replicas with a reused voter identity remain outside this workflow. The
existing refusal behavior is a safety check, not a migration step.

## Preflight and writer fencing

Planning preflight is read-only and cannot establish the final source digest
while application writes continue. After the operator starts migration, the
source is drained and durably fenced; only then does the authoritative scan,
backup, and transfer begin. The implementation should:

1. before stopping service, validate migration-aware source/target binaries,
   source format eligibility, target emptiness and identity, and conservative
   disk/memory reserve without mutating either generation;
2. acquire exclusive migration ownership, stop new application operations,
   drain admitted work at one deployment-wide boundary, and persist the source
   migration ID and monotonically increasing fence epoch before declaring the
   source fenced;
3. recover and scan each stream through its declared current source reader,
   checking offset
   continuity, frame checksums where applicable, key UTF-8 validity, payload
   lengths, timestamp fields, and request-ID mappings;
4. load each consumer checkpoint and journal, replaying only complete events and
   checking committed, out-of-order acknowledged, and attempt offsets against
   `[earliest, next)`, and checking configured and per-offset policy versions
   and values against supported limits;
5. record source configuration and compatibility descriptors, including
   broker-wide fallback timeout, attempt limit, retry delay, configured
   consumer policies and pinned snapshots, retention/replay policy, current
   protocol and schema versions, declared source writer formats, and migration
   tool version;
6. create and independently verify a restorable recovery artifact from the
   frozen source boundary, separate from the target and from source-only
   rollback state; and
7. persist final source and target inventories, counts, bounds, and content
   digests before copying any application data.

The fence must be enforced by the source broker and migration controller, not
only by a service-manager convention:

- ordinary startup checks the durable marker and refuses service while it names
  an unresolved migration;
- all mutating paths, including stream creation, publish, poll/attempt,
  acknowledgement, policy configuration, retry scheduling, and dead-letter
  movement, check the same fence epoch;
- operations admitted before the boundary either finish durably and appear in
  the final inventory or return a definitive no-effect rejection; an unknown
  request outcome remains unknown to its client, though any committed effect
  is included in the frozen inventory; and
- only explicit migration resume or abort can release the source. An abort
  reopens it only after proving that target activation and target-only writes
  did not occur and that the frozen source still validates.

The current local engine has per-stream operation lanes but no persisted
migration epoch, so this is not implemented. Stopping a process by itself is
not a fence: every supported broker binary that can open the marked source
must honor the marker, and an older or unaware binary must fail closed rather
than serve it. Source record and consumer files remain immutable after the
final inventory; only separate migration-authority metadata changes.

At the fence boundary, drain acknowledgements already admitted, then stop new
polls and invalidate every remaining local delivery token. Preserve its
attempt and pinned policy state; the target issues only new target receipts.
The acknowledgement timeout is not transferred as a local `Instant` deadline.
Any durable retry-delay schedule is carried as bounded remaining delay and
rebased to target time; an in-flight attempt without an observed-expiry record
uses ADR 0033's target-side first-observation rule. A late source acknowledgement
is rejected by the marker and cannot advance imported progress. Redelivery can
duplicate application work, as permitted by at-least-once delivery, but cannot
lose or regress acknowledged broker state.

## Data and consumer-state transfer

Transfer logical state in bounded, checksummed chunks. The exporter reads
through the source engine/storage adapter, not by asking the application to
replay every record through the provisional network protocol.

### Stream bundle

For each stream, the migration bundle should contain a versioned header with:

- stream name, source generation, target stream identity, target data-group
  identity, and migration ID;
- source earliest offset, next offset, record count, retained-byte count, and
  digest algorithm/value;
- source record-format descriptors and compatibility limits; and
- a bounded sequence of chunks. Each record in a chunk carries its logical
  offset, key, exact payload bytes, publish timestamp, and optional request ID.

The importer appends or materializes records only when the next expected
offset matches. It verifies chunk lengths and checksums before durable apply and
verifies the complete stream digest before marking the stream ready. Replaying
the same migration ID and chunk ordinal with the same digest is a no-op;
reusing an ordinal with different content is a hard validation failure.

Do not use ordinary target `Publish` commands for historical records. A normal
publish would assign offsets from the target’s current state, would not carry
all source consumer state, and would make a failed request indistinguishable
from a migration retry. The importer needs an internal, versioned data-group
import protocol whose activation is separate from normal publish traffic.

### Consumer state conversion

For every `(stream, consumer)` pair, import:

- `committed_offset` exactly;
- every out-of-order acknowledged offset that is still at or above the
  committed offset;
- every persisted delivery attempt, preserving its maximum observed attempt;
- the configured consumer policy, including whether it is explicit, its
  monotonic version, acknowledgement timeout, attempt limit, and fixed retry
  delay;
- each outstanding attempted offset's pinned policy snapshot, so a policy
  change made after first delivery does not silently change that record's
  retry or dead-letter behavior;
- any durably scheduled retry-not-before state, represented as remaining
  bounded delay at the fence and rebased against the target clock;
- for attempted offsets with no persisted policy snapshot, the same effective
  source fallback policy, including legacy events whose attempt record predates
  per-offset policy snapshots;
- the consumer’s stream/name identity and a state digest; and
- no local delivery token, `Instant` deadline, or transient member ownership.

The target’s canonical clustered representation is a
`GroupConsumerState`. The import must create the equivalent grouped state even
for a local ordinary consumer, because the clustered compatibility path routes
ordinary poll and acknowledgement through grouped data-group operations. If
the target also materializes its legacy ordinary-consumer offset map, the
importer must establish and verify one coherent value rather than allowing the
two views to diverge.

An active local message that was not acknowledged at the fence remains
deliverable under the target's normal post-recovery retry rule. Its attempt
count and pinned policy are not reset, so the same attempt limit and delay
still govern later delivery or dead-letter movement. A target delivery token
is new and must be acknowledged only with the target response. Migration must
not make a source token valid on the target.

Existing `.dead-letter` streams are copied with their own records and consumer
state. A local dead-letter move may have been at-least-once across two files,
whereas a new clustered dead-letter move is one replicated transition. The
import must preserve the observed local history, including any already durable
duplicates, and only apply clustered atomicity to moves that happen after
activation.

### Request-ID deduplication

The source export preserves each recovered identity kind and mapping from
request-aware frames. For public IDs, the target must compare canonical key
bytes and exact payload bytes under ADR 0034: an exact retry returns the
original offset, while changed content is a confirmed conflict. Internal
dead-letter move IDs remain distinct from public identities under ADR 0029.
The target data group must have all mappings and comparison records before
application writes are enabled. A committed pre-fence publish whose response
was lost therefore resolves through any target node and after target restart
or leadership change when the retry carries the same ID and content.

For a pre-fence publish without a request ID, neither engine can safely infer
whether an unknown response corresponded to a committed record. The migration
must report that limitation; it must not invent an ID or silently remove a
possible duplicate. A publish that was definitely rejected before the fence can
be retried on the target. An acknowledgement whose durable outcome is unknown
is safe to retry after cutover because the target either contains the imported
progress or redelivers the unacknowledged record.

Changed-content reuse of a retained public request ID is rejected under ADR
0034; migration cannot weaken that rule or invent IDs for records that lack
them. If the source-format/target-schema combination cannot preserve identity
kind, original content, and offset, preflight refuses it rather than merging
identity namespaces or accepting an unverifiable retry.

## Target construction and cutover

The target cluster is a staging generation, not a normal serving cluster with
empty streams. The following sequence illustrates the accepted outcomes; file
and command choices remain open:

1. initialize target `storage.json` with a new cluster identity and validate
   every configured voter identity and address (three in the initial
   deployment);
2. create metadata records in `Creating` state and prepare one data group per
   source stream, deriving the current target stream/group identities from the
   stream name (`stream/<name>` and `group/<name>/data`) while binding them to
   the new cluster identity;
3. import stream chunks and consumer state into data groups through the
   migration protocol, with each committed chunk carrying migration ID, stream
   identity, ordinal, expected next offset, and digest;
4. require every configured voter to recover the same target generation and
   validate the imported digest and local durable state. A lagging or
   unavailable voter keeps target activation and endpoint readiness false;
5. commit one target metadata activation record containing the target
   generation, all stream digests, compatibility descriptors, and the writer
   activation epoch; and
6. activate the target read-only, explicitly record and switch the external
   endpoint, and verify that readiness identifies the target generation; and
7. admit the first target mutation only after the target has durably entered
   the cluster-wide `write-pending` state.

The target metadata record selects whether the staged generation may serve.
Because the current implementation has independent data groups and no
cross-group transaction, activation must include an all-stream readiness
check. If any stream is missing or not validated, target readiness is false
and no stream may appear as an accidental empty stream. Per-stream cutover is
outside ADR 0043.

An endpoint switch is external state and cannot be made atomic with a Raft
commit by the current system. The safe ordering is therefore conservative:

1. the source fence and frozen inventory are durable;
2. every target voter is caught up, and target activation selects a
   read-only generation;
3. the endpoint owner records the target generation and switches traffic;
4. a post-cutover probe confirms target generation and readiness while
   application mutation remains disabled; and
5. the first target mutation crosses its durable `write-pending` boundary before
   the operation can commit.

If the coordinator stops between these steps, a migration-status operation must
use the durable records to complete or abort the transition. It must not start
both brokers and infer authority from reachability.

## Rollback boundary

Use ADR 0037's first-target-write boundary, not target activation alone, as the
point after which the source is stale:

- before target activation, an explicit abort may discard unreferenced target
  staging and release the source fence only after revalidating the frozen
  source, backup, and durable migration record;
- after target activation but before any target-only durable mutation, target
  is selected and read-only. An explicit offline rollback is permitted only
  after every target process is stopped, durable target state proves that no
  target-only mutation was accepted, the endpoint owner records source
  selection, and the source's migration-aware startup validates the original
  generation before releasing its fence;
- before the first target mutation, a cluster-wide durable `write-pending`
  transition blocks source rollback. If the operation commits, target becomes
  `active-committed`. A proven no-effect failure may durably restore
  `active-reversible`; an ambiguous outcome stays pending, blocks further
  writes, and forbids source selection until target recovery resolves it;
- after any target-only durable mutation is accepted, recovery is forward-only
  from target state or a verified target recovery artifact. The source remains
  frozen but stale: selecting it would hide acknowledged publishes, consumer
  progress, attempts, or policy changes; and
- source cleanup is an explicit later action after the configured recovery
  window and backup policy permit it. Retained stale bytes are not permission
  to roll back or delete the only recoverable generation.

The write-pending rule applies to engine migration even though the source and
target use different artifacts. The migration needs a cluster-wide authority
record before the first user-visible target mutation because that mutation can
make the frozen local source stale. ADR 0037's artifact-by-artifact
read/write/mixed/migrate matrix remains specific to storage upgrades; this ADR
adds logical conversion equality, target read-only activation, and the
cross-engine authority barrier.

## Interruption, retry, and ambiguous outcomes

Each phase must have a deterministic restart rule:

| Interruption | Required result |
| --- | --- |
| Before the source fence | Source remains writable and authoritative. An incomplete read-only plan can be abandoned without changing logical state. |
| During fence/drain | Ordinary startup sees the durable source marker and refuses service until the same migration resumes or is explicitly aborted. It must not accept a publish while the boundary is unresolved. |
| During stream copy | The frozen source generation is unchanged. Target resumes from the last verified bounded chunk or staging is discarded. A partial chunk is never served. |
| During consumer-state import | The last durable state record is authoritative. Reapplying the same state digest is idempotent; a different digest for the same migration/consumer fails. |
| During target validation | Target remains not-ready. Source can be restored only through explicit abort and revalidation. |
| After read-only target activation but before endpoint switch | Source stays fenced; target writes stay disabled. Reconcile the endpoint to target or perform explicit offline rollback after proving zero target-only mutations. |
| During endpoint switch or status reporting | Readiness is conservative until endpoint owner and durable target activation agree. Unknown route state is an operational incident, not permission for two writers. |
| During `write-pending` | Target blocks further mutations; source rollback is forbidden until target state establishes the operation had no effect and a durable reversible state is restored. An ambiguous result stays pending. |
| After first target-only durable mutation | Target recovery is forward-only. Source pointer rollback is forbidden. |

### Required observable failure outcomes (not yet implemented)

The current protocol can report ordinary validation, cluster, not-leader,
stream-not-ready, stale-delivery, and transport/unknown outcomes, but it has no
migration outcome vocabulary. A future migration surface should make the
authority decision explicit without asking an operator to infer it from those
generic errors:

| Failure point | Durable migration outcome and readiness | Required operator/client interpretation |
| --- | --- | --- |
| Preflight or source validation | No migration mutation; source remains ready and authoritative. | Fix the input or configuration and retry the plan. No client reconciliation is needed. |
| Fence acquisition or drain cannot complete | `failed` or `aborted` before target activation; source remains fenced until the record is reconciled. | Do not start a second source. Resolve the recorded owner, then explicitly abort and revalidate before reopening source traffic. |
| Chunk, consumer-state, or digest mismatch | Target staging is not ready; source remains the authority if activation has not committed. | Quarantine or discard only unreferenced staging and investigate the named migration/ordinal/digest. Do not skip the record or continue from an unverified prefix. |
| Target replica or activation readiness failure | Target remains not ready; source stays fenced once cutover has begun. | Resume target recovery or declare a pre-activation abort. Never route clients to a partial or empty target. |
| Activation committed, route switch unknown | Target selection is durable and the target remains read-only; readiness is conservative until the endpoint owner agrees. Source rollback remains eligible only if the first-write gate proves no target-only mutation was accepted. | Reconcile the endpoint to target or keep service down. If rollback is chosen, stop every target process and perform the explicit offline rollback; never restart source based on reachability alone. |
| Stale source write or acknowledgement after activation | Source rejects the operation under the migration epoch; target accepts only a retried operation with its current delivery/request identity. | A publish with a stable request ID may be resolved explicitly; an ID-less publish remains unknown; a stale acknowledgement must not move target progress. |

These are accepted operator-visible consequences, not current response codes.
The implementation must define their serialization and add real-process
tests before the procedure can be advertised as supported.

The retry identity rules are equally important:

- a stable request ID present in a source record is imported before target
  serving, so a retry after an unknown response resolves to the original
  offset;
- a request ID absent from the source map is not treated as committed merely
  because a client timed out;
- a request with no ID remains unknown after a dropped response and needs
  application-level reconciliation; and
- duplicate chunk, consumer-state, activation, and status requests use the
  migration ID and content digest, not a new physical append.

The migration tool should expose whether a source request identity was imported
and whether a target retry resolved, but it must not expose physical offsets or
storage paths as new application concepts.

## Retention and replay guarantees

The current engines retain history from offset zero and expose one-record,
inclusive offset replay. Replay does not create delivery state, increment an
attempt, or change ordinary consumer progress. Migration preserves the exact
`[earliest, next)` range and returns the same record for every available
offset. An absent offset remains an explicit `history_unavailable` outcome; it
must not become ordinary `empty`. It also preserves publish timestamps so the
target can satisfy [ADR 0038](../decisions/0038-timestamp-based-replay-selector.md):
the timestamp selector returns the lowest matching logical offset despite
timestamp regressions and retains its accepted no-match and deleted-prefix
semantics. Any lookup index is derived state and is rebuilt/validated from the
imported records.

The migration must not use replay as a substitute for a bulk export. Replay is
an application read, does not carry local request-ID metadata, and is bounded to
one logical record. Import must read the source representation with a
source-aware adapter.

If retention is implemented before migration support, the fence must freeze
the source retention floor and any consumer/replay pins, and the target must
preserve those together with `next` and explicit unavailable-history outcomes.
A source whose retained history cannot satisfy the target's declared replay
scope fails preflight; migration never fills a gap with empty history.

## Compatibility policy

This is a logical migration between engine generations, with a deliberately
small compatibility matrix:

| Dimension | Supported first slice | Refused or deferred |
| --- | --- | --- |
| Public protocol | The target implements the negotiated application protocol accepted by [ADR 0031](../decisions/0031-protocol-v2-contract.md). Migration adds no previous-version compatibility or automatic client reconnection promise. | Provisional v1 declaration, mixed-version operation, or topology fields as migration evidence. |
| Local record encoding | Current `RNL3` stream frames and consumer-state schemas emitted by the migration-aware current source, subject to target representability. | `RNL1`, `RNL2`, mixed histories, old software generations, obsolete state schemas, unknown or malformed complete records, unbounded lengths, and guessed conversion. Historical reader support creates no compatibility promise. |
| Cluster representation | Current target metadata/data-group layout: `storage.json` and the Raft log use version 1, the state-machine journal uses record version 1, checkpoint and snapshot payloads emit version 2 with narrow version-1 read-forward support, and the current `group.json` manifest shape binds stream/group identity. | Import into an older target, unknown target schema, or arbitrary OpenRaft on-disk layout. |
| Consumer semantics | Local committed and out-of-order acknowledged progress, attempts, configured policy/version including retry delay, durable retry scheduling, and per-offset policy snapshots convert into coherent clustered state. Outstanding local tokens/deadlines are dropped and redelivered under target rules. | Transferring local receipts or monotonic deadlines, dropping a pinned policy or scheduled delay, or changing retry/ack semantics. |
| Producer identity | Public IDs preserve original offsets and exact comparison content under ADR 0034; internal dead-letter identities remain distinct under ADR 0029. | Deduplicating requests without IDs, inventing IDs, merging identity kinds, or changing key/payload conflict behavior. |
| Configuration | Preserve configured policies/versions and per-offset snapshots; require equivalent source/target fallbacks for acknowledgement timeout, attempt limit, and retry delay where effective behavior depends on them. Preserve retained-history floors/pins and offset/timestamp replay semantics. | Unreviewed policy changes that alter redelivery, dead letters, retention, or replay; resetting versions or pinned snapshots. |
| Identity | New target cluster identity; deterministic target stream identity and validated data-group manifests. | Copying local state into a target `storage.json`, reusing a different cluster/node identity, or guessing ownership. |
| Downgrade | Explicit offline source rollback can be eligible after read-only target activation until a target-only durable mutation is accepted; ADR 0037 governs the pending/commit resolution. | Automatic downgrade, old-binary startup against target-only state, source selection while `write-pending`, or rollback after target writes. |

The [safe storage-upgrade design](storage-upgrade-safety-plan.md) defines
related generation, writer-epoch, and fail-closed vocabulary. ADR 0043 applies
the offline source, activation, and first-write boundary to engine migration,
while adding logical state conversion and the all-voter target check. It does
not add generic artifact compatibility, rolling operation, automatic
upgrade, or downgrade support.

## Observability and operator controls

Migration status must be available without inspecting implementation files. It
should report, at minimum:

- migration ID, source/target engine and schema descriptors, source/target
  generation, target cluster identity, writer epoch, and current phase;
- source streams, total and per-stream records/bytes copied and validated,
  earliest/next offsets, digest status, last completed chunk, retry count,
  start/last-progress times, and last failure reason;
- consumer-state records copied/validated, request-ID mappings copied,
  outstanding local deliveries converted to redelivery, and any ambiguous
  pre-fence outcomes reported by the operator;
- target data-group readiness, lagging/unverified replicas, snapshot/journal
  recovery activity, activation record, endpoint generation, rollback
  eligibility, and source-fence status; and
- staging, backup, target reserve, cleanup, and orphan-byte status.

Metrics should use bounded labels such as engine, phase, outcome, and reason.
Stream, consumer, and migration IDs belong in structured logs or an explicitly
bounded diagnostic response. Useful counters include chunks attempted,
validated, retried, and failed; records/bytes copied and validated; fence and
stale-writer rejections; validation failures; activations; aborted migrations;
and cleanup/orphan bytes. Existing snapshot and peer-transfer metrics can
describe target replica recovery, but they do not replace migration progress
metrics.

Readiness must be false while the source fence or target activation is
ambiguous, while any required stream is not validated, or while a target
generation is only staging. A process that starts successfully but cannot
prove which generation it may serve is not ready.

This required migration status surface is not current HTTP or public protocol
behavior. Today, health and metrics expose broker, request, delivery, storage,
peer, and snapshot observations, but no migration phase, source-fence epoch,
target generation, or endpoint owner. Until the migration status exists, an
operator cannot infer migration authority from process start, a reachable
socket, or an ordinary health response.

## Resource bounds and operational budget

Migration is inherently proportional to the retained state being moved, so
“bounded” means bounded concurrent work, memory, queueing, and temporary space
relative to an explicit inventory—not constant total work independent of data
size.

ADR 0043 accepts these resource outcomes; implementation must choose and test
concrete values for the supported source/target schema:

- one migration per source deployment and one active import per stream unless
  measured resource isolation justifies more;
- a fixed maximum chunk byte/count budget below the selected protocol and
  peer-frame limits, with one or a small configured number of chunks buffered;
- checksum and digest work that streams payloads rather than retaining a whole
  stream or whole deployment in the migration process;
- bounded retry queues and a throttle so migration cannot consume all target
  peer, storage, or request-admission capacity;
- free-space preflight for the untouched source, its verified recovery copy,
  target staging, each of the three target replicas, journals/snapshots, and
  temporary files. The tool must reject a plan without reserve rather than
  fail halfway through after accepting traffic; and
- cancellation at chunk and state-record boundaries. Cleanup must be resumable
  and must not delete the active generation or the only recovery artifact.

The current clustered state keeps complete retained history in each data-group
state machine and snapshots rewrite complete materialized state. A large
source stream therefore has both network/disk transfer cost and a target
materialization cost. The design must not make a runtime or performance claim
until that cost is measured. Future local storage, snapshot, and retained-state
work tracked by TD-002, TD-009, and TD-010 may change the efficient import
representation without changing this logical contract.

## Reference designs and research

These sources inform the boundary; none establishes Runnel compatibility.

| Reference | Relevant mechanism | Difference that matters to Runnel |
| --- | --- | --- |
| [PostgreSQL 18 `pg_upgrade`](https://www.postgresql.org/docs/18/pgupgrade.html) | Provides a no-mutation compatibility check, initializes a separate destination, stops both servers, and defaults to copying so the old cluster remains usable until the new one is used; link/swap can remove that rollback property earlier. | This supports preflight, a quiesced source, and source-preserving generations. Runnel must logically translate records and consumer state because local files are not clustered state; it does not reuse `pg_upgrade`'s binary-compatible table files. |
| [Apache Kafka 4.3 MirrorMaker 2](https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/) | Separates cross-cluster topic mirroring from consumer-group checkpointing and exposes transfer/checkpoint progress. | This supports validating consumer state as migration data and surfacing progress. Kafka's replication offsets and topic/partition model do not equal Runnel's local/cluster logical offsets or per-offset delivery attempts; Runnel needs an engine-aware converter. Online mirroring remains deferred. |
| [Apache Kafka 4.3 upgrade guide](https://kafka.apache.org/43/getting-started/upgrade/) | Uses a staged rolling-binary phase followed by feature finalization, and says metadata downgrade is unsupported for releases with metadata changes. | This reinforces a precise compatibility and downgrade boundary. Runnel has no mixed-version guarantee and instead selects an offline new engine generation; a retained source is rollback-eligible only before target-only durable state. |
| [Apache Kafka partition reassignment](https://kafka.apache.org/36/operations/basic-kafka-operations/) and [leader-epoch fencing in the protocol](https://kafka.apache.org/37/design/protocol/) | Reassignment uses an explicit plan/verify workflow and a replication throttle. Kafka’s protocol carries leader epochs so stale clients/replicas can be rejected rather than allowed to write against an old authority. | Kafka moves replicas that already share one log protocol. Runnel’s local engine has no Raft membership, committed log identity, or migration epoch, so Kafka-like live reassignment cannot be applied to local files. Its explicit verification, throttling, and stale-authority rejection are useful requirements, but need a Runnel-owned fence. |
| [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/) and [runtime reconfiguration](https://etcd.io/docs/v3.7/op-guide/runtime-configuration/) | A new member receives state as a non-voting learner, cannot serve normal client traffic, and is promoted only after it catches up and passes safety checks. Learner count and replication load are bounded. | Runnel should apply the readiness-before-authority principle to each target replica. A local source is not an etcd/Raft member, so it cannot simply be added as a learner; logical import must first create target data-group state, after which normal controlled replica recovery can apply. |
| [The Raft paper](https://raft.github.io/raft.pdf) and [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/) | Consensus applies an ordered command stream and snapshots carry a committed state boundary and membership information for replica recovery. | A local log has no Raft log index, membership, or committed term to install. The target may use its normal Raft snapshot/recovery path after import, but local-to-cluster conversion needs a Runnel-owned bundle and schema validation before target activation. |
| [RocksDB MANIFEST/CURRENT](https://github.com/facebook/rocksdb/wiki/MANIFEST) | A transactional version-edit log plus a `CURRENT` pointer identifies the latest consistent generation; recovery does not infer state from arbitrary files and does not apply partial atomic groups. | Runnel needs the same explicit-generation and no-partial-activation discipline. Its marker must additionally bind stream, consumer, request-ID, engine, and writer-epoch semantics; a generic file pointer is insufficient. |
| [Online, Asynchronous Schema Change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/) | Online readers and writers can corrupt shared data when schema transitions are not mutually compatible; F1 constrains transitions to a formally safe bounded version window. | This is evidence against casually adding a live local writer plus clustered importer. An online Runnel design would need a compatibility proof for every old/new publish, acknowledgement, retry, and ordering interaction; the first slice therefore uses a fence. |

The differences are consequential: Kafka and etcd can stream state inside one
replication/consensus vocabulary, PostgreSQL provides a side-by-side process
boundary, RocksDB provides an explicit generation-selection pattern, and F1
demonstrates why mixed online writers need more than a reader that can decode
old bytes. Runnel combines the conservative parts of those designs while
keeping the public model independent of physical topology.

## Alternatives considered

### Live copy with a source tail

An exporter could copy a source prefix while the local broker remains live,
then copy a tail after a final sequence barrier. This reduces the maintenance
window, but the current local engine has no durable append sequence exposed to a
cross-engine importer, no dual-write transaction, and no writer epoch that a
cluster can enforce. A source publish, consumer acknowledgement, or dead-letter
move could be observed in one engine but not the other. Defer until a dedicated
online protocol specifies a consistent barrier and fault behavior.

### Dual-write every operation

Writing each publish and acknowledgement to both engines before responding
could keep the target warm. Sequential writes are not atomic: a crash can
commit one side, return an unknown outcome, or produce different offsets and
consumer fences. A two-phase coordinator would add a new durable protocol and
would still need history backfill. Reject for the first migration; evaluate
only with a formal transaction/compensation design and fault injection.

### Copy local files into the target

This is fast to describe but incorrect. Local stream frames, local consumer
files, and local in-memory ownership are not clustered state-machine snapshots;
cluster offsets are implicit in materialized message order and data groups need
cluster identity and Raft membership. Reject.

### Republish through the public API

Publishing records in order into an empty cluster is observable and could
preserve payloads, but it assigns target offsets independently, loses original
publish timestamps unless a new internal path is added, does not transfer
consumer progress or attempts, and cannot carry source request-ID mappings
without changing client behavior. It also makes interruption look like normal
application traffic. Reject as the migration mechanism.

### Use clustered snapshots as the interchange format

Snapshots are appropriate for replacing a clustered replica after a committed
Raft boundary. They contain clustered state and membership metadata and assume
the target group identity. A local source has neither. Reuse the target’s
snapshot and validation machinery after logical import, but do not pretend a
local log is an OpenRaft snapshot. Defer a common cross-engine snapshot format
until more than one migration needs it.

### Asynchronous mirror or external replication

An external mirror could minimize downtime and decouple migration scheduling,
but asynchronous mirroring can lose ordering or linearizability across a
disconnect unless the source and target define a durable sequence and replay
protocol. It may become useful for cross-cluster or disaster-recovery goals,
not as the first one-node-to-cluster cutover.

## Outcome and evidence gates

ADR 0043 accepts the boundary, not a prescribed code sequence. The gates below
name independently verifiable implementation outcomes; schema shape, chunk
protocol, phase storage, and command interfaces remain implementation choices
when they satisfy that contract.

1. **Schema and preflight outcome.** Evidence establishes a versioned logical
   export/import schema, source/target identity tuple, digest rules,
   compatibility matrix, and a read-only source scanner. Fixtures cover
   current `RNL3` source records, explicit `RNL1`/`RNL2` and mixed-history
   refusals, request identities, out-of-order acknowledgements, attempts and
   pinned policies, malformed state, invalid names, and unavailable history.
   Historical formats or records outside the declared migration matrix are
   proven to refuse without mutation.
2. **Fresh-target logical import outcome.** Evidence establishes an internal
   target data-group import mechanism with bounded, idempotent chunks,
   stream/consumer digests, explicit offsets, request-ID mappings, and
   target-side validation. Imported groups remain non-serving until complete;
   restart/resume is proven without public cutover.
3. **Durable fence and activation outcome.** Evidence establishes migration
   ownership, writer epochs, source drain behavior, explicit phase transitions,
   target metadata activation, conservative readiness, endpoint-generation
   reporting, and pre-activation abort, with crash coverage at each durable
   boundary.
4. **Real-process migration outcome.** Evidence exercises a real local broker
   and three real clustered broker processes with the public protocol before
   and after the migration tool. The fault matrix covers follower forwarding,
   leader change, target restart,
   target replica recovery, source restart attempts, stale writer attempts,
   endpoint cutover, and cleanup.
5. **Operational hardening outcome.** Evidence establishes bounded metrics,
   status output, disk/memory reserve checks, throttling, cancellation, orphan
   cleanup, backup/recovery documentation, and compatibility fixtures for
   future format versions.
6. **Online migration research, if needed.** Only after the fenced path is
   correct and its downtime/resource envelope is measured, design a separate
   live-tail or dual-write protocol and a new ADR. Do not expand the first
   migration implementation opportunistically.

## Real-process verification plan

The verification must prove serving authority and durable state, not only that
an import command returned success. Use unique temporary data directories,
broker/HTTP/peer ports, target/build resources, and process lifetimes. Prefer
the repository’s isolated workflow for process-heavy runs; the existing
[`just smoke`](../testing.md) and [`just cluster-test`](../testing.md) patterns
are the starting points for a future named migration workflow.

### Baseline and data fixture

Start a migration-aware current local source process and use the supported
client to create multiple streams, including a dead-letter stream, then
publish:

- empty and non-empty keys;
- binary and UTF-8 payloads;
- records with and without stable request IDs, including exact retries and a
  changed-content conflict;
- every writer format the source binary declares eligible for migration; and
- enough history to cross the local bounded tail index.

Create independent consumers and a shared consumer. Acknowledge records in and
out of order, leave one delivery in flight, persist retry-delay state and
multiple delivery attempts, and change a policy after one offset has pinned its
snapshot. Include timestamps that regress and exercise both offset and
timestamp replay. Record source health, replay results, consumer state,
request-ID mappings, policy fallback configuration, and per-stream digests.

### Fault and cutover matrix

For each phase and at least one representative stream, stop or interrupt:

- the exporter during a chunk and at the durable progress record;
- a target data-group process during import, validation, and activation;
- one target voter lagging or unavailable during all-voter validation;
- a target leader before and after a committed import or activation entry;
- the source process during fence drain and while a client has a pending
  publish or acknowledgement; and
- the endpoint coordinator between target activation and route switch; and
- the target process before, during, and after `write-pending`, covering a
  committed mutation, a proven no-effect result, and an ambiguous result.

After every interruption, verify the phase-specific authority rule, process
health, target readiness, source immutability, no partial stream visibility,
offset continuity, payload/key/timestamp equality, consumer progress, attempt
counts, and digest equality. A partial target must never appear as an empty
cluster.

After successful cutover, use the public protocol through follower and leader
addresses to verify:

- a pre-fence request ID with a dropped response resolves to its original
  offset and appends no duplicate, while changed content is rejected;
- a no-ID ambiguous publish remains explicitly unknown and is not silently
  retried by the migration;
- acknowledged offsets remain acknowledged and unacknowledged offsets are
  redelivered at the imported attempt count;
- old source tokens are rejected while target tokens acknowledge only target
  deliveries;
- keyed delivery preserves per-key ordering and unrelated keys continue to
  make progress under the target contract;
- replay returns the same pre-cutover records, leaves ordinary progress alone,
  reports the same unavailable range, and timestamp selection retains the
  lowest matching offset under timestamp regression; and
- target restart, follower restart, leader failure, and the documented target
  replica-recovery path preserve all imported state.

Test pre-activation abort, read-only activated rollback, first-write
reconciliation, and post-commit target recovery separately. A post-commit
failure must recover target state or fail closed; it must not pass by starting
the source with a stale configuration. Include a validation-failure case for
every field that can create an offset, identity, checksum, consumer,
request-ID, or policy mismatch. Prove that a source format outside the declared
current migration matrix is refused with all source bytes and consumer state
unchanged.

## Migration cost benchmark plan

No benchmark is required for this design-only change, and this note makes no
runtime throughput, latency, memory, or migration-duration claim. Existing
publish and cluster benchmarks measure unchanged paths and would not establish
migration evidence.

Once an implementation exists, add a migration-specific, sequential benchmark
under the documented host/resource lock. Use at least three retained-history
sizes spanning two orders of magnitude, 100-byte and 1-KiB payloads, streams
with and without ordering keys, request-ID density, consumer-state density, and
one interrupted-transfer case. Record:

- total and per-stream copy, validation, fence, activation, and cleanup time;
- records/bytes per second and target network bytes, with the durability point
  for each phase;
- source/target CPU, RSS, peak transfer buffers, disk usage, and temporary
  amplification per target node;
- target import/recovery time, chunk retries, interruption resume work, and
  post-migration replay/consumer checks; and
- repetition count, fixed CPU/memory/storage limits, raw artifacts, observed
  ranges, and stability status.

The benchmark must report the full source-unavailable interval separately
from copy, validation, activation, and endpoint work. It must not describe the
fence as short unless measurements establish that. It must not claim that the
current materialized clustered state scales to arbitrary retained history;
that remains an open storage design question.

## Implementation risks and evidence required before support

- **Fence linearization.** The current local engine serializes per-stream
  operations but has no migration epoch. Implement and fault-test the exact
  point at which a publish, acknowledgement, poll attempt, or dead-letter move
  is included or rejected.
- **Cross-group activation.** Metadata and stream data groups cannot currently
  commit one cross-group transaction. Prove the all-stream readiness/activation
  gate under target process loss and avoid serving any missing stream as empty.
- **In-flight consumer state.** Local tokens and deadlines are volatile while
  clustered delivery uses replicated tokens and absolute lease timestamps.
  Current local and clustered state also retain consumer policy versions and
  per-offset policy snapshots; the migration must preserve these while making
  source tokens stale. Existing local restart and clustered leader-failure
  tests cover policy persistence/transfer, but not cross-engine conversion.
  Verify redelivery, attempt limits, stale acknowledgements, and keyed ordering
  across the fence.
- **Request-ID conversion.** Preserve typed identity namespaces, exact
  comparison bytes, and original offsets under ADRs 0029 and 0034. Prove retry
  after response loss, restart, leader change, and target import retry.
- **Retention evolution.** The current all-history policy is simpler than the
  future retention/replay contract. Add retention floors and replay pins to the
  migration inventory before advertising migration for retained history that
  may be deleted during copy.
- **Resource amplification.** Source, recovery copy, three target replicas,
  materialized JSON state, snapshots, journals, and staging can exceed local
  capacity. Measure and enforce reserve checks before accepting a migration.
- **Format evolution.** Local frame schemas and clustered command/snapshot
  schemas evolve independently. Add old/new fixtures and fail closed on
  unknown or contradictory descriptors; serde parsing alone is not a semantic
  compatibility proof.
- **Endpoint authority.** DNS/load-balancer or operator routing is outside the
  current Raft transaction. Define an endpoint owner and status reconciliation
  protocol that prefers safe downtime over dual writers.
- **Target recovery.** Existing empty-replica recovery is experimental and
  static-cluster placement is not production-ready. Verify imported target state
  with the supported cluster recovery path before treating migration as a
  general availability feature.
- **Availability fit.** The accepted full offline fence may be too disruptive
  for some workloads. Measure its full downtime/resource envelope first; any
  live-tail or dual-write alternative requires a separately accepted protocol
  and fault evidence, not an expansion of this implementation.

## Design gate and planning assessment

ADR 0043 accepts the first migration behavior, but no procedure is supported
until the schema/refusal matrix, durable source fence, bounded import, all-voter
validation, endpoint reconciliation, first-write resolution, and real-process
recovery gates pass. The [backlog outcome](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive)
and [TD-007](../tech-debt.md#td-007-storage-conversion-and-artifact-compatibility-remain-open)
remain open for that runtime work. Old software generations and historical
formats not emitted by an eligible current source are explicitly outside the
acceptance matrix. Online migration, dynamic placement, and segmented storage
remain separate outcomes; no additional tech-debt entry is warranted because
these are not current implementation shortcuts.

## References

### Runnel sources

- [Current architecture](../architecture.md)
- [Growth-from-one-node backlog outcome](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive)
- [Safe durable storage upgrades](storage-upgrade-safety-plan.md)
- [Durable storage upgrade policy](storage-upgrade-policy.md)
- [TD-007 storage compatibility evidence](td-007-storage-compatibility-evidence.md)
- [TD-008 static-cluster evidence](td-008-static-cluster-evidence.md)
- [TD-009 snapshot scalability evidence](td-009-snapshot-evidence.md)
- [TD-010 retained-state evidence](td-010-retained-state-evidence.md)
- [TD-022 storage-executor evidence](td-022-storage-executor-evidence.md)
- [Clustered outcome contract](clustered-outcome-contract.md)
- [ADR 0004: Multi-Raft first distributed engine](../decisions/0004-multi-raft-first-distributed-engine.md)
- [ADR 0006: separate metadata and stream data groups](../decisions/0006-separate-metadata-and-data-groups.md)
- [ADR 0007: snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [ADR 0018: safe replica recovery boundary](../decisions/0018-safe-replica-recovery-boundary.md)
- [ADR 0019: clustered storage identity](../decisions/0019-clustered-storage-identity.md)
- [ADR 0023: independent retained storage and placement](../decisions/0023-independent-retained-storage-and-placement.md)
- [ADR 0024: explicit offset replay](../decisions/0024-explicit-offset-replay-read.md)
- [ADR 0026: semantic engine error classification](../decisions/0026-semantic-engine-error-classification.md)
- [ADR 0027: consumer-scoped retry policy](../decisions/0027-consumer-scoped-retry-policy.md)
- [Raft follower recovery and replacement research](../research/raft-recovery-and-replacement.md)
- [Testing and local operation](../testing.md)

### External references

- [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html)
- [Apache Kafka basic operations and partition reassignment](https://kafka.apache.org/36/operations/basic-kafka-operations/)
- [Apache Kafka cross-cluster data mirroring](https://kafka.apache.org/35/operations/geo-replication-cross-cluster-data-mirroring/)
- [Apache Kafka MirrorMaker checkpoint configuration](https://kafka.apache.org/38/configuration/mirrormaker-configs/)
- [Apache Kafka protocol leader epochs](https://kafka.apache.org/37/design/protocol/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [etcd runtime reconfiguration](https://etcd.io/docs/v3.7/op-guide/runtime-configuration/)
- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/)
- [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST)
- [Online, Asynchronous Schema Change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/)
