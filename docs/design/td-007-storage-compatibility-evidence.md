# TD-007: Storage compatibility evidence

- Status: source and test evidence only; runtime conversion remains unimplemented
- Last reviewed: 2026-10-06
- Baseline: `6b53cc0ed3a83017e59d42319ce696f825fb388f`
- Scope: local durable files, clustered durable artifacts, and the boundary
  between same-binary recovery and supported release upgrades
- Related debt: [TD-007](../tech-debt.md#td-007-storage-conversion-and-artifact-compatibility-remain-open)
- Related outcome: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe)
- Related designs: [Durable storage upgrade policy](storage-upgrade-policy.md)
  and [safe durable storage upgrades](storage-upgrade-safety-plan.md)
- Related decisions: [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), and
  [ADR 0019](../decisions/0019-clustered-storage-identity.md), with the
  accepted behavior in [ADR 0037](../decisions/0037-offline-side-by-side-storage-upgrades.md)

This note records what the source and tests at the stated baseline establish.
ADR 0037 separately accepts the first operational upgrade behavior; this note
does not describe a migration command or claim that conversion, rolling
upgrade, downgrade, or new-format runtime support has been implemented.

## Current conclusion

Local and clustered engines have separate persistent artifacts and different
version boundaries:

- The local stream writer emits only checksummed RNL3 version-2 frames, including ordinary records with no request ID. Flags distinguish public IDs, dead-letter move IDs, and no identity; keys, payloads, and identities are bounded. Startup inspects all stream files before repairing incomplete tails. Tests verify that RNL1, RNL2, and RNL3 version 1 fail explicitly without changing their files or truncating another stream, and cover current-format round trips, checksums, bounded lengths, and incomplete-tail recovery. See the [core recovery tests](../../crates/runnel-core/src/lib.rs) and [local frame implementation](../../crates/runnel-core/src/stream_log.rs).
- Local consumer checkpoints and their bounded JSON-lines event journal
  recover progress and delivery attempts on reopen. These artifacts do not
  carry an explicit format version, so those tests establish same-binary
  recovery only.
- Cluster startup validates the known legacy root paths, storage identity,
  grouped layout, manifests, Raft logs, state-machine checkpoints, journals,
  and snapshots before `GroupManager` opens groups. Invalid known state is
  refused in the tested cases. The standalone layout validator has a
  read-only fixture test; this does not make all engine startup read-only.
- Clustered Raft logs use a version-2 marker, bounded entry segments, and a
  bounded control record. Version-1 whole-map artifacts fail without mutation;
  no v1 reader, writer, or conversion is retained. Focused tests cover v2
  reopen, vote/commit recovery, truncation, purge, incomplete-tail repair,
  completed-batch corruption, and explicit v1 refusal. Real-process
  follower/snapshot/purge interaction and the complete storage fault matrix
  remain open. The state-machine journal uses version 2; checkpoints and
  snapshot payloads use version 3. Those readers require the current version
  and every current field, with no pre-retry format read-forward. Tests reject
  prior and future checkpoint, snapshot, and journal versions without mutation.
- Snapshot installation and the test-only empty-replica experiment establish
  recovery mechanisms under their tested conditions. [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md)
  explicitly excludes empty-replica replacement from the production
  compatibility and availability promise.

The phrase “read-only preflight” applies to validation of existing known
clustered artifacts: it rejects unsupported or contradictory input before
groups are opened, and the current-layout fixture verifies that validation
does not rewrite its files or create state-machine directories. Normal startup
creates the data directory, and a new store may write `storage.json`; that
initialization is not a migration. Pre-retry checkpoint, snapshot, and
state-machine journal schemas are rejected; no in-memory read-forward or
startup conversion is provided for those state artifacts.

## Evidence matrix

| Boundary | Current evidence | What remains unproven |
| --- | --- | --- |
| Local stream history | The sole writer emits checksummed RNL3 v2 frames for every record. Recovery validates exact version, identity flag, field bounds, checksums, and contiguous offsets; all stream files are inspected before incomplete-tail repair. Tests cover ordinary and request-ID records, non-mutating refusal of RNL1/RNL2/RNL3 v1, checksum and length failures, and crash-tail recovery. [Core tests](../../crates/runnel-core/src/lib.rs) [Frame implementation](../../crates/runnel-core/src/stream_log.rs) | These tests establish current-format same-binary recovery, not release-pair compatibility. The request-identity map still grows with distinct retained IDs, and per-record limits do not establish an aggregate memory budget. Conversion and local-to-cluster migration are outside this evidence row. |
| Local consumer state | JSON checkpoints persist committed and out-of-order acknowledgement progress, delivery attempts, and policy; a bounded JSON-lines journal replays events and discards an incomplete final line. Tests cover reopen after an acknowledgement journal failure, partial-tail recovery, the journal bound, and rejection of oversized journal data. [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L13) [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L120) [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L228) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1882) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1941) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2155) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2253) | Checkpoint and journal records have no explicit schema version or release-pair writer contract. Delivery tokens and deadlines are volatile. No old-binary/new-state reopen test exists. |
| Cluster storage identity | `storage.json` records metadata version 1, cluster name, and node ID. Existing mismatched, malformed, or unsupported metadata and unmarked grouped state fail before group open in covered cases. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L29) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L48) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L901) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1639) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1859) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2093) | The marker identifies cluster and node ownership; it is not a generation selector, migration record, or downgrade authority. New-store initialization may write it. |
| Clustered layout preflight | Startup rejects recognized old single-group paths. Validation checks the metadata group, data-group directory names, stream/group identity in `group.json`, Raft logs, checkpoints, journals, and snapshots before `GroupManager` opens groups. `group.json` has no explicit schema-version field. Tests cover old root layouts, missing metadata group, unsupported data-group log, contradictory manifest, and an unchanged current-layout fixture. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L32) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L901) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L86) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L671) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L686) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L816) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1797) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1893) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1954) | The fixture verifies the validator on a known current layout. It is not an end-to-end migration from the old layout, proof against arbitrary filesystem contents, or crash-safe activation evidence. The manifest shape is an identity check, not a release compatibility contract. |
| Cluster Raft log and state-machine journal | The Raft log uses a version-2 marker, checksummed segment family, and bounded control record; it rejects v1 without mutation or empty-store fallback. Tests cover current-format reopen and control recovery, truncation, purge, incomplete-tail recovery, completed-batch corruption, and v1 refusal. The separate state-machine journal uses strict version-2 frames, rejects other versions, and discards an incomplete final record. [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs) [`raft_log_segments.rs`](../../crates/runnel-raft/src/raft_log_segments.rs) [`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs) [`lib.rs`](../../crates/runnel-raft/src/lib.rs) | Current-format recovery tests do not establish all real-process follower/snapshot interactions or the complete storage fault matrix. No cross-release OpenRaft log or command compatibility is established; current envelope versions do not promise release stability. |
| Cluster state-machine checkpoint | Current writer emits version 3 and the reader accepts only version 3. Required fields and current stream shapes must be present. Tests reject versions 1 and 2 and a future version without rewriting the checkpoint or creating a journal. [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L94) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L44) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L374) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2298) | No old-format migration or cross-release state compatibility is provided. |
| Cluster snapshot payload and install | Current snapshot payload writer emits version 3 and the reader accepts only version 3 with all required fields and current stream shapes. Tests reject versions 1 and 2 and a future version without changing the snapshot or creating a journal. Installation validates before publishing in-memory state and persists the snapshot/checkpoint before replacing the in-memory image; current-format failure tests retain the prior state. [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L56) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L175) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L863) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2360) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L964) | No old-format migration, resumable transfer, or proof that different binary versions can exchange snapshots is provided. |
| Peer protocol boundary | Peer requests are a set of operation variants carried in bounded, length-prefixed JSON frames; the request enum has no explicit protocol version or handshake. [`network.rs`](../../crates/runnel-raft/src/network.rs#L17) [`framing.rs`](../../crates/runnel-raft/src/network/framing.rs#L9) | No old/new peer interoperability matrix establishes that mixed binaries can exchange Raft entries, commands, forwarded operations, or snapshots safely. |
| Empty-replica snapshot replacement | A real three-process scenario exists behind the explicit `test-replacement-recovery` feature. It erases one node's local state, retries interrupted snapshot transfer, and verifies recovery. ADR 0018 keeps this permissive OpenRaft path test-only. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1242) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1334) [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) | This experiment does not authorize erasing/reusing a voter directory in production and does not establish a storage compatibility or release-upgrade policy. |

## Compatibility classification

| Relation | Supported today? | Evidence boundary |
| --- | --- | --- |
| Current-binary reopen of tested local or clustered artifacts | Yes, within the reader and layout checks described above | Focused same-binary recovery tests; the ordinary three-process cluster recovery workflow covers preserved-state restart, not binary version changes. |
| Local stream frame | Yes for RNL3 version 2; old local frame versions are explicitly refused | Current-format restart and crash-tail tests plus non-mutating refusal tests cover the implemented reader. No cross-release writer/reader guarantee is claimed. |
| Clustered version-1 checkpoint/snapshot read-forward | Observed for specific version-1 fixtures | Current code reads those fields into its current in-memory representation. It does not imply a general schema guarantee or a startup migration. |
| Older binary opening state written by a newer binary | No | No release-pair tests; read-forward support in the current binary proves only the opposite reader direction for listed fixtures. |
| Clustered rolling binary upgrade or mixed-version operation | No | No real old/new binary matrix or peer compatibility handshake is established here. |
| Converted layout/encoding, downgrade, or local-to-cluster migration | No | No source/target generation, conversion boundary, activation selector, reverse conversion, or operator procedure exists. |
| Empty-replica recovery with a production binary | No | The permissive snapshot replacement scenario is explicitly test-only under ADR 0018. |

Report read-forward with the artifact, encoded version, fields or fixture, and
reader tested. “Backward-compatible storage” or “rolling-upgrade compatible”
is too broad for the current evidence.

## Gates before claiming supported compatibility

Before an ADR or a release compatibility promise, a future change must satisfy
the following gates for each affected artifact and supported release pair:

1. **Inventory and identity:** identify every on-disk and peer artifact,
   reader/writer version, engine, cluster/node/group identity, and source
   boundary before opening or selecting state.
2. **Bounded validation:** reject unknown, malformed, contradictory,
   unsupported, or identity-mismatched data before mutation; bound frame,
   record, payload, and decompression allocations; validate checksums and
   offset/applied-boundary continuity.
3. **Logical equality:** compare messages and offsets, consumer progress,
   out-of-order acknowledgements, delivery attempts, leases, and producer
   request identity—not just serialized shape or counts.
4. **Explicit conversion and activation:** when conversion is required, build
   a side-by-side target from a recorded source boundary, retain the source as
   authority through validation, and make target activation durable and
   deterministic.
5. **Mutation fencing:** prevent stale writers, acknowledgements, forwarded
   requests, leaders, and cleanup owners from mutating or deleting state across
   activation.
6. **Interruption and rollback:** test preflight, transfer, validation,
   activation, restart, cleanup, and process/node failure. A verified source
   or target remains authoritative; downgrade requires a tested inverse or
   recovery artifact.
7. **Mixed-version operation:** outside the first behavior accepted by ADR
   0037. Any later decision to permit rolling or mixed-version serving must
   establish a separate compatibility contract and use real old/new binaries
   to test peer, command, snapshot, checkpoint, journal, publish,
   acknowledgement, replay, deduplication, failover, recovery, and response
   loss.
8. **Operational evidence:** expose bounded phase, identity, progress, failure,
   rollback, and orphan/cleanup diagnostics, and measure transfer workspace
   and recovery cost for representative retained streams.

The [storage-upgrade safety plan](storage-upgrade-safety-plan.md) contains the
accepted operational migration contract and its implementation acceptance
matrix. These gates describe evidence needed before runtime conversion is
supported; they do not define a required API or layout.

## Refactor and planning assessment

The local log now has one RNL3 v2 writer and no format selector. The
implementation inspects every stream before repairing an incomplete tail and
refuses obsolete local files without mutation. Aggregate local materialization
remains open under [TD-028](../tech-debt.md#td-028-aggregate-local-record-materialization-lacks-a-memory-budget).
This local-format change does not alter the separate clustered artifact
boundaries, storage-upgrade contract, or local-to-cluster migration work.

## Verification

The local runtime evidence is the focused core recovery test set
referenced above. This note does not imply release-pair tests, a migration
benchmark, or changes to clustered recovery behavior.
