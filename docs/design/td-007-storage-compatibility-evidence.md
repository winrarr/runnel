# TD-007: Storage compatibility evidence

- Status: exploratory evidence note; no migration implementation authorized
- Last reviewed: 2026-09-29
- Baseline: `6b53cc0ed3a83017e59d42319ce696f825fb388f`
- Scope: local durable files, clustered durable artifacts, and the boundary
  between same-binary recovery and supported release upgrades
- Related debt: [TD-007](../tech-debt.md#td-007-storage-format-compatibility-is-not-yet-defined)
- Related outcome: [Make durable storage upgrades safe](../backlog.md#make-durable-storage-upgrades-safe)
- Related designs: [Durable storage upgrade policy](storage-upgrade-policy.md)
  and [safe durable storage upgrades](storage-upgrade-safety-plan.md)
- Related decisions: [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md),
  [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md), and
  [ADR 0019](../decisions/0019-clustered-storage-identity.md)

This note records what the current source and tests establish. A current
binary's ability to read selected older bytes is same-binary recovery evidence;
it does not promise that different releases can read, write, or operate on the
same active store. This note does not authorize a migration command, a rolling
upgrade, a downgrade, or a new storage format.

## Current conclusion

Local and clustered engines have separate persistent artifacts and different
version boundaries:

- The local stream reader dispatches recognized `RNL1`, `RNL2`, and `RNL3`
  frames by magic. `RNL2` is a versioned, checksummed record; `RNL3` carries a
  request identity. The mixed-format test covers `RNL1` followed by `RNL2`;
  request-ID persistence and restart recovery are tested separately. A
  complete record with an invalid legacy key, unsupported magic, checksum
  mismatch in formats that carry one, or offset gap fails recovery; an
  incomplete trailing frame is discarded. The initial `RNL1` writer encoded a
  28-byte header (magic, little-endian `u64` offset and timestamp, and
  independent little-endian `u32` key and payload lengths) followed by UTF-8
  key bytes and opaque payload bytes, with no lower format or application cap.
  Each field could therefore be as large as `u32::MAX` bytes, for a maximum
  encoded record of `28 + 2 × u32::MAX` (8,589,934,618) bytes, subject to
  physical storage and process addressability. This establishes the historical
  encoded envelope, not that near-limit records exist or that such allocations
  are operationally safe.
  The current reader has no smaller `RNL1` allocation limit; see the evidence
  matrix and [TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget).
- Local consumer checkpoints and their bounded JSON-lines event journal
  recover progress and delivery attempts on reopen. These artifacts do not
  carry an explicit format version, so those tests establish same-binary
  recovery only.
- Cluster startup validates the known legacy root paths, storage identity,
  grouped layout, manifests, Raft logs, state-machine checkpoints, journals,
  and snapshots before `GroupManager` opens groups. Invalid known state is
  refused in the tested cases. The standalone layout validator has a
  read-only fixture test; this does not make all engine startup read-only.
- Clustered Raft logs and state-machine journals each use their own strict
  version-1 format. State-machine checkpoints and snapshot payloads are
  written as version 2 and accept a narrow set of version-1 fields through
  defaults and legacy stream shapes. Tests cover specific version-1 fixtures,
  not every historical release or schema combination.
- Snapshot installation and the test-only empty-replica experiment establish
  recovery mechanisms under their tested conditions. [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md)
  explicitly excludes empty-replica replacement from the production
  compatibility and availability promise.

The phrase “read-only preflight” applies to validation of existing known
clustered artifacts: it rejects unsupported or contradictory input before
groups are opened, and the current-layout fixture verifies that validation
does not rewrite its files or create state-machine directories. Normal startup
creates the data directory, and a new store may write `storage.json`; that
initialization is not a migration. Snapshot read-forward likewise means the
current binary can materialize a tested older payload in memory, not that
startup converts the source artifact into a new generation.

## Evidence matrix

| Boundary | Current evidence | What remains unproven |
| --- | --- | --- |
| Local stream history | Reader dispatches `RNL1`, `RNL2`, and `RNL3`; it checks record-length arithmetic and completeness, contiguous offsets, and format-specific fields/checksums. `RNL2` and `RNL3` have explicit key/body limits. Tests cover incomplete tails, one malformed complete legacy key, mixed `RNL1`/`RNL2` history, `RNL2` checksum failure, request-ID restart recovery, and bounded request-ID parsing. [`lib.rs`](../../crates/runnel-core/src/lib.rs#L45) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L13) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L101) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L727) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L751) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L769) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L782) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L556) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L807) [`stream_log.rs`](../../crates/runnel-core/src/stream_log.rs#L913) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1824) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1850) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1888) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1967) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2000) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2046) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2086) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L2148) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L551) [initial writer and reader](https://github.com/winrarr/runnel/blob/b47d9df4bb6b7675c2f3eba9fc8e5e8f1c273ce7/crates/runnel-core/src/lib.rs#L294-L396) [initial TCP input path](https://github.com/winrarr/runnel/blob/b47d9df4bb6b7675c2f3eba9fc8e5e8f1c273ce7/crates/runnel-server/src/main.rs#L217-L245) | The mixed-format test does not mix `RNL3` with the other families. The historical writer accepted any key/body byte lengths that fit `u32` (`u32::MAX` each); this is an encoded ceiling, not proof that near-limit files exist. The initial reader also loaded the whole log into memory. The current reader verifies completeness before allocation, allocates a complete key during recovery, and allocates a complete payload on delivery, with no smaller `RNL1` cap. Tests do not cover valid near-limit `RNL1` data or resource behavior at a policy boundary. Do not apply `RNL2`/`RNL3` limits retroactively without an accepted compatibility path for existing data. There is no root generation marker, cross-release writer/reader matrix, proof an older binary can interpret newer frames, or conversion path. |
| Local consumer state | JSON checkpoints persist committed and out-of-order acknowledgement progress, delivery attempts, and policy; a bounded JSON-lines journal replays events and discards an incomplete final line. Tests cover reopen after an acknowledgement journal failure, partial-tail recovery, the journal bound, and rejection of oversized journal data. [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L13) [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L120) [`consumer_state.rs`](../../crates/runnel-core/src/consumer_state.rs#L228) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1515) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1574) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1624) [`lib.rs`](../../crates/runnel-core/src/lib.rs#L1664) | Checkpoint and journal records have no explicit schema version or release-pair writer contract. Delivery tokens and deadlines are volatile. No old-binary/new-state reopen test exists. |
| Cluster storage identity | `storage.json` records metadata version 1, cluster name, and node ID. Existing mismatched, malformed, or unsupported metadata and unmarked grouped state fail before group open in covered cases. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L29) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L48) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L901) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1639) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1859) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2093) | The marker identifies cluster and node ownership; it is not a generation selector, migration record, or downgrade authority. New-store initialization may write it. |
| Clustered layout preflight | Startup rejects recognized old single-group paths. Validation checks the metadata group, data-group directory names, stream/group identity in `group.json`, Raft logs, checkpoints, journals, and snapshots before `GroupManager` opens groups. `group.json` has no explicit schema-version field. Tests cover old root layouts, missing metadata group, unsupported data-group log, contradictory manifest, and an unchanged current-layout fixture. [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L32) [`engine.rs`](../../crates/runnel-raft/src/engine.rs#L901) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L86) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L671) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L686) [`group_manager.rs`](../../crates/runnel-raft/src/group_manager.rs#L816) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1797) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1893) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L1954) | The fixture verifies the validator on a known current layout. It is not an end-to-end migration from the old layout, proof against arbitrary filesystem contents, or crash-safe activation evidence. The manifest shape is an identity check, not a release compatibility contract. |
| Cluster Raft log and state-machine journal | The Raft log (`raft-log.json`) and length-framed state-machine journal each validate their own version-1 records and reject unsupported versions. The journal discards an incomplete final record, but does not read forward unknown versions. [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L18) [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L97) [`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs#L13) [`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs#L57) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2034) [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs#L584) | No cross-release OpenRaft log or command compatibility is established by these parser checks. Version 1 is not evidence that the format is stable across releases. |
| Cluster state-machine checkpoint | Current writer emits version 2. The reader accepts versions 1 and 2; optional fields default, and legacy stream arrays are materialized with derived current stream identity. Tests cover a specific version-1 checkpoint with messages and consumer progress, plus a version-1 checkpoint whose omitted lease clock defaults and then survives journal replay/reopen. [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L86) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L34) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L310) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L460) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2324) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L1119) | Tests do not prove all version-1 field combinations, every historical producer, cross-release writes, or that a previous binary can safely open a checkpoint after current-version state has been written. |
| Cluster snapshot payload and install | Current snapshot payload writer emits version 2; the reader accepts version 1 or 2 and defaults omitted fields. A version-1 snapshot fixture reopens without the fixture bytes changing. Installation validates before publishing in-memory state and persists the snapshot/checkpoint before replacing the in-memory image; failure tests retain the prior state. [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L192) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L881) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L810) [`lib.rs`](../../crates/runnel-raft/src/lib.rs#L2368) [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs#L964) | Read-forward and install ordering are not a general migration, a resumable transfer, or proof that old and new snapshot writers/readers can coexist. |
| Peer protocol boundary | Peer requests are a set of operation variants carried in bounded, length-prefixed JSON frames; the request enum has no explicit protocol version or handshake. [`network.rs`](../../crates/runnel-raft/src/network.rs#L17) [`framing.rs`](../../crates/runnel-raft/src/network/framing.rs#L9) | No old/new peer interoperability matrix establishes that mixed binaries can exchange Raft entries, commands, forwarded operations, or snapshots safely. |
| Empty-replica snapshot replacement | A real three-process scenario exists behind the explicit `test-replacement-recovery` feature. It erases one node's local state, retries interrupted snapshot transfer, and verifies recovery. ADR 0018 keeps this permissive OpenRaft path test-only. [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1242) [`cluster_smoke.rs`](../../crates/runnel-server/tests/cluster_smoke.rs#L1334) [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md) | This experiment does not authorize erasing/reusing a voter directory in production and does not establish a storage compatibility or release-upgrade policy. |

## Compatibility classification

| Relation | Supported today? | Evidence boundary |
| --- | --- | --- |
| Current-binary reopen of tested local or clustered artifacts | Yes, within the reader and layout checks described above | Focused same-binary recovery tests; the ordinary three-process cluster recovery workflow covers preserved-state restart, not binary version changes. |
| Local recognized-frame read | Yes for `RNL1`, `RNL2`, and `RNL3` records recognized by this reader | The test suite separately covers mixed `RNL1`/`RNL2` records and `RNL3` request-ID recovery; it does not test a cross-release writer pair or every possible mixture. |
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
7. **Mixed-version operation:** use real old/new binaries to exercise peer,
   command, snapshot, checkpoint, journal, publish, acknowledgement, replay,
   deduplication, failover, recovery, and response-loss behavior.
8. **Operational evidence:** expose bounded phase, identity, progress, failure,
   rollback, and orphan/cleanup diagnostics, and measure transfer workspace
   and recovery cost for representative retained streams.

The [storage-upgrade safety plan](storage-upgrade-safety-plan.md) contains the
fuller proposed migration state machine and acceptance matrix. These gates are
a classification aid for future work; they do not accept the proposed API or
layout.

## Refactor and planning assessment

No runtime refactor is included in this evidence-only review. Adding generation
selection, migration ownership, or shared format abstractions before a policy
is accepted would add runtime surface without retiring TD-007. The RNL1
allocation-bound gap is recorded as a specific resource-policy debt in
[TD-028](../tech-debt.md#td-028-rnl1-materialization-lacks-an-operational-allocation-budget).
That item does not set a new cap or authorize a migration. Existing TD-007 debt
and storage-upgrade backlog records remain open. The adjacent `DurableFormat`
source comment now names the request-aware RNL3 reader path, matching the parser
and its restart-recovery test.

## Verification

This update changes documentation only. Runtime tests and benchmarks do not
apply. `git diff --check` and the local Markdown link/line-anchor check cover
the change. The existing focused recovery tests and real-process clustered
test remain the runtime evidence referenced above; no migration or
rolling-upgrade benchmark is implied.
