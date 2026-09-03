# Single-node to clustered migration boundary

- Status: exploratory design note; not an accepted compatibility decision
- Last reviewed: 2026-09-03
- Baseline: `origin/main` `dfcdfc74a2819bef3992f0b6b5be9b8095eff907`
- Scope: backlog outcome [Make growth from one node to a cluster non-disruptive](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive) and [TD-004](../tech-debt.md#td-004-local-and-clustered-durable-state-have-no-supported-migration-path)

## Summary

The first supportable local-to-cluster migration should be a side-by-side,
logical export/import into a fresh three-node clustered deployment. It should
preserve logical stream offsets, record bytes, publish timestamps, ordering
keys, producer request identities, consumer progress, and durable delivery
attempts. It should use a short, explicit source fence for the final consistent
boundary, then activate the target through a durable generation record and an
external endpoint change.

This is non-disruptive to the application model and to acknowledged state. It
is not a promise of zero downtime in the first slice: the initial supported
procedure has a bounded maintenance window while the source is fenced and the
target is verified. A live copy with a replicated tail or dual writes is a
separate design and remains outside the first supportable boundary until its
ordering, acknowledgement, and fencing proof exists.

The migration is an engine boundary, not a durable-format upgrade. Local log
files must be read through the local compatibility reader and converted into
cluster data-group state. Copying a local file into a clustered directory,
republishing through the public API, or installing local files as an OpenRaft
snapshot is not a supported migration.

## Current evidence and the boundary it creates

The current implementation provides two compatible messaging contracts, but
not two compatible durable representations. The public boundary is deliberately
topology-free: [`Engine`](../../crates/runnel-engine/src/lib.rs) exposes streams,
publishes, polls, replay, acknowledgements, and health, while engine selection
is made when the process starts. [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md)
explicitly defers mixed engines and live engine migration.

### Local state

`runnel-core` currently owns the following state:

| State | Current representation | Migration consequence |
| --- | --- | --- |
| Stream history | `streams/<stream>.log`, with legacy `RNL1`, versioned `RNL2`, and request-aware `RNL3` record families. Each frame carries a logical offset, publish timestamp, optional UTF-8 key, payload lengths, and, for `RNL3`, a request ID. | Read records in logical offset order and write an explicitly versioned import representation. Preserve fields and bytes, not the local frame layout or file name. A mixed valid frame history is a source format case, not a target cluster format. |
| Recovery/index state | The local log scans complete frames on open, truncates only an incomplete trailing frame, retains a bounded recent index, and uses a bounded sparse index for older reads. | Export only after normal recovery has established a complete source boundary. A malformed complete frame is a validation failure; it must not be skipped or turned into a gap. |
| Producer retry identity | The local `request_ids` map is rebuilt from request-aware frames. A repeated request ID returns its first recovered offset and, for public publishes, preserves the current behavior of ignoring a key/payload mismatch. | Export every recovered request ID and its original offset. The importer must reject an offset mismatch or duplicate conflicting mapping, while preserving the current public retry result. Records without a request ID remain non-deduplicated. |
| Ordinary and grouped consumer state | `consumers/<stream>/<consumer>.json` contains `committed_offset`, out-of-order `acknowledged_offsets`, and `delivery_attempts`. The adjacent `.json.tmp` path is an append-only event journal with a bounded size and atomic checkpoint compaction. | Convert the logical state into the clustered consumer-state schema. Do not copy the JSON file as if it were a clustered snapshot. Validate every offset against the imported stream and preserve attempts. |
| Active deliveries | Local in-flight ownership, deadlines, and delivery tokens are process memory. Attempts are persisted before a delivery is returned, but local tokens do not survive restart. | Do not transfer local tokens, members, or `Instant` deadlines. At the fence, outstanding deliveries become eligible redeliveries on the target; an acknowledgement that races after the fence is rejected and must be retried against the target. |
| Stream names and paths | Names are restricted to 1–128 ASCII letters, digits, `.`, `_`, and `-`; names are later used below the local `streams` and `consumers` directories. | Validate names before export and again before import. A migration tool must never accept an arbitrary source path or infer a name from an unsafe filename. |

The local durable log retains all history in the current slice. Its bounded
in-memory indexes do not mean that old records are unavailable: old replay and
delivery lookups can scan from a sparse checkpoint. This distinction matters
when estimating migration work and memory.

### Clustered state

`runnel-raft` uses a metadata Raft group and one data group per stream. The
metadata group records stream identity and the `Creating` to `Active` lifecycle;
the data group contains the stream records and consumer state. The first
clustered topology statically replicates every group to the configured three
voters. [ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md)
and [ADR 0007](../decisions/0007-snapshot-based-replica-recovery.md) make those
group and snapshot boundaries explicit.

The clustered state machine currently materializes complete retained messages
and stores versioned checkpoints, snapshots, and an incremental journal. Its
state includes:

- stream metadata, lifecycle, and `StoredMessage` values, with offsets implied
  by their position in the stream vector;
- ordinary consumer offsets;
- grouped consumer `committed_offset`, out-of-order acknowledged offsets,
  delivery attempts, in-flight member/token/deadline state, and the replicated
  lease-clock floor;
- per-stream request-ID-to-offset deduplication; and
- redelivery and dead-letter counters.

The counters are operational observations rather than message semantics. The
local counters are process-lifetime values and are not recoverable source
state; the migration may reset them while reporting the reset in diagnostics.
Existing dead-letter streams and their records are ordinary streams and must be
copied. Historical duplicate dead-letter records must not be silently merged.

Cluster startup validates `storage.json`, group directories, data-group
manifests, Raft logs, state-machine checkpoints, snapshots, and journals before
opening groups. It refuses legacy single-group layouts, unmarked clustered
state, partial layouts, and cluster/node identity mismatches. Those checks are
important safety boundaries, but they are not a local-to-cluster converter:
[ADR 0019](../decisions/0019-clustered-storage-identity.md) says that storage
identity must not be guessed, and [ADR 0018](../decisions/0018-safe-replica-recovery-boundary.md)
keeps empty-replica recovery test-only.

### Public outcomes during and after migration

The provisional client makes transport failures after a request may have been
written `Unknown`. A publish with a stable request ID can be retried explicitly
and resolve to the original offset; a publish without one remains ambiguous.
The server also treats request timeouts conservatively because engine work may
already have committed. The migration cannot turn an unknown pre-fence publish
without a request ID into a confirmed or deduplicated result.

After activation, the same public protocol and engine contract remain in use.
Clients reconnect to the target endpoint; they do not learn Raft groups,
stream placement, storage paths, or node identities.

## Goals and non-goals

The proposed supported boundary has these goals:

- move a complete supported local deployment, including all streams and
  consumer state, to a fresh supported static cluster;
- preserve logical offsets, record ordering, timestamps, keys, exact payload
  bytes, replay eligibility, request-ID retry identity, acknowledged progress,
  and persisted delivery attempts;
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

“Supported” here means a documented, versioned workflow with recovery tests. It
does not mean that every historical local format, arbitrary target version, or
future distributed engine can be migrated automatically.

## Proposed authority model

Treat the source and target as immutable logical generations during migration.
The migration record is bound to:

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

The record must be durable before the source is fenced. Its exact storage path
and administration API are implementation choices, not public protocol fields.
The record should use an explicit generation pointer or activation marker;
startup must never select a generation by directory order or by whichever
partial file happens to parse. RocksDB’s [`CURRENT` and `MANIFEST` design](https://github.com/facebook/rocksdb/wiki/MANIFEST)
is a useful reference for this recovery principle, although Runnel needs a
broker-level migration record rather than RocksDB’s version-edit format.

The proposed phases are:

`planned → preflighted → fenced → copying → validating → ready → activated → complete`

with durable `failed` and `aborted` outcomes. The phase rules are:

| Phase | Authoritative deployment | Allowed actions | Restart result |
| --- | --- | --- | --- |
| `planned` / `preflighted` | Source | Normal application traffic; target is empty or staging-only. | Source starts normally. A failed preflight does not mutate the source. |
| `fenced` | None for mutating traffic | No source publishes, creates, polls, or acknowledgements. Target may receive bounded import work but is not ready for clients. | Resume or abort the same migration after validating the source fence. Do not start a second writer. |
| `copying` / `validating` | None for mutating traffic | Idempotent chunk transfer and read-only validation. | Resume from the last durable chunk boundary or discard unreferenced target staging. |
| `ready` | Source remains the last serving generation, but remains fenced if cutover has started | Target may be checked through an internal migration/read-only path. No public target traffic. | Keep target staged and source fenced; resolve by activating target or explicitly aborting. |
| `activated` | Target | Public traffic only through the target endpoint. Source is permanently fenced for this migration. | Target must recover as the authority. It must not silently fall back to source or appear empty. |
| `complete` | Target | Cleanup of old artifacts after the retention/rollback policy permits it. | Target remains authoritative; cleanup can resume independently. |

The source fence and target activation are intentionally ordered to avoid
split-brain: fence the source, validate and durably activate the target, then
switch the application endpoint. A coordinator crash between those actions may
cause downtime, but it must not leave two writable deployments. If endpoint
state is ambiguous, both deployments remain not-ready until the durable
migration record and endpoint owner are reconciled.

## Supported input and target contract

The first implementation should accept only a narrow, explicit matrix.

### Source

- A cleanly recoverable current local store opened by the source-compatible
  `runnel-core` reader. This includes valid local histories containing the
  supported `RNL1`, `RNL2`, and `RNL3` record families in the combinations the
  current reader accepts.
- Valid stream and consumer names, complete logical offsets, and consumer
  states whose offsets and attempt entries can be checked against their stream.
- A source process that has a durable migration record and can acquire the
  writer fence. For the first operational release, the final copy starts only
  after the source broker is stopped or placed in an equivalent fenced mode.
- A source configuration whose delivery and retention behavior is either equal
  to the target or explicitly covered by a compatibility rule. The first
  implementation should require equal `ack_timeout` and
  `max_delivery_attempts` values.

### Target

- A freshly initialized current clustered deployment with a new, explicit
  cluster identity and the configured static three-voter membership.
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

Preflight should be read-only until the migration record is durably created.
It should:

1. enumerate streams and consumer files through validated logical names;
2. recover and scan each stream through the local reader, checking offset
   continuity, frame checksums where applicable, key UTF-8 validity, payload
   lengths, timestamp fields, and request-ID mappings;
3. load each consumer checkpoint and journal, replaying only complete events and
   checking committed, out-of-order acknowledged, and attempt offsets against
   `[earliest, next)`;
4. record source configuration and compatibility descriptors, including the
   acknowledgement timeout, attempt limit, retention/replay policy, protocol
   version, local record-format families, and migration tool version;
5. estimate source backup, target-per-node, target journal/snapshot, staging,
   temporary, and transfer-buffer requirements; and
6. write a source and target inventory with counts and content digests before
   asking an operator to begin the fence.

The fence must be stronger than a process convention. The required runtime
work is:

- acquire exclusive migration ownership and persist a monotonically increasing
  writer epoch before accepting the `fenced` phase;
- stop new stream creation, publish, poll, and acknowledgement work at a
  defined operation boundary;
- let operations already past that boundary finish and include their durable
  effects in the export, or reject them clearly; and
- cause a stale broker process, stale migration owner, or delayed client path
  to receive a fencing/retryable result instead of appending or acknowledging.

The current local engine has per-stream operation lanes but no persisted
migration epoch, so this is a prerequisite for calling the procedure supported.
Stopping the process is a useful first implementation mechanism only when the
durable migration marker makes a later unfenced restart refuse service or
explicitly aborts the migration after validation.

Local in-flight deliveries need a deliberate barrier. The simplest first
contract is to stop new polls, allow acknowledgements that entered before the
fence to complete, then capture consumer state and invalidate all remaining
local delivery tokens. Remaining attempts are preserved and redelivered on the
target. A late source acknowledgement is not copied opportunistically: it is
rejected by the fence, and the application retries after target activation.
This can produce a duplicate delivery, which is allowed by at-least-once
semantics; it must not move acknowledged progress backward.

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
deliverable. Its attempt count is not reset, so a configured attempt limit can
still dead-letter it according to the target policy. A target delivery token is
new and must be acknowledged only with the target response. The migration must
not make a source token valid on the target.

Existing `.dead-letter` streams are copied with their own records and consumer
state. A local dead-letter move may have been at-least-once across two files,
whereas a new clustered dead-letter move is one replicated transition. The
import must preserve the observed local history, including any already durable
duplicates, and only apply clustered atomicity to moves that happen after
activation.

### Request-ID deduplication

The source export must derive `request_id → offset` from the recovered
request-aware frames, using the same first-mapping behavior as local recovery.
The target import must populate the target data group’s per-stream dedup map
before public traffic is enabled. For a pre-fence publish that committed but
whose response was lost, retrying the same request ID after cutover must return
the imported original offset without appending a second record. This must work
through any target node and after target restart or leader change.

For a pre-fence publish without a request ID, neither engine can safely infer
whether an unknown response corresponded to a committed record. The migration
must report that limitation; it must not invent an ID or silently remove a
possible duplicate. A publish that was definitely rejected before the fence can
be retried on the target. An acknowledgement whose durable outcome is unknown
is safe to retry after cutover because the target either contains the imported
progress or redelivers the unacknowledged record.

The current public behavior returns the original offset for an existing request
ID even if a retry supplies different key or payload data. The first migration
must preserve that behavior and document it as a compatibility constraint. A
future conflict-detecting request-ID policy would require a separate protocol
and migration decision.

## Target construction and cutover

The target cluster should be created as a staging generation, not as a normal
serving cluster with empty streams. A proposed sequence is:

1. initialize target `storage.json` with a new cluster identity and validate all
   three node identities and addresses;
2. create metadata records in `Creating` state and prepare one data group per
   source stream, preserving deterministic stream identity while assigning the
   new target group identity;
3. import stream chunks and consumer state into data groups through the
   migration protocol, with each committed chunk carrying migration ID, stream
   identity, ordinal, expected next offset, and digest;
4. make every target replica validate the imported digest and local durable
   state. A node that has not imported or recovered the target state cannot be
   ready or participate as an unverified authority;
5. commit one target metadata activation record containing the target
   generation, all stream digests, compatibility descriptors, and the writer
   activation epoch; and
6. only after that record is durable, switch the application endpoint to the
   target cluster and verify readiness, health, publish retry resolution,
   replay, poll, and acknowledgement through the public protocol.

The metadata activation record is the target cluster’s authority for whether
the staged generation may serve. Because the current implementation has
independent data groups and no cross-group transaction, activation must include
an all-stream readiness check. If any required stream is missing or not
validated, target readiness is false and no stream is served as an accidental
empty stream. A future per-stream migration could weaken the all-stream fence,
but it would need a separate application-visible partial-cutover contract.

An endpoint switch is external state and cannot be made atomic with a Raft
commit by the current system. The safe ordering is therefore conservative:

1. source fence is durable;
2. target activation is durable and target readiness is true;
3. the endpoint owner records the target generation and switches traffic; and
4. a post-cutover probe confirms the target generation and writer epoch.

If the coordinator stops between these steps, a migration-status operation must
use the durable records to complete or abort the transition. It must not start
both brokers and infer authority from reachability.

## Rollback boundary

Rollback is a generation transition, not a promise that an old directory can
always be restarted. The supported first-slice boundary is:

- before target activation, the source remains the recoverable authority. A
  failed or interrupted target can be resumed from a verified checkpoint or
  discarded as unreferenced staging. An explicit abort releases the source
  fence only after the source generation and migration record validate again;
- once target activation is committed, the target is authoritative and the
  source is permanently fenced for that migration. The source copy remains a
  recovery artifact, not a second writer;
- after target activation, restoring service from the source would hide target
  publishes, acknowledgements, delivery attempts, and consumer progress. It is
  therefore not an automatic or supported pointer rollback. Recovery uses the
  target’s clustered snapshots/restart path or a separately designed reverse
  migration; and
- cleanup of the source is delayed until the documented recovery/retention
  window, backup verification, and target burn-in policy permit it. Failure to
  clean up is an observable space leak, not permission to delete the only
  known recovery copy.

This intentionally offers a strong pre-activation rollback point and a clear
post-activation no-downgrade boundary. It is safer than claiming that a
side-by-side copy remains reversible after target state has changed.

## Interruption, retry, and ambiguous outcomes

Each phase must have a deterministic restart rule:

| Interruption | Required result |
| --- | --- |
| Before the source fence | Source remains writable and authoritative. An incomplete plan can be abandoned without changing logical state. |
| During fence/drain | Startup finds the durable epoch and either completes the fence or fails closed. It must not accept a publish while the boundary is unresolved. |
| During stream copy | The source generation is unchanged. Target resumes from the last complete chunk and revalidates its digest, or staging is discarded. A partial chunk is never served. |
| During consumer-state import | The last durable state record is authoritative. Reapplying the same state digest is idempotent; a different digest for the same migration/consumer fails. |
| During target validation | Target remains not-ready. Source can be restored only through explicit abort and revalidation. |
| After target activation commit but before endpoint switch | Source stays fenced. Target activation is authoritative; the operator completes endpoint cutover or declares a target recovery failure, never silently restarts source. |
| During endpoint switch or status reporting | Readiness is conservative until endpoint owner and durable target activation agree. Unknown route state is an operational incident, not permission for two writers. |
| After target activation and new traffic | Target recovery or a new migration is required. Source pointer rollback is forbidden. |

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
attempt, or change ordinary consumer progress. The initial migration must
preserve the exact `[earliest, next)` range and return the same record for every
available offset. An absent offset remains an explicit `history_unavailable`
outcome; it must not become ordinary `empty`.

The migration must not use replay as a substitute for a bulk export. Replay is
an application read, does not carry local request-ID metadata, and is bounded to
one logical record. Import must read the source representation with a
source-aware adapter.

When retention is implemented, migration is supportable only if the policy
defines which history is entitled to move. The fence must freeze the source
retention floor for the inventory, and the target must preserve the floor,
`next` offset, replay-unavailable behavior, and any consumer/replay pins. A
source whose history is already below the target’s promised replay scope must
fail preflight rather than claim a complete migration. Replay sessions,
time-based selectors, retention cleanup, and progress replacement remain
unsupported in this design; [ADR 0024](../decisions/0024-explicit-offset-replay-read.md)
defines the intentionally smaller current replay contract.

## Compatibility policy

This is a logical migration between engine generations, with a deliberately
small compatibility matrix:

| Dimension | Supported first slice | Refused or deferred |
| --- | --- | --- |
| Public protocol | Existing provisional JSON-lines requests/responses and client outcome classes remain valid after reconnect. | Protocol redesign, transparent automatic client reconnection, and new topology fields. |
| Local record encoding | Valid source histories read by the current local reader, including supported mixed `RNL1`/`RNL2`/`RNL3` frames. | Unknown versions, malformed complete frames, unbounded lengths, or guessed format conversion. |
| Cluster representation | Current target metadata/data-group, checkpoint, snapshot, journal, and manifest versions. | Import into an older target, unknown target schema, or arbitrary OpenRaft on-disk layout. |
| Consumer semantics | Local committed and out-of-order acknowledged progress plus delivery attempts convert into coherent clustered state. Outstanding local tokens become redelivery. | Transferring local volatile leases/tokens or changing retry/ack semantics during migration. |
| Producer identity | All recovered source request IDs map to the same logical offsets after import. | Deduplicating requests that had no ID, inventing IDs, or silently changing key/payload conflict behavior. |
| Configuration | Equal acknowledgement timeout, attempt limit, and current retention/replay policy. | An unreviewed policy change whose effects could alter redelivery, dead letters, or replay eligibility. |
| Identity | New target cluster identity; deterministic target stream identity and validated data-group manifests. | Copying local state into a target `storage.json`, reusing a different cluster/node identity, or guessing ownership. |
| Downgrade | Abort and source recovery before activation. | Automatic post-activation downgrade, old-binary startup against target-only state, or source pointer rollback after target writes. |

The existing [safe storage-upgrade design](storage-upgrade-safety-plan.md)
defines related version, generation, writer-epoch, and fail-closed vocabulary.
This note narrows that vocabulary to the cross-engine local-to-cluster case;
it must not be read as accepting the broader storage-upgrade proposal or an
automatic upgrade/downgrade policy.

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

## Resource bounds and operational budget

Migration is inherently proportional to the retained state being moved, so
“bounded” means bounded concurrent work, memory, queueing, and temporary space
relative to an explicit inventory—not constant total work independent of data
size.

The first implementation should enforce these bounds:

- one migration per source deployment and one active import per stream unless
  measured resource isolation justifies more;
- a fixed maximum chunk byte/count budget below the current 64 MiB protocol and
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
until that cost is measured. The eventual segmented-storage work in TD-002,
TD-009, and TD-010 may change the efficient import representation without
changing this logical contract.

## Reference designs and research

These sources inform the boundary; none establishes Runnel compatibility.

| Reference | Relevant mechanism | Difference that matters to Runnel |
| --- | --- | --- |
| [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html) | Runs compatibility checks before mutation, initializes a separate destination, keeps the old cluster usable for ordinary copy/clone paths, and documents that link/swap choices can remove the old-cluster rollback property. | This is the closest operational model for a side-by-side generation. Runnel should retain the source and validate before activation, but must logically translate records and consumer state because local files are not clustered state. It should not adopt link-mode semantics that let two engines share mutable files. |
| [Apache Kafka partition reassignment](https://kafka.apache.org/36/operations/basic-kafka-operations/) | Moves partitions to new brokers through the same replicated log protocol, offers execute/verify phases, preserves the current assignment for rollback, and supports a replication-bandwidth throttle. | Kafka expands one cluster whose source and target replicas already speak one log protocol. Runnel’s local engine has no Raft membership, committed log identity, or clustered consumer/dedup schema, so a Kafka-like live replica reassignment cannot be applied to local files. Its explicit plan, verify, rollback artifact, and throttle remain useful. |
| [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/) and [runtime reconfiguration](https://etcd.io/docs/v3.6/op-guide/runtime-configuration/) | A new member receives state as a non-voting learner, cannot serve normal client traffic, and is promoted only after it catches up and passes safety checks. Learner count and replication load are bounded. | Runnel should apply the readiness-before-authority principle to each target replica. A local source is not an etcd/Raft member, so it cannot simply be added as a learner; logical import must first create target data-group state, after which normal controlled replica recovery can apply. |
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

## Staged implementation plan

The design should become code in independently verifiable stages:

1. **Schema and preflight.** Define a versioned logical export/import schema,
   source/target identity tuple, digest rules, compatibility matrix, and
   read-only source scanner. Add fixtures for mixed local frame families,
   request IDs, out-of-order acknowledgements, attempts, malformed state,
   incomplete tails, invalid names, and unavailable history.
2. **Fresh-target logical import.** Add an internal target data-group import
   protocol with bounded, idempotent chunks, stream/consumer digests, explicit
   offsets, request-ID mappings, and target-side validation. Keep imported
   groups non-serving until complete. Add restart/resume tests without public
   cutover.
3. **Durable fence and activation.** Add migration ownership, writer epochs,
   source drain behavior, explicit phase transitions, target metadata
   activation, conservative readiness, endpoint-generation reporting, and
   pre-activation abort. Add crash injection at every durable boundary.
4. **Real-process migration.** Exercise a real local broker and three real
   clustered broker processes with the public protocol before and after the
   migration tool. Cover follower forwarding, leader change, target restart,
   target replica recovery, source restart attempts, stale writer attempts,
   endpoint cutover, and cleanup.
5. **Operational hardening.** Add bounded metrics, status output, disk/memory
   reserve checks, throttling, cancellation, orphan cleanup, backup/recovery
   documentation, and compatibility fixtures for future format versions.
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

Start a real local source process and use the public client/CLI to create
multiple streams, including a dead-letter stream, then publish:

- empty and non-empty keys;
- binary and UTF-8 payloads;
- records with and without stable request IDs;
- request-aware records interleaved with legacy records; and
- enough history to cross the local bounded tail index.

Create independent consumers and a shared consumer. Acknowledge records in and
out of order, leave one delivery in flight, expire one delivery, and persist
multiple delivery attempts. Record source health, replay results, unavailable
offset ranges, consumer state, request-ID mappings, and per-stream digests.

### Fault and cutover matrix

For each phase and at least one representative stream, stop or interrupt:

- the exporter during a chunk and at the durable progress record;
- a target data-group process during import, validation, and activation;
- a target leader before and after a committed import or activation entry;
- the source process during fence drain and while a client has a pending
  publish or acknowledgement; and
- the endpoint coordinator between target activation and route switch.

After every interruption, verify the phase-specific authority rule, process
health, target readiness, source immutability, no partial stream visibility,
offset continuity, payload/key/timestamp equality, consumer progress, attempt
counts, and digest equality. A partial target must never appear as an empty
cluster.

After successful cutover, use the public protocol through follower and leader
addresses to verify:

- a pre-fence request ID with a dropped response resolves to its original
  offset and appends no duplicate;
- a no-ID ambiguous publish remains explicitly unknown and is not silently
  retried by the migration;
- acknowledged offsets remain acknowledged and unacknowledged offsets are
  redelivered at the imported attempt count;
- old source tokens are rejected while target tokens acknowledge only target
  deliveries;
- keyed delivery preserves per-key ordering and unrelated keys continue to
  make progress under the target contract;
- replay returns the same pre-cutover records, leaves ordinary progress alone,
  and reports the same unavailable range; and
- target restart, follower restart, leader failure, and the documented target
  replica-recovery path preserve all imported state.

Test pre-activation abort and source recovery separately from post-activation
failure. The latter must recover target state or fail closed; it must not pass by
starting the source with a stale configuration. Include a validation-failure
case for every bundle field that can create an offset, identity, checksum,
consumer, or request-ID mismatch.

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

The benchmark must distinguish the proportional cost of moving retained data
from the short final fence and cutover window. It must not be used to claim that
the current materialized clustered state scales to arbitrary retained history;
that remains an open storage design question.

## Unresolved risks and evidence required before an ADR

- **Fence linearization.** The current local engine serializes per-stream
  operations but has no migration epoch. Implement and fault-test the exact
  point at which a publish, acknowledgement, poll attempt, or dead-letter move
  is included or rejected.
- **Cross-group activation.** Metadata and stream data groups cannot currently
  commit one cross-group transaction. Prove the all-stream readiness/activation
  gate under target process loss and avoid serving any missing stream as empty.
- **In-flight consumer state.** Local tokens and deadlines are volatile while
  clustered delivery uses replicated tokens and absolute lease timestamps.
  Verify redelivery, attempt limits, stale acknowledgements, and keyed
  ordering across the fence.
- **Request-ID semantics.** Preserve source first-mapping behavior and test
  retry after response loss, restart, leader change, and target import retry.
  Decide later whether mismatched key/payload retries should remain accepted.
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
- **Online availability.** If the bounded fence is too disruptive for a real
  workload, compare a live-tail protocol and dual writes with failure tests
  before expanding the scope. Do not infer safety from a successful happy-path
  copy.

## Design gate and planning assessment

This note is exploratory and does not accept a durable compatibility promise or
an ADR. Implementation should satisfy the design/research evidence class with
source-backed compatibility analysis, then satisfy the contract/migration gate
with schema fixtures, interruption/restart tests, real-process cutover tests,
and explicit rollback outcomes before an ADR or backlog retirement is proposed.

The note does not edit the backlog, technical-debt register, ADRs, protocol, or
runtime code. No additional tech-debt item is warranted: the work is directly
scoped to TD-004, while online migration, dynamic placement, and segmented
storage remain named unresolved boundaries rather than new implementation
shortcuts.

## References

### Runnel sources

- [Current architecture](../architecture.md)
- [Growth-from-one-node backlog outcome](../backlog.md#make-growth-from-one-node-to-a-cluster-non-disruptive)
- [TD-004](../tech-debt.md#td-004-local-and-clustered-durable-state-have-no-supported-migration-path)
- [Safe durable storage upgrades](storage-upgrade-safety-plan.md)
- [ADR 0004: Multi-Raft first distributed engine](../decisions/0004-multi-raft-first-distributed-engine.md)
- [ADR 0006: separate metadata and stream data groups](../decisions/0006-separate-metadata-and-data-groups.md)
- [ADR 0007: snapshot-based replica recovery](../decisions/0007-snapshot-based-replica-recovery.md)
- [ADR 0018: safe replica recovery boundary](../decisions/0018-safe-replica-recovery-boundary.md)
- [ADR 0019: clustered storage identity](../decisions/0019-clustered-storage-identity.md)
- [ADR 0024: explicit offset replay](../decisions/0024-explicit-offset-replay-read.md)
- [Raft follower recovery and replacement research](../research/raft-recovery-and-replacement.md)
- [Testing and local operation](../testing.md)

### External references

- [PostgreSQL `pg_upgrade`](https://www.postgresql.org/docs/current/pgupgrade.html)
- [Apache Kafka basic operations and partition reassignment](https://kafka.apache.org/36/operations/basic-kafka-operations/)
- [etcd learner design](https://etcd.io/docs/v3.6/learning/design-learner/)
- [etcd runtime reconfiguration](https://etcd.io/docs/v3.6/op-guide/runtime-configuration/)
- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [OpenRaft snapshot replication](https://docs.rs/openraft/latest/openraft/docs/protocol/replication/snapshot_replication/)
- [RocksDB MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST)
- [Online, Asynchronous Schema Change in F1](https://research.google/pubs/online-asynchronous-schema-change-in-f1/)
