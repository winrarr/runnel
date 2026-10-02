# Dead-letter recovery across durable boundaries

- Status: exploratory design note; local reconciliation and both conflicting-content and same-content public-ID wire outcomes are tested, while physical durability, the accepted ID identity contract, and provenance evidence remain open
- Last reviewed: 2026-10-02
- Baseline: `906199f88e2b275750a270e33c9e4d213f050740`
- Reading guide: [design-note conventions](README.md)
- Related debt: [TD-017](../tech-debt.md#td-017-dead-letter-movement-spans-separate-durable-records), [TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse), and [TD-029](../tech-debt.md#td-029-public-request-ids-can-collide-with-local-dead-letter-move-ids)
- Related decisions: [ADR 0014](../decisions/0014-local-retry-and-dead-letter-policy.md), [ADR 0016](../decisions/0016-clustered-retry-and-dead-letter-policy.md), [ADR 0026](../decisions/0026-semantic-engine-error-classification.md), and [ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)
- Related boundaries: [durability and delivery policy](durability-delivery-policy.md), [application-aware retry policy](application-aware-retry-policy.md), and [clustered outcomes](clustered-outcome-contract.md)

This note separates the observed local append/reconcile behavior from the
clustered same-group transition and from future cross-group choices. It is a
design input, not an acceptance of new protocol behavior. Rust code and tests
at the named baseline remain authoritative; the implementation sketches and
evidence gates below are not requirements to introduce a particular API,
storage format, or transaction protocol.

## Scope and non-goals

The current local slice uses a stable dead-letter move identity and retains
the existing no-loss ordering:

1. Keep the existing safety order: durably append the derived record before
   durably advancing the source consumer.
2. Give each logical move a stable identity derived from the source stream,
   source consumer, and source offset. The consumer name is required because
   independent consumers may legitimately dead-letter the same source record.
3. Make the derived-stream append idempotent for that identity. A retry after
   an uncertain append must resolve an existing record with the same identity,
   rather than append another record.
4. Advance the source only after the target append is known to be durable. If
   the target result is uncertain or cannot be reconciled, leave source
   progress unchanged and return a backend failure with the conservative
   `Unknown` outcome; a later poll/recovery attempt retries the same identity.

The local implementation reuses the durable request-aware record identity,
rebuilt from the target stream log on open, for this internal move identity.
This is not a new public request contract. A same-ID key/payload mismatch
returns an explicit invalid-data storage error; the public publish request-ID
behavior, which intentionally ignores mismatched payloads for compatibility,
is not sufficient for this internal invariant. Both kinds of ID are held in
the same per-target-stream request-ID index, and the internal value is
predictable and within the public ID length limit. A public publish using that
exact ID could therefore collide with a later move. The key/payload mismatch
case is reproduced by
[`dead_letter_move_content_mismatch_is_storage_error_without_acknowledgement`](../../crates/runnel-core/src/lib.rs#L1450): a public publish path reserves the exact move ID with different content, and the later move returns an invalid-data error again after reopen instead of advancing source progress. The real-server test [`network_protocol_preserves_public_record_on_dead_letter_move_id_collision_after_restart`](../../crates/runnel-server/tests/server_smoke.rs#L923) now verifies the same mismatch through the wire path: the poll response is classified as `storage_error`, the conflicting public target record remains unchanged, and polling the source after restart still returns the error rather than showing advanced progress. This at-least-once-safe outcome can leave the source delivery blocked until the target conflict is resolved. Matching-content public-ID reuse is also covered by core test [`dead_letter_move_same_content_public_id_reconciles_after_restart`](../../crates/runnel-core/src/lib.rs#L1489) and real-server test [`network_protocol_reconciles_same_content_public_dead_letter_id_after_restart`](../../crates/runnel-server/tests/server_smoke.rs#L1053): after a public publish uses the move ID with matching key and payload, the local retry treats it as the existing move and source progress completes across restart. These tests establish behavior but not move provenance. The typed-identity alternatives and RNL3 upgrade ambiguity are assessed in the [TD-029 dead-letter identity research note](../research/td-029-dead-letter-identity-contract.md); the accepted identity contract remains open in TD-029.

The current clustered implementation does not use this local move identity.
It appends the derived record and advances source progress in one replicated
state-machine transition while both logical streams remain in the source
stream's data group. That co-location is an accepted property of the current
static-cluster slice, not a general cross-group transaction guarantee.

Neither engine exposes source provenance or a redrive operation. The recovery
goal is at most one durable local derived record per move identity after
reconciliation, not exactly-once delivery or exactly-once application
processing. A dead-letter consumer can still be redelivered, and its external
side effects remain the consumer's responsibility.

## Observed local append and reconciliation behavior

The local engine has two independent durable objects. In
[`Broker::poll_group`](../../crates/runnel-core/src/broker.rs#L309), an
exhausted delivery calls [`Broker::dead_letter_record`](../../crates/runnel-core/src/broker.rs#L592),
which reads the source record, appends its key and payload to the derived
stream, and only then persists a source `Acknowledge` event. Stream appends
call `sync_data`; source consumer events append to a bounded journal and call
`sync_all` before the operation continues. Recovery reconstructs consumer
state from its checkpoint and journal and rebuilds request-aware target
identity by scanning complete target-log frames. The relevant persistence
boundaries are [`StreamLog::append_with_move_id`](../../crates/runnel-core/src/stream_log.rs#L332)
and [`persist_consumer_event`](../../crates/runnel-core/src/consumer_state.rs#L144).

Each new move stores its identity in the same request-ID map used by public
publishes. The map is rebuilt from all complete request-aware frames on open;
it grows with retained identities, and recovery scans the target file. This
reconciliation path has no retention bound today, consistent with the broader
local log/index limitations recorded in [TD-002](../tech-debt.md#td-002-one-file-and-a-startup-scan-per-local-stream).
The broker also pins the consumer policy on first delivery; the attempt limit
that triggers movement is the policy snapshot recorded for that source offset,
even if the consumer's configured policy later changes ([ADR 0027](../decisions/0027-consumer-scoped-retry-policy.md)).

The resulting durable order is intentional:

```text
source message reaches attempt limit
        |
        v
target append + sync_data
        |
        v
source consumer event + sync_all
        |
        v
source progress advances
```

The internal move ID is currently a bounded, length-prefixed textual value:

```text
runnel-dlq/v1/<source-stream-length>:<source-stream>/<source-consumer-length>:<source-consumer>/<source-offset>
```

[`dead_letter_move_id`](../../crates/runnel-core/src/lib.rs#L227) derives it
from the validated source stream, source consumer, and source offset. The ID
is stored in an `RNL3` request-aware target frame and is looked up only within
that target stream's in-memory request-ID index, which also holds public publish
IDs. The writer enforces the request-aware key, payload, and identity limits.
On reopen, complete request-aware frames rebuild that index; an incomplete
trailing frame is discarded, while a complete checksum or format failure is
reported rather than silently treated as a successful move.

The relevant local recovery states and current evidence are:

| State or failure boundary | State observed after reopen | Evidence and limitation |
| --- | --- | --- |
| Before a complete target frame is written | Source remains eligible. The target may be absent or end with an incomplete frame that is truncated on reopen. | `dead_letter_move_recovers_after_partial_target_write_and_restart` injects a half-header write, checks unchanged source progress, reopens, and confirms one reconciled target record. The injected test does not simulate an OS write failure or power loss. |
| A complete target frame is written but `sync_data` has not succeeded | Source remains eligible; a complete-looking target frame may be found on reopen. | `dead_letter_move_reconciles_complete_target_write_reported_as_failure` injects an error after the complete frame and before the real sync call. Reopen rebuilds the ID index and reuses the matching record. It does not establish whether an actual failed device sync leaves durable bytes. |
| Target append succeeds before the source acknowledgement event is written | Target contains the copied record and move ID; source progress remains eligible. | `dead_letter_move_reconciles_target_after_restart` creates this state directly, then reopen/poll reconciles it. `dead_letter_move_reconciles_after_source_ack_persistence_failure_and_restart` exercises the same order through a failed source-event append. |
| The complete source acknowledgement event is written but its sync returns an error | The test filesystem retains the event for reopen; source progress replays and the target remains single. | `dead_letter_move_retries_after_source_event_sync_failure_and_restart` injects the sync error after writing the journal event, drops and reopens the broker, and checks progress and the one target record. It is not a real sync failure or power-loss test. |
| The public poll response is lost after a completed move | The server has completed the source transition and the response is unavailable to the client; after restart the source poll is empty and the target record is consumable once in the tested case. | `network_protocol_reconciles_dead_letter_after_ambiguous_poll_and_restart` covers this real-server journey. It does not kill the server between target sync and source-event persistence. |
| A public target record reserves a local move ID with mismatching content | The source poll returns a `storage_error`; after restart, retry returns the same classification and source progress has not advanced. The public target record remains at its original offset with its original content. | `network_protocol_preserves_public_record_on_dead_letter_move_id_collision_after_restart` covers this conflict through the real server process and protocol. Same-content reuse and the accepted identity contract remain open. |

The local target append checks an existing move ID's key and payload before
reusing it. Core tests cover stable/scoped/bounded identities
([identity](../../crates/runnel-core/src/lib.rs#L1133)), repeated append and
restart reconciliation ([retry](../../crates/runnel-core/src/lib.rs#L1143),
[restart](../../crates/runnel-core/src/lib.rs#L1179)), source acknowledgement
failure and injected file states ([source persistence](../../crates/runnel-core/src/lib.rs#L1225),
[partial target frame](../../crates/runnel-core/src/lib.rs#L1287),
[complete frame before sync](../../crates/runnel-core/src/lib.rs#L1331),
[source event sync error](../../crates/runnel-core/src/lib.rs#L1375)), and
same-ID content mismatch ([mismatch](../../crates/runnel-core/src/lib.rs#L1442)).
Real-server tests cover movement after the attempt limit and restart recovery
([restart](../../crates/runnel-server/tests/server_smoke.rs#L729)) plus a lost
poll response and restart ([ambiguous response](../../crates/runnel-server/tests/server_smoke.rs#L821)), and a conflicting public target ID through restart and retry
([ID collision](../../crates/runnel-server/tests/server_smoke.rs)).
Together these tests support reconciliation for the injected and process-level
states named above; they do not prove behavior under actual filesystem or
device failures, power loss, or a process kill at the exact inter-log boundary.

## Observed clustered same-group movement

The clustered engine has a different boundary. A grouped `PollGroup` is one
Raft command. In [`apply_group_poll`](../../crates/runnel-raft/src/delivery.rs#L49),
the original message is appended to the derived stream held in the same
`SnapshotState` as the source consumer state, then source progress is
advanced before the command response is returned. The state-machine journal
is persisted before applying the command and replayed after a restart through
[`StateMachineStore::apply`](../../crates/runnel-raft/src/state_machine_store.rs#L766).
The derived stream is resolved back to the source data group by
[`data_group_for_stream`](../../crates/runnel-raft/src/group_manager.rs#L338)
when it is addressed through the public protocol.

Consequently, the current clustered path has one replicated logical transition
for source progress plus the derived record. A committed transition is not
split by a process crash or leader change under the data-group durability
guarantee, and a replayed state-machine journal entry restores the same
transition rather than applying an independent local target write. This is
stronger than the local path, but it is still only broker-side movement and
does not make a dead-letter consumer's processing exactly once. The clustered
transition has no move-ID or source provenance field, so its duplicate-safety
comes from source progress and replicated command application, not from a
cross-stream identity index.

The current evidence includes
[`persistent_raft_dead_letters_after_the_configured_attempt_limit`](../../crates/runnel-raft/src/lib.rs#L1327),
which reopens the persistent engine after the transition, and the real
three-process failover test
[`three_process_cluster_reassigns_group_delivery_after_node_failure`](../../crates/runnel-server/tests/cluster_smoke.rs#L928),
which exercises retry exhaustion and dead-letter consumption after reassignment.
The latter validates the current real-process same-group path; the focused
persistent-engine test uses a single-node cluster. A second real-process test
[`three_process_cluster_transfers_consumer_policy_and_delivery_snapshot_after_leader_failure`](../../crates/runnel-server/tests/cluster_smoke.rs#L1243)
shows that a first-attempt policy snapshot still causes the terminal move after
a leader change even though the consumer's current policy allows more
attempts. This extends current evidence to policy transfer, not to split-group
movement or a new transaction protocol.

The clustered transition has no move ID or source-provenance field. Its
duplicate-safety comes from replicated source progress and command application,
not from a cross-stream identity index. The evidence does not establish a
transaction across independent data groups. If a future placement policy puts
the derived stream in another group, the cross-group problem returns and the
current same-group claim must not be generalized without a separately accepted
transaction or reconciliation design.

## Current provenance and redrive boundary

Both engines currently expose the copied key and payload as dead-letter
content, with a target offset and newly generated delivery metadata. The local
append assigns a new timestamp; the clustered transition copies the stored
source message, including its publish timestamp. Neither exposes the source
stream incarnation, source consumer, source offset, attempt history, policy
version, terminal reason, or a move identity through the public
message/protocol model. Local `RNL3` frames carry the internal move ID for
reconciliation, but it is not public provenance; clustered `StoredMessage`
values carry no equivalent move ID. Existing dead-letter records therefore
cannot be retroactively enriched with reliable origin metadata.

There is also no broker redrive operation. An application can consume a
dead-letter stream and publish a new record, but that is a new operation with
new offset and retry state; it is not an atomic source-to-target move and
cannot preserve provenance by inference. Provenance, explicit terminal
outcomes, and redrive remain the separate application-aware policy work in
[TD-018](../tech-debt.md#td-018-retry-policy-and-dead-letter-provenance-are-coarse).

## Future cross-group recovery choices

The current clustered same-group transition is the accepted first layout. No
cross-group transaction or reconciliation design has been accepted. If
placement or named dead-letter targets later put source progress and the
derived record in different durable groups, the implementation must choose a
new boundary rather than inherit the current claim of atomicity.

The outcome boundary to preserve across any future choice is:

- a source record is not considered dead-lettered until a durable target
  record for its stable operation identity exists;
- source progress never advances past a move whose target outcome is unknown;
- retries may occur after crashes and uncertain responses;
- a given source-consumer/offset move has one logical target record after
  reconciliation, while distinct source consumers retain distinct moves; and
- consumers of the dead-letter stream remain at least once.

These are outcome and evidence requirements, not a required command shape or
storage layout. A future design also needs to define how provenance and
redrive interact with an uncertain move, and how pending work is bounded when
the target group is unavailable.

### Candidate mechanisms and trade-offs

| Alternative | Benefit | Cost or unresolved concern |
| --- | --- | --- |
| Preserve same-group placement | Retains the current single replicated transition and avoids a cross-group protocol. | Constrains placement, named targets, balancing, and independent scaling. It is the current accepted boundary, not a general solution. |
| Stable identity plus reconciliation | Lets independently durable groups retry an uncertain move without creating a second logical target record. | Requires durable identity/provenance, target lookup, retention fences, bounded pending work, and explicit handling for target unavailability; source and target are still not one atomic commit. |
| Durable source outbox or pending-move journal plus a relay | Makes the intent recoverable even if the process stops before sending to the target and can provide operator-visible backlog. | Adds another durable state machine and relay lifecycle. Without target identity reconciliation, a crash after target append still duplicates; with it, eventual completion and cleanup semantics remain to be specified. |
| Coordinator-driven cross-group transaction | Can make source progress and target visibility one all-or-none replicated operation. | Requires coordinator and participant state, prepare/commit records, timeout and recovery rules, fencing, and client visibility for in-doubt work. It also expands the public compatibility surface. |
| Combined physical transaction log | Gives source and target one durability boundary without a distributed coordinator. | Changes local/clustered layout, target offsets, retention, recovery, and the separation between source and derived streams; it makes one storage choice dictate the engine contract. |
| Saga or compensating delete | Breaks the move into local transactions without a coordinator. | A compensation after a visible target append can itself be lost or race with a dead-letter consumer. It favors eventual cleanup, not a simple no-loss and duplicate-safe invariant. |
| Keep append-then-checkpoint without an identity | Preserves the original local behavior with minimal code. | The duplicate window remains and legacy records stay opaque. This is only a compatibility baseline, not a candidate for a stronger cross-group guarantee. |

### Reference evidence

Apache Kafka's transaction design combines produced records and consumed
offsets in an atomic unit, uses a persistent transaction log, and requires a
stable transaction identity to recover unfinished work. It also explicitly
limits the guarantee to the transactional broker/consumer boundary rather
than arbitrary external processing ([KIP-98: Exactly Once Delivery and
Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)).
This is evidence for the coordinator-driven alternative, not a requirement
for Runnel's local implementation; adopting it would be a substantially
larger storage and protocol change.

RabbitMQ documents the opposite side of the trade-off. Ordinary dead-letter
exchange republishing removes a message without publisher confirms and can
lose it when the target is unavailable; quorum-queue at-least-once
dead-lettering retains the source until the target confirms, but retries can
produce duplicates and retained pending messages consume source resources
([Dead Letter Exchanges](https://www.rabbitmq.com/docs/next/dlx), [Quorum
Queues: at-least-once dead-lettering](https://www.rabbitmq.com/docs/quorum-queues)).
The comparison makes the trade-off explicit: retaining source progress until
the target is confirmed protects against loss, while retries can duplicate
records and pending work consumes source resources. Any future Runnel design
needs an explicit bound and observability model for that pressure.

The primary research points in the same direction. Garcia-Molina and Salem’s
original Sagas paper models a long operation as interleaved local transactions
with compensating actions ([Sagas, Princeton technical report](https://www.cs.princeton.edu/techreports/1987/070.pdf);
[ACM DOI](https://doi.org/10.1145/38713.38742)). A compensation is not a safe
substitute for an atomic dead-letter move because deleting a target copy can
race with its consumer. Helland’s CIDR position paper describes the practical
alternative: independent durable entities manage uncertainty as workflow and
remember unique messages so repeated delivery is harmless ([Life beyond
Distributed Transactions](https://ics.uci.edu/~cs223/papers/cidr07p15.pdf)).
That supports a stable move identity and reconciliation, but does not prove
exactly-once processing for Runnel or any external consumer.

## Remaining evidence gates

The local slice has focused identity, restart, injected file-state, source-event,
and mismatch coverage. These gates identify evidence not established by those
tests; they are not a retroactive implementation checklist:

1. **Physical durability faults:** test or operational evidence using actual
   filesystem/device write and sync failures, and power-loss conditions, shows
   how source progress and target records recover. The current injected tests
   model selected byte states and error returns but do not establish device
   durability. Generic storage failures remain `Unknown` under [ADR 0026](../decisions/0026-semantic-engine-error-classification.md).
2. **Exact local crash window:** terminate a real broker process after the
   target `sync_data` and before source-event persistence, then restart and
   prove a single same-content target record and eventual source progress. The
   current real-server test covers restart after the completed move and a lost
   response, not termination inside this interval.
3. **Corruption and format handling:** a same-ID target record with different
   content, malformed or torn target data, an unsupported durable format, or
   an unavailable target produces an explicit storage/corruption outcome and
   does not advance source progress. A test covers same-ID key/payload mismatch
   and a partial trailing target frame; broader malformed complete-frame,
   unsupported-format, unavailable-target, and legacy-record reconciliation
   behavior remains open.
4. **Recovery and retention bounds:** identity lookup and reconciliation use
   bounded or explicitly accounted-for indexes/journals, do not scan unrelated
   streams without a documented bound, and retain move evidence until source
   progress no longer depends on it.
5. **Identity contract:** define the accepted outcome when a public publish ID
   equals a computed local move ID. A core test and a real-server wire test
   prove that conflicting content fails reconciliation, preserves the public
   target record, and leaves source progress unadvanced across reopen/retry;
   the wire response is `storage_error`. Same-content reuse and the accepted
   identity contract remain open and are recorded in [TD-029](../tech-debt.md#td-029-public-request-ids-can-collide-with-local-dead-letter-move-ids).
6. **Cluster same-group coverage:** retain real-process tests for committed
   same-group movement through reassignment and policy transfer. Describe the
   result as same-data-group atomicity, not general cross-group atomicity.
7. **Future split-group coverage:** if the target ever moves to another
   durable group, add participant failure, coordinator/retry recovery,
   duplicate-command, retention, and ambiguous-client-outcome tests before
   calling the operation atomic.
8. **Semantic wording:** tests and protocol documentation say at-least-once
   for source-to-target and target-consumer delivery. Exactly-once is not
   claimed unless a later decision proves the complete boundary, including
   application side effects.

## Hypotheses and unresolved risks

- The current request-aware target record and rebuilt ID index provide the
  focused local deduplication slice without a second pending-move journal.
  Tests cover partial frame recovery, a complete frame before sync, and a
  source-event sync error on the current test filesystem. Behavior under real
  device errors or power loss, retention, and future format changes remains
  unverified. The index shares the public request-ID namespace; core and
  real-server tests demonstrate that a conflicting ID blocks source progress
  across reopen/retry and preserves the public target record. TD-029 tracks
  the missing same-content evidence and the unresolved identity contract.
- **Hypothesis:** lazy reconciliation on the next source poll is sufficient
  for correctness. A background reconciler may be needed for operational
  visibility or to make progress when no consumer polls, but it must not
  advance source state on an unconfirmed target.
- A future retention policy must not delete a target record or its move-ID
  evidence while the source move is still unacknowledged. This is a direct
  coupling to the retention work and must be made a durable fence, not a
  best-effort scan.
- Adding provenance later must preserve the local internal identity and
  distinguish intentionally separate moves by independent consumers. It must
  also define how old records with no provenance are represented rather than
  inferring origin from target offsets.
- A target append can complete while its response is lost, and filesystem
  durability can differ from process-crash behavior. Injected returned errors,
  partial frames, and restart tests cover selected cases; actual process
  termination at the two-log boundary and filesystem/device durability remain
  open. A clean in-memory retry is insufficient evidence.
- If source and target storage are placed on different filesystems or devices
  in a future deployment, even an ordered pair of sync calls has no common
  durability boundary. The identity/reconciliation protocol remains useful,
  but full atomicity would require a different accepted design.
- A future named target or placement change can split the current clustered
  data-group boundary. Co-location, reconciliation, an outbox, and a
  coordinator transaction have different failure, retention, and client
  outcome semantics; none is accepted yet.
