# Reusing a publish request ID with changed content

- Status: exploratory contract review; no Runnel behavior or public contract decision accepted
- Last reviewed: 2026-10-05
- Baseline: `fe511a58c701c4061e8a4642c48265fdfca49883` (merged PR #398)
- Related outcome: [Make client interactions dependable and evolvable](../backlog.md#make-client-interactions-dependable-and-evolvable)
- Related decision: [ADR 0004: Use Multi-Raft as the first distributed engine](../decisions/0004-multi-raft-first-distributed-engine.md)
- Distinct from: [TD-029 public request IDs and dead-letter move identity](td-029-dead-letter-identity-contract.md), which concerns separating caller IDs from internal dead-letter move IDs

This review asks what a broker should do when a publish reuses a `request_id`
for the same stream but changes its key or payload. It verifies the current
local and clustered behavior, compares two primary product references, and
records alternatives for a later contract decision. It does not change runtime
behavior or accept a compatibility promise.

## Current Runnel behavior

**Observed locally:** `Broker::publish_with_request_id` looks up the public ID
in the stream's request index and returns its stored offset before examining
the new key or payload. The local index is per stream and is rebuilt from
request-aware log frames on open. The core test
[`repeated_request_id_returns_original_offset_without_appending`](../../crates/runnel-core/src/lib.rs#L451)
publishes `original-key`/`original`, retries the ID with
`retry-key`/`retry-payload`, and asserts offset `0` plus the original stored
message. [`request_id_deduplication_survives_restart`](../../crates/runnel-core/src/lib.rs#L551)
repeats the mismatch after reopening the same log and again gets the original
offset.

**Observed in the clustered implementation:** the replicated publish command
looks up `dedup[stream][request_id]` and returns `Published { offset }` before
appending the incoming key or payload. The dedup map is part of persisted
state-machine and snapshot state. The real-process cluster test
[`three_process_cluster_replicates_and_recovers_after_failures`](../../crates/runnel-server/tests/cluster_smoke.rs#L165)
retries the same ID from another node with the same content; it demonstrates
cross-node deduplication, but does not test a changed key or payload. Therefore
cluster mismatch behavior follows directly from the state-machine code, while
the corresponding mismatched-content cluster case lacks focused end-to-end
coverage.

The ID is scoped to one stream in both implementations. Neither lookup uses a
time-based duplicate window: the local mapping is recovered from retained log
records, while clustered state stores the mapping with replicated state. The
current retained-history slice has no message-retention policy that defines an
ID expiry boundary. Future retention or deletion work would need to state how
it interacts with retry identity.

The [client README](../../crates/runnel-client/README.md#run-the-application-example)
says to treat an ID as unique to one logical publish on its stream and retry
with the same ID and bytes. [ADR 0004](../decisions/0004-multi-raft-first-distributed-engine.md)
accepts that a stable ID lets a safe retry return its original offset, but does
not define how changed publish inputs should be classified. The
[single-node-to-cluster migration design](../design/single-node-to-cluster-migration.md#local-state)
records preservation of today's mismatch behavior as a migration constraint;
it is a design proposal rather than a decision on the public contract. The
current local code comment calls the behavior intentional for compatibility,
and the local tests assert it. These are evidence of existing implementation
intent, not an explanation of the client-facing tradeoff or a cross-engine
contract decision.

## Reference behavior

**Stripe API:** Stripe documents that it compares parameters on reuse of an
idempotency key and errors when they differ. It retains a result for the first
request after endpoint execution begins; validation failures and concurrent
execution conflicts are not saved as idempotent results. Keys may be pruned
after at least 24 hours, after which reuse starts a new request. This makes
input mismatch visible, but its HTTP operation-result model and expiry policy
do not establish what Runnel's retained-message identity should be. See
[Stripe's idempotent request contract](https://docs.stripe.com/api/idempotent_requests).

**NATS JetStream:** JetStream documents message-ID-only duplicate detection:
its example sends the same `Nats-Msg-Id` with different bodies, consults only
the ID, and retains only the first message. It scopes duplicate recognition to
a configurable sliding window, with a documented two-minute default in the
model deep dive. This is closer to a message broker's publish path and to
Runnel's current first-write-wins behavior, but its finite window differs from
Runnel's current log-backed mapping. See the official
[JetStream deduplication description](https://github.com/nats-io/nats.docs/blob/master/using-nats/jetstream/model_deep_dive.md#message-deduplication)
and [publish header reference](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/headers.md#publish).

These references show two established choices rather than one universal rule:
Stripe rejects mismatched operation parameters; JetStream treats the supplied
message ID itself as the deduplication identity and ignores body differences
within its window. Neither reference defines a Runnel contract.

## Runnel-specific assessment

The current behavior makes an exact retry after a lost response resolve to the
original offset without adding a second record. It also returns that same
confirmed offset if an application accidentally reuses an ID for a different
key or payload. A caller can therefore mistake a successful response for
acceptance of the new content. This risk is especially relevant to a reusable
client contract: the client can preserve an ID, but cannot verify from the
response that the retried input matched the original record.

Three plausible contract choices remain:

1. **First use wins, regardless of content.** Keep the current code. Document
   that the ID denotes the logical publish, that the first accepted record is
   authoritative, and that callers must retry the exact original inputs. This
   has a cheap lookup and matches JetStream's identity-only choice. It must
   also define the ID's lifetime as retention evolves. The main risk is silent
   suppression of a genuinely new message when an ID is mistakenly reused.
2. **Reject a mismatched reuse.** Return the original offset for equivalent
   publish inputs, but return a specific rejection when the key or payload
   differs. This is aligned with Stripe's mismatch detection and surfaces
   accidental ID reuse. The contract would need to define equality over all
   message-affecting fields and how the rejection helps a caller resolve the
   original ambiguous publish. A local implementation may need to read or
   fingerprint the original payload; clustered and local engines must preserve
   the same comparison and error semantics.
3. **Make content part of the caller's identity.** Require a derived or
   otherwise content-bound request ID. This can make changed input appear as a
   different publish, but it moves ID construction and collision handling to
   clients and does not itself report accidental reuse. It is not implied by
   the current API.

**Inference / recommendation for a future decision:** the contract should
explicitly say whether a request ID denotes only an operation identity or an
operation plus its semantic inputs, and state its stream scope and lifetime.
Rejecting mismatched key/payload reuse is the clearest way to expose a caller
bug, while preserving first-use-wins may be preferable if Runnel treats the ID
as a pure message identity and accepts that a duplicate acknowledgement says
only “this ID already committed.” The references and current evidence are not
sufficient to select between those meanings. Keep the intended outcome open
until the project accepts one with its response and retention consequences.

## Evidence gaps and disposition

- Add a real-server clustered test that reuses one ID with a changed key and
  payload, including after leader change or restart, once the contract is
  selected. Current local tests cover the mismatch and restart path; current
  clustered process coverage retries identical content only.
- If first-use-wins is selected, document that the returned offset identifies
  the first accepted record even when retry contents differ, and preserve the
  exact-input retry guidance in supported clients.
- If mismatch rejection is selected, specify the public error/outcome and
  compare exact-input enforcement cost and durable index options across both
  engines before implementation.
- No runtime change, test, or ADR is warranted by this research-only change.
  The client-interactions backlog outcome remains open; this note supplies
  evidence for a later contract decision.
