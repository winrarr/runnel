# Peer protocol versioning and upgrade contract

Status: exploratory evidence; no wire format or upgrade policy accepted

Reviewed: 2026-10-05

Baseline: `d3a7d766c48d8e1a46f4df387ccd55595a21284b`

Scope: determine what compatibility boundary exists today between clustered
Runnel peers, and what must be decided before a mixed-binary or rolling-upgrade
claim is made. This note addresses protocol versioning and upgrade behavior.
Peer identity, authentication, TLS, authorization, credential handling, and
rotation are separate security questions and are not evaluated here.

Related: [Make broker and peer communication efficient and evolvable](../backlog.md#make-broker-and-peer-communication-efficient-and-evolvable),
[Make the clustered deployment operable](../backlog.md#make-the-clustered-deployment-operable),
[TD-012 peer transport ownership](td-012-peer-transport-ownership.md),
[ADR 0006](../decisions/0006-separate-metadata-and-data-groups.md), and
[cluster deployment upgrade assumptions](../../deploy/kubernetes/README.md#upgrade-and-rollback).

## Observed Runnel behavior

- [`framing.rs`](../../crates/runnel-raft/src/network/framing.rs) encodes each
  message as a four-byte big-endian body length followed by JSON, with a 64 MiB
  maximum body. The framing has no magic, protocol version, or negotiation
  preface.
- [`network.rs`](../../crates/runnel-raft/src/network.rs) defines private
  Serde `PeerRequest` and `PeerResponse` enums. Requests include OpenRaft vote,
  append, and snapshot RPC types alongside Runnel forwarding and data-group
  setup operations. Responses carry corresponding OpenRaft types or Runnel
  results. There is no envelope version or declared compatibility range.
- [`inbound.rs`](../../crates/runnel-raft/src/network/inbound.rs) decodes each
  frame directly as the current `PeerRequest`, dispatches it, and serializes
  the current `PeerResponse`. A decode failure closes that connection; there
  is no typed incompatible-version response.
- [`outbound.rs`](../../crates/runnel-raft/src/network/outbound.rs) serializes
  the current request type and deserializes the expected current response type
  over pooled or retained TCP connections. A malformed frame or unexpected
  response becomes an I/O or `Unreachable` error. That path does not
  distinguish schema incompatibility from network failure.
- The workspace pins OpenRaft to exactly `0.9.25` with its `serde` and
  `storage-v2` features ([workspace manifest](../../Cargo.toml)). OpenRaft's
  Serde-derived network structures therefore participate in the JSON shape
  sent between Runnel processes; the internal peer schema is coupled to both
  Runnel's enums and the pinned dependency's serialized types.
- Runnel does version and validate several **local persisted** formats,
  including cluster metadata, Raft logs, state-machine snapshots, and its
  journal. Those checks in [`engine.rs`](../../crates/runnel-raft/src/engine.rs),
  [`log_store.rs`](../../crates/runnel-raft/src/log_store.rs),
  [`state_machine_store.rs`](../../crates/runnel-raft/src/state_machine_store.rs),
  and [`state_machine_journal.rs`](../../crates/runnel-raft/src/state_machine_journal.rs)
  are separate from peer wire compatibility. They do not negotiate which RPC
  representations two live processes can exchange.
- The public client protocol has its own declared version range and payload
  encodings in [`runnel-protocol`](../../crates/runnel-protocol/src/lib.rs).
  That public-client contract is not used by the internal peer listener.
- The Kubernetes example is explicitly a development-only static three-node
  deployment. Its StatefulSet uses `OnDelete`, and its operations guide says
  that no rolling-upgrade, downgrade, or rollback procedure is supported and
  that peer/protocol compatibility remains under development. It also warns
  that `OnDelete` does not make mixed binaries compatible
  ([manifest](../../deploy/kubernetes/runnel.yaml),
  [upgrade and rollback guidance](../../deploy/kubernetes/README.md#upgrade-and-rollback)).
- The existing [TD-012 transport note](td-012-peer-transport-ownership.md)
  already records that frames lack version negotiation and compares transport
  ownership and multiplexing alternatives. This investigation narrows the
  unresolved question to the compatibility and deployment contract; it does
  not reopen its connection-pooling conclusions.

## Primary and reference findings

- The pinned [OpenRaft 0.9.25 `RaftNetwork` API](https://docs.rs/openraft/0.9.25/openraft/network/trait.RaftNetwork.html)
  defines the application-provided RPC transport interface. Its
  [Serde feature documentation](https://docs.rs/openraft/0.9.25/openraft/docs/feature_flags/index.html#feature-flag-serde)
  says Serde derives are provided for types used in storage and network, such
  as `Vote` and `AppendEntriesRequest`. These sources describe APIs and
  serialization availability; they do not establish a stable Runnel wire
  schema, cross-release JSON compatibility, or an upgrade procedure.
- OpenRaft's [0.9.25 release notes](https://github.com/databendlabs/openraft/releases/tag/v0.9.25)
  identify that release as bug-fix-only and report no public API or storage
  format changes. This is useful for identifying the exact dependency update,
  but it is not a promise about Runnel's enclosing peer enums or a future
  OpenRaft RPC serialization shape. OpenRaft's [crate documentation](https://docs.rs/openraft/0.9.25/openraft/)
  also describes the pre-1.0 API as not stable and says an upgrade may contain
  incompatible changes. API stability and wire compatibility are distinct
  contracts.
- The etcd 3.7 [upgrade policy](https://etcd.io/docs/v3.7/upgrades/upgrading-etcd/)
  limits supported upgrades to defined patch or adjacent-minor transitions.
  Its [3.6-to-3.7 procedure](https://etcd.io/docs/v3.7/upgrades/upgrade_3_7/)
  specifies that mixed-version members operate using the lowest common
  protocol version, that members negotiate the cluster version, and that
  rollback is available only before the cluster version advances. The
  relevant Runnel lesson is the evidence surface: a rolling-upgrade promise
  includes supported version skew, negotiated feature behavior, health gates,
  and rollback boundaries, not just a version integer in a frame. etcd's
  mechanisms are a reference, not a proposed Runnel design.
- The Protocol Buffers [message update guidance](https://protobuf.dev/programming-guides/proto3/#updating-a-message-type)
  documents which schema changes are wire-safe or conditionally compatible
  for its binary encoding and warns against reusing removed field numbers.
  This demonstrates that compatibility rules depend on the chosen encoding and
  schema evolution rules. It does not imply that Runnel should replace JSON
  with Protocol Buffers.

## Runnel-specific inference

The current three-node deployment is configured with one static membership
list and does not support mixed binaries as an operational procedure. A
version handshake is therefore not required to substantiate the deployment's
current documented feature set: a development cluster is supported only as
the implementation currently behaves, with no rolling-upgrade claim. A
failure to decode a changed peer schema during an unsupported mixed-binary
experiment is a known limitation, not evidence that a version marker is
already promised.

Before Runnel advertises mixed-version operation, the current transport needs
an explicit compatibility policy and enough protocol machinery to enforce
it. Without a pre-dispatch compatibility check, a changed Serde enum or
OpenRaft request/response representation may fail as a generic decode or RPC
failure. Depending on the shape of a change, successful parsing alone would
also not prove that the message retains its intended meaning. A wire-version
integer by itself is insufficient: peers need a documented supported range
or capability contract, a deterministic incompatible-peer outcome, and
tests for every supported old/new combination. How cluster-wide feature
activation and downgrade work must be decided separately from identifying a
frame schema.

Near-term disposition: resolving this boundary is reasonably implementable
as a bounded policy decision and test plan before production upgrade support
is scheduled. A versioned handshake can then be added if the accepted policy
requires mixed versions. Do not add a version field alone and infer rolling
compatibility from it; do not treat the public protocol's v1 range or local
storage format versions as peer compatibility. This outcome remains open
because the repository has not accepted a peer compatibility policy or
validated an upgrade path.

## Alternatives

1. **Keep peers same-build only and make no mixed-version claim.** This matches
   the current development deployment. Document and enforce the operational
   boundary in deployment guidance; before adding a supported upgrade flow,
   decide whether a full-cluster maintenance restart is acceptable and test
   cluster stop/start recovery. It avoids premature wire negotiation, but
   does not provide rolling upgrades and leaves configuration mistakes hard
   to diagnose.
2. **Add a version/capability handshake around the current JSON messages.**
   Define the handshake representation, protocol family, supported versions,
   required capabilities, negotiation result, and fail-closed mismatch
   behavior. Keep the accepted compatibility set narrow at first. This is an
   incremental route if independent node upgrades become a product need, but
   still requires schema discipline for both Runnel and OpenRaft-owned types.
3. **Define a Runnel-owned peer schema independent of OpenRaft Serde types.**
   Map versioned Runnel RPC records to and from the OpenRaft adapter types.
   This gives Runnel clearer control over wire evolution and error semantics,
   at the cost of conversion code, fixtures, and an intentional protocol
   boundary refactor. It does not by itself decide mixed-version behavior.
4. **Adopt a schema-governed encoding such as Protocol Buffers.** This can
   provide explicit field-level evolution rules, but is a broader change in
   dependencies, code generation, representation, and benchmarking. The
   current evidence does not show that changing encodings is needed to settle
   the upgrade contract.

## Open risks and evidence gates

Before accepting any mixed-version or rolling-upgrade guarantee, a design and
implementation review should settle:

- whether supported operation requires identical broker release, a defined
  peer-protocol version range, or a compatibility matrix across Runnel and
  OpenRaft versions;
- how a connection identifies its protocol before decoding a version-specific
  request, how common versions/capabilities are selected, and how unknown or
  incompatible peers fail without being reported as ordinary network outages;
- which message families and semantic changes are covered: vote, append,
  snapshot chunks and completion, forwarded operations/results, data-group
  setup, errors, payload limits, and any future multiplexed traffic;
- whether compatibility is symmetric, which node may lead during mixed
  versions, when new features may be emitted, how downgrade is constrained,
  and what state requires backup or migration;
- compatibility fixtures for prior supported protocol shapes, tests for
  unknown variants/fields and missing required data, and real three-process
  tests that upgrade one node at a time while checking quorum, append, snapshot
  catch-up, forwarding, restart, and rollback gates;
- a deployment-visible way to report local broker version, peer protocol
  support, and the negotiated cluster protocol without high-cardinality
  labels. Authentication and trust are independent requirements and need
  their own design and evidence.

The existing static-cluster tests establish same-build process behavior and
recovery paths; they do not establish mixed-version compatibility. No
benchmark is needed to establish a versioning contract, though any eventual
encoding or transport change should follow the communication benchmark gate
in [`docs/benchmarking.md`](../benchmarking.md).

## Disposition

Keep the deployment's no-rolling-upgrade statement in force. Treat an explicit
peer protocol version and tested upgrade contract as a prerequisite to any
future mixed-binary support, rather than as a requirement for the current
same-build development cluster. The backlog outcome remains open: its
communication compatibility acceptance criterion is not met by versioned
local files, the separate public protocol version, or same-build integration
tests. No ADR or runtime change is warranted until the product requires a
specific upgrade mode and its supported skew and rollback behavior are
accepted.
