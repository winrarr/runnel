# ADR 0032: Authenticate static cluster peers with mutual TLS

- Status: accepted
- Date: 2026-10-06
- Revalidated against baseline: `c3a894b6d88a40245c1116e2c5006b94f5573aee`
- Primary evidence class: design/research
- Related: [ADR 0004](0004-multi-raft-first-distributed-engine.md), [TD-008](../tech-debt.md#td-008-distributed-raft-backend-is-an-early-static-cluster-implementation)
- Research: [Cluster peer transport security](../research/cluster-peer-transport-security.md)

## Context

The static Raft deployment currently sends length-prefixed JSON peer requests
over plain TCP. The same listener receives append, vote, and snapshot RPCs as
well as forwarded broker operations and data-group setup. A reachable caller
can therefore send internal broker operations without proving that it is a
configured cluster member. The public client and HTTP operations listeners
are separate boundaries and are not part of this decision.

Runnel owns the OpenRaft network adapter, and its static configuration already
maps node IDs to peer addresses. Static mutual TLS therefore fits the existing
transport and membership model. The peer transport is a new TLS-using protocol
with no older-client population or mixed-binary rollout promise. [RFC 9852](https://www.rfc-editor.org/info/rfc9852/)
therefore sets TLS 1.3 as the initial peer profile; TLS 1.2 is not enabled by
default. etcd's [peer transport guidance](https://etcd.io/docs/v3.6/op-guide/security/)
distinguishes certificate-authenticated peers from self-signed TLS that only
encrypts; [RFC 9525](https://www.rfc-editor.org/info/rfc9525/) defines
service-identity matching, and [RFC 9846](https://www.rfc-editor.org/info/rfc9846/)
describes TLS 1.3 including replay considerations for early data. These
references support the boundary, but do not define Runnel's cluster or node
identity.

## Decision

Every inter-node connection used by the Raft peer listener must use mutually
authenticated TLS before sending or reading a Runnel frame. This includes
consensus RPCs, snapshot transfer, forwarded broker operations, and
data-group setup. No peer operation may fall back to plaintext after a
handshake, trust, or identity failure.

In Raft mode, each node must be configured with a peer trust bundle, its own
leaf certificate, and the matching private key. The trust bundle contains
only operator-selected cluster peer trust anchors; the implementation must
not implicitly add operating-system or public web PKI roots. Each node has a
distinct key pair. Missing, unreadable, malformed, or mismatched credentials
prevent startup before the peer listener accepts traffic. Credential contents
must be delivered by the operator through a protected runtime secret source,
not embedded in an image, command-line value, or broker data directory;
arguments may identify protected file paths or secret-source references only.

A trusted certificate must also identify exactly the intended configured
peer. Its leaf certificate must be valid at the time of the handshake, chain
to a configured peer trust anchor, permit both TLS client and server use, and
carry the Runnel peer identity as a `dNSName` Subject Alternative Name. The
canonical identity is:

```text
n<node-id>.c-<cluster-hash>.peer.runnel.invalid
```

`node-id` is the configured unsigned node ID in canonical decimal notation
(zero is `n0`, with no leading zeroes). `cluster-hash` is the 52-character
lowercase, unpadded form of RFC 4648 Base32 encoding of SHA-256 over the exact
UTF-8 bytes of the configured cluster name, using the RFC 4648 alphabet with
ASCII case folded to lowercase. The TLS reference identifier is constructed
from the expected configured node ID and cluster name, independently of the
socket's dial address, then checked using the standard DNS-ID matching rules.
The `peer.runnel.invalid` suffix is an identity namespace, not a DNS
destination. Common Name, wildcard names, and other SAN values do not satisfy
this check.

Outbound connections must validate the exact target node ID and cluster
identity, not merely accept any certificate issued by the configured CA.
Inbound connections must map the authenticated identity to a configured
static peer ID other than the local node, in the same cluster, before reading
a frame or dispatching a request. Transport pools must retain the expected
node identity alongside an address; an address alone is not an identity. If a
path cannot determine one unique configured peer identity, it must fail
closed. This policy does not change Raft membership or permit identities
outside the static peer map.

Require TLS 1.3 for the initial peer profile and reject TLS 1.2 and earlier;
disable TLS 1.3 early application data (0-RTT). Do not add TLS 1.2 as a
compatibility mode unless deployment evidence justifies it and a later
decision accepts it as an explicit non-default option. Consequently, nodes in
environments that support only TLS 1.2 cannot form or operate a cluster. Peer
protocol versioning is separate from TLS negotiation. The first secure
deployment has no mixed plaintext/TLS mode and makes no mixed-binary or
rolling-upgrade compatibility promise. Existing clusters must make a
coordinated cutover:
stop all old plaintext nodes, install the common trust bundle and each node's
own credentials, then start the full cluster with the same static membership
and binary version. New clusters start with these settings from the beginning.

Credential files are read at process startup. Replacing a file does not
reload trust, rotate an active TLS session, or revoke an established
connection; restart is required. Leaf renewal under the current CA proceeds
one node at a time, preserving at least two available voters in a three-node
cluster. CA rotation uses an overlap bundle: distribute old and new trust
anchors and restart nodes one at a time; replace node leaves one at a time;
then remove the old anchor and restart nodes one at a time again. Verify peer
traffic and quorum after each restart. Online CRL/OCSP checks and individual
certificate revocation are outside the initial policy. If a node key is
suspected compromised, leaf replacement alone is insufficient while that
certificate remains trusted; operators must rotate the issuing trust anchor
and all affected leaves. The old anchor remains accepted during an overlap
rotation, so the compromise response has a bounded transition window; if that
window is unacceptable, the cluster requires an outage for an immediate
trust-boundary change.

Public-client authentication, public-client TLS, HTTP operations endpoint
authentication or TLS, Kubernetes NetworkPolicy, dynamic membership, and
authorization differences between configured peers remain outside this
decision. Network filtering is defense in depth, not a substitute for peer
authentication or encryption. A valid node credential identifies a trusted
static member; it does not make a compromised member benign or distinguish
two processes holding the same private key.

## Rationale and alternatives

Static mTLS supplies confidentiality and integrity for every message on the
peer transport and cryptographic identity for each configured member, while
keeping the boundary in the existing Runnel-owned TCP adapter. The explicit
SAN-to-node mapping prevents a certificate from a broad organizational CA
from gaining peer authority merely because its chain is trusted. A cluster
hash keeps arbitrary cluster names out of DNS labels and prevents accidental
cross-cluster identity reuse when a CA bundle is shared.

Managed SPIFFE/SPIRE identity can provide short-lived certificates and
streamed trust-bundle updates, but adds workload attestation, agents, and a
control-plane dependency not present in the initial static deployment.
Per-certificate pinning can narrow trust further but makes member replacement
and trust changes more manual than a cluster CA plus exact SAN mapping.
NetworkPolicy reduces reachability but does not encrypt or cryptographically
identify a caller. Server-only TLS does not authenticate the connecting
member. Automatic self-signed TLS encrypts without establishing the configured
node identity, and a shared token would leave confidentiality, replay, and
rotation behavior to a new custom protocol. These options do not meet the
chosen peer boundary as directly as static mTLS.

## Consequences and implementation gates

- TLS must wrap all peer sockets before frame parsing, including direct
  OpenRaft clients and the forwarding/setup connection pools.
- Transport code must preserve the expected node ID for both direct and
  pooled connections. Duplicate address mappings that make the intended
  identity ambiguous must be rejected rather than resolved by address alone.
- Startup and handshake errors must distinguish configuration, certificate
  trust, peer identity, and ordinary reachability failures without logging
  private material or untrusted high-cardinality identities.
- Handshake concurrency, deadlines, sockets, and TLS/frame memory remain
  bounded. TLS does not remove the unauthenticated resource-exhaustion risk
  before a handshake completes.
- Focused certificate tests and real three-process tests must cover accepted
  member identities; missing, malformed, expired, untrusted, wrong-node,
  wrong-cluster, and unconfigured certificates; plaintext rejection before
  dispatch; successful TLS 1.3 handshakes; TLS 1.2 rejection; and the full
  consensus, forwarding, and setup paths.
- Rotation tests must demonstrate old/new trust overlap and one-node-at-a-time
  restart while quorum remains available. Public client and HTTP behavior
  must remain unchanged by this peer-only policy.
- A controlled clustered benchmark must measure handshake/reconnect cost,
  steady peer RPC overhead, CPU and memory use, and commit tail latency before
  making a production performance claim. The policy itself makes no claim
  that TLS has negligible cost.
- Static identity does not solve key theft, dynamic membership, process
  incarnation fencing, or storage compatibility. Those remain governed by
  their separate cluster recovery and compatibility work.

## Verification basis

This decision is implemented for the current static Raft server path. Startup
loads explicit per-node trust, certificate, and key files before binding the
peer listener. The runnel-raft transport uses rustls TLS 1.3 with early data
disabled, binds the exact certificate SAN to configured node and cluster
identity, and bounds handshakes, active peer sessions, frame memory, and
concurrent writes. Focused certificate tests cover expiry and identity/trust
failures; real three-process tests cover peer transport during consensus,
recovery, forwarding, plaintext rejection, and unconfigured identity rejection.

This implementation status does not mean the full operational outcome is
complete: trust-overlap rotation is not process-tested or automated, the
Kubernetes example requires an operator-provided per-pod credential overlay,
and representative clustered measurements have not been collected. The
backlog remains open for those rollout and evidence requirements. The
source-backed research inspected Runnel's inbound dispatch, outbound direct
and pooled connections, static node map, deployment assumptions, OpenRaft
network interfaces, etcd peer-security guidance, RFC 9525 service identity,
RFC 9846 TLS behavior, SPIFFE identity lifecycle, and Kubernetes network-policy
and Secret guidance.
