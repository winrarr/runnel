# Single-node client and operations security

**Status:** first application-client security contract accepted by [ADR 0035](../decisions/0035-first-application-client-security.md); runtime behavior is not implemented. **Last reviewed:** 2026-10-06. **Observed baseline:** `5d3625d9c63903809e2a026c997522cf182786e5`.

This document records the accepted boundary and the implementation work it
leaves. Current behavior is defined by code and tests. The source-backed
[authentication research](../research/client-authentication.md) compares that
behavior with first-party broker references and primary TLS guidance. This
design adds no runtime control or security guarantee.

## Current behavior and exposure

- The server binds the client JSON-lines listener directly over plain TCP,
  defaulting to `127.0.0.1:4222`. There is no TLS, authentication, or
  authorization state before requests reach dispatch ([bootstrap](../../crates/runnel-server/src/bootstrap.rs),
  [connection handling](../../crates/runnel-server/src/connection.rs),
  [dispatch](../../crates/runnel-server/src/dispatch.rs)).
- The reusable Rust client opens a plain `TcpStream`; `runnelctl` uses the same
  path. Neither currently accepts credentials or TLS settings
  ([client](../../crates/runnel-client/src/lib.rs),
  [CLI](../../crates/runnel-cli/src/main.rs)).
- The HTTP listener also defaults to loopback but serves `/health/live`,
  `/health/ready`, and `/metrics` without TLS or authentication. Readiness
  exposes stream count and storage bytes; metrics show operational signals
  ([observability](../../crates/runnel-server/src/observability.rs)).
- The development Kubernetes manifest binds both listeners to `0.0.0.0`,
  exposes port 8080 through its regular and headless Services, and uses the
  shared HTTP listener for health probes and metrics. No NetworkPolicy or
  HTTP access control is installed. This is not a private boundary by default
  and is not a supported production security configuration
  ([manifest](../../deploy/kubernetes/runnel.yaml),
  [deployment notes](../../deploy/kubernetes/README.md#health-and-traffic-routing),
  [metrics notes](../../deploy/kubernetes/README.md#metrics-and-monitoring)).
- The separate Raft peer listener also uses plain TCP today. Application
  listener security does not protect peer RPCs or make a cluster secure.

## Accepted first client contract

The accepted choice is summarized here; [ADR 0035](../decisions/0035-first-application-client-security.md)
is authoritative if wording differs.

- Terminate TLS in Runnel for secured application connections and require TLS
  1.3. Clients validate the server certificate chain and connection name.
  TLS 1.2 may be added only as an explicit non-default compatibility extension
  if supported-client evidence justifies its additional profile.
- Disable TLS 1.3 early data (0-RTT). Runnel has no replay-safe protocol
  profile for mutating operations, so requests cannot reach dispatch before
  the full handshake completes ([RFC 9846](https://www.rfc-editor.org/info/rfc9846/)).
- TLS and pre-authentication input must use bounded connection slots, deadlines,
  memory, and frame sizes; an expired or failed authentication attempt closes
  the unauthenticated connection.
- Retain unauthenticated plaintext only on the loopback local-development
  default. A non-loopback client listener requires both TLS and a bearer
  credential; partial or invalid secure config fails closed, with no protocol
  downgrade. An explicitly named development/test override does not make a
  remote deployment secure.
- Authenticate each secured connection before any ordinary protocol request.
  A runtime policy file contains credential IDs, token verifier digests, and
  one fixed role per token; raw 256-bit random tokens come from protected
  runtime client secret sources. Multiple token entries per role allow a
  staged restart-based rotation.
- `application` credentials can publish, batch publish, consume, replay,
  acknowledge, and inspect consumer policy. `operator` credentials can issue
  all current protocol operations, including stream creation, consumer-policy
  changes, and protocol health inspection. Authorization is checked before
  engine dispatch. The policy has no per-stream or per-consumer grants; every
  application credential can access all broker data.
- Keep HTTP health and metrics outside the application credential boundary.
  The listener stays loopback by default. If a deployment binds it beyond
  loopback, its network must allow only trusted health-probe sources and the
  designated private scraper; do not publish it to an untrusted or public
  route. The current development manifest does not enforce this requirement.
- Scope the first deployment guarantee to a single node. Credential policy is
  supplied locally, not replicated. A clustered deployment needs consistent
  policy on every client-serving replica and coordinated rotation; this ADR
  does not establish a safe rolling configuration procedure or peer security.

Authentication exchange bytes, protocol-version negotiation, TLS crate/API,
configuration syntax, command-line names, client API shape, token generation
tooling, and HTTP listener implementation remain implementation or protocol
design work. In particular, do not add an authentication frame or negotiation
field here; the protocol compatibility decision owns those bytes.

## Implementation guidance and release gates

The server must validate the certificate/key pair and complete credential
policy before binding a secure client listener. Invalid configuration,
unknown roles, or unreadable files are startup errors. The client must never
send a token before TLS succeeds, must verify the server's identity, and must
not automatically replay an operation after an authentication or transport
failure. Credential and certificate changes require a restart; old and new
token verifiers may overlap during client migration, but restarting drops
persistent connections. When requests may already have reached dispatch, the
current unknown-outcome rules still apply.

Authorization must be an exhaustive mapping from each protocol operation to a
role requirement. Unknown or unclassified operations are denied until that
mapping is updated. A denial must not reach the engine or reveal stream or
consumer existence. The policy must stay outside the engine and persistent
state: client identity is a server transport concern, not a durable message
attribute.

Logs and metrics must omit tokens, verifier digests, private keys, message
payloads, message keys, resource names, and attacker-controlled identity
labels. Security metrics should use only fixed low-cardinality operation and
reason values. Repeated bad credentials must not create unbounded log or
metric state.

The secured listener needs real-process tests for loopback development mode,
remote-bind rejection and explicit development override, TLS 1.3 success and
TLS 1.2/plaintext rejection, disabled 0-RTT, server trust and name validation,
incomplete handshake and oversized pre-authentication bounds, credential
failure before dispatch, both roles across every operation, exhaustive
authorization for future variants, invalid config before listener acceptance,
secret redaction, and restart-based token and certificate rotation. Tests must
prove no request is replayed automatically when its result may be unknown.

Deployment evidence must separately validate HTTP access. For Kubernetes,
health probes and the designated scraper must reach their intended routes,
while untrusted namespaces and ingress cannot reach port 8080. Validate the
actual network-policy behavior with the supported CNI; the presence of policy
YAML is not proof of isolation. The existing development manifest remains
explicitly insecure until TLS/authentication, protected deployment networks,
and the separate peer-security boundary are implemented and tested.

Correctness and operational safety are the primary evidence. A docs-only
contract decision has no applicable runtime tests and makes no throughput or
latency claim. Measure connection setup and persistent-connection costs only
after implementation if making a performance claim.

## Deferred boundaries

- Named principals with exact stream/consumer grants and custom ACL patterns
  are deferred. They could isolate mutually untrusted services, but Runnel is
  not initially multi-tenant and has no principal/policy management surface.
- Mutual TLS, OAuth/OIDC, LDAP, password authentication, automated token
  issuance, token expiry, live reload, and external secret-manager plugins are
  deferred until a concrete deployment requires their lifecycle and
  availability contracts.
- The shared cleartext HTTP listener remains a security gap. Route-level
  authentication, TLS, or split listener policy may be a later runtime slice;
  until then, operations must be restricted to a verified private network.
- Cluster-wide credential distribution, safe rolling rotation, peer mTLS,
  node identity, and peer protocol compatibility remain outside this
  single-node contract and require their own accepted design.

The existing [single-node readiness backlog](../backlog.md#make-the-single-node-deployment-ready-for-real-use)
remains open until runtime controls, client configuration, deployment
isolation, and the real-server test matrix are delivered. This design and
research record no longer recommends per-resource ACLs or treats HTTP routes
as private by assumption; those are explicitly deferred or operationally
bounded as above.
