# ADR 0035: Define the first application-client security contract

- Status: accepted
- Date: 2026-10-06
- Revalidated against baseline: `5d3625d9c63903809e2a026c997522cf182786e5`
- Primary evidence class: design/research

## Context

The current application listener is plain TCP with no authentication or
authorization. The Rust client and `runnelctl` use that listener without TLS
or credentials. The client defaults to `127.0.0.1:4222`, and the separate
HTTP listener defaults to `127.0.0.1:8080`. The development Kubernetes
manifest binds both listeners to all pod interfaces, exposes the HTTP port
through two Services, and uses the unauthenticated HTTP routes for probes. It
is a development deployment and supplies no network policy or TLS.

Runnel initially targets small engineering teams that need a self-contained
broker and do not operate a dedicated messaging security control plane. It is
not initially a hosted multi-tenant service. The product backlog already
requires runtime-supplied credentials, client authentication and
authorization, documented client TLS, and repeatable security tests. The
source-backed [authentication research](../research/client-authentication.md)
compares the current code and first-party Kafka and NATS references with the
applicable IETF guidance.

The existing [single-node security design](../design/single-node-security.md)
proposed named principals with per-stream and per-consumer ACLs. That policy
surface would require a permission matrix and operator lifecycle not present
in Runnel. The current product does not require tenant isolation. A fixed
application role plus a fixed operator role is a narrower first authorization
boundary; it does not claim per-service or per-stream isolation.

## Decision

Accept the following first application-client security contract for the
Runnel server's application-protocol listener. This decision defines intended
semantics, not current behavior, config syntax, code structure, or a stable
wire schema.

### Transport and listener defaults

- The server terminates TLS for application client connections. TLS must
  complete before any credential or application request is sent. The first
  secure profile supports TLS 1.3 only. The 2026 IETF Best Current Practice
  requires TLS 1.3 for new TLS-using protocols; TLS 1.2 remains an optional
  future compatibility extension only if supported-client evidence justifies
  it. Runnel currently has a Rust client and no backward-compatibility promise,
  unlike Kafka and NATS, which retain TLS 1.2 paths for their wider client
  ecosystems. The explicit cost is that TLS-1.2-only clients cannot connect.
- The server presents a runtime-supplied certificate chain and matching
  private key. A client must validate the chain and the DNS or IP identity it
  used to connect. Private trust roots may be supplied by the operator; clients
  must not offer a normal insecure verification bypass.
- Disable TLS 1.3 early data (0-RTT) for Runnel application operations. Accept
  no credentials or request bytes for dispatch until the full TLS handshake is
  complete. TLS 1.3 does not provide inherent cross-connection replay safety
  for early data, and Runnel has no protocol profile that marks operations
  safe to replay.
- TLS negotiation and pre-authentication exchange must remain within the
  configured connection, memory, and request-time bounds. An incomplete or
  over-limit pre-authentication exchange is closed without dispatch; a failed
  credential attempt cannot continue on the same unauthenticated connection.
- TLS termination belongs in Runnel. A TCP proxy may pass the TLS stream
  through. Terminating TLS at a proxy and forwarding credentials to Runnel over
  an unprotected connection is not a supported secure configuration.
- Keep the existing loopback bind as the local-development default. With no
  security configuration, an unauthenticated plaintext listener is allowed
  only on a loopback address. Any non-loopback application bind requires both
  TLS and authentication. An explicitly named insecure development/test
  override may allow a non-loopback plaintext listener for isolated examples,
  but does not create a protected deployment and must not be documented as a
  production setting. Supplying only one of TLS or authentication, or invalid
  secure configuration, is a startup error; there is no plaintext fallback.
  Loopback mode trusts every local process and user able to reach the socket;
  it is not isolation for a shared host.
- On a secured connection, establish one authenticated role for the lifetime
  of that connection before accepting any ordinary protocol operation,
  including `Health`. A missing, unknown, or invalid credential is rejected
  before dispatch with one generic authentication failure or a connection
  close, then close the unauthenticated connection. Do not distinguish absent,
  unknown, malformed, or revoked credentials. The exact authentication
  exchange, response, and any frame or field encoding remain protocol
  decisions under the protocol-compatibility work; this ADR does not define
  them.

### Credentials and authorization

- Use opaque bearer credentials containing 256 bits of cryptographically
  generated randomness, encoded as unpadded base64url. Possession authenticates
  the credential; it is not a human password, certificate identity, or
  self-describing authorization token.
- The server reads a runtime-only policy file. It contains non-secret
  credential IDs, SHA-256 verifier digests of the tokens, and one fixed role
  per credential; it does not contain the raw token. Clients receive the raw
  token from a protected runtime secret file or secret provider. The broker
  policy and client token must not be committed, embedded in an image, stored
  under broker data, passed as command-line values or environment variables,
  printed in diagnostics, or included in `Debug` output or logs. File access
  is limited to the broker identity and intended client. Invalid, unreadable,
  duplicate, or incomplete policy is a startup failure before the listener
  accepts connections.
- The reusable Rust client must accept the token through a secret-safe config
  path that is neither `Copy` nor capable of revealing it through `Debug`.
  Clients validate the server against system trust roots or operator-supplied
  roots and the DNS/IP name they dial. `runnelctl` reads a protected credential
  file by path; no client or CLI accepts a token as an argument or environment
  variable.
- Define two fixed roles and no custom permission language:

  | Role | Allowed operations |
  | --- | --- |
  | `application` | Publish, binary publish, publish batch, poll, grouped poll, replay, acknowledgement, grouped acknowledgement, and inspect consumer policy. |
  | `operator` | Every current application-protocol operation, including stream creation, consumer-policy configuration, and protocol `Health`. |

- Every credential maps to exactly one of these roles. Authorization is
  applied to each parsed request before engine dispatch. A denied operation
  returns a stable authorization-denied result without revealing stream or
  consumer existence and without invoking the engine. Every protocol variant
  must be classified explicitly; an unclassified future operation is denied
  until its role requirement is decided.
- Both roles apply to every stream and consumer. Credentials do not carry
  per-resource grants, tenants, quotas, or ordering privileges. Every
  `application` credential can read, publish, replay, and acknowledge across
  the broker's streams. `operator` credentials can also create streams, change
  consumer policy, and inspect broker health. Anyone needing isolation between
  mutually untrusted applications needs a different product boundary.
- Local-development mode without a credential file retains the current
  all-operations loopback behavior. A configured secure listener always
  authenticates, including loopback connections. Authentication failures are
  generic; authorization denials are explicit. Neither outcome is an ambiguous
  engine result because dispatch did not occur. If a client disconnects after
  a request may have reached dispatch, existing unknown-outcome rules apply;
  the client never replays a publish or acknowledgement automatically.

### Operations, rotation, and deployment scope

- Permit multiple active credential entries for either fixed role so token
  rotation can overlap. Add a new credential verifier and restart the server;
  move clients to its raw token; then remove the old verifier and restart
  again. Restart terminates persistent connections. No hot reload, automatic
  token expiry, issuing service, or zero-downtime rotation is promised.
- Replace the server certificate and key as a pair and restart the server.
  During planned CA changes, clients may trust both roots while the leaf is
  replaced. Certificate reload, revocation distribution, and client
  certificate authentication are deferred.
- The token policy is a local runtime input; it is not replicated through the
  engine. This contract is a single-node security guarantee. If an operator
  enables it on multiple clustered client listeners, every served replica
  must receive the same policy version and clients must be routed only to
  replicas with consistent credentials and roles. Runnel currently provides
  no safe rolling policy update or cluster-wide policy consistency mechanism;
  the clustered deployment must not be called secured on the basis of this
  ADR. Peer authentication, peer TLS, membership identity, and peer policy
  remain separate decisions.
- The HTTP listener remains a separate operations surface. In the current
  implementation `/health/live`, `/health/ready`, and `/metrics` all use one
  cleartext, unauthenticated listener. The loopback default is not enough to
  describe the Kubernetes manifest: it binds `0.0.0.0`, and the probes and
  Services make port 8080 reachable on the pod network. Until HTTP route
  security or separate listeners are implemented, deployments must restrict
  that port to trusted health-probe sources and an explicitly authorized
  monitoring path, and must not expose it to public or otherwise untrusted
  clients. Readiness exposes stream count and storage bytes; metrics reveal
  operational load and cluster state. This ADR does not claim that the
  current Kubernetes example satisfies that boundary.
- Security events may record bounded event and reason codes. Neither logs nor
  metrics may contain token values, verifier digests, payloads, keys, stream or
  consumer labels, or untrusted credential IDs. Metrics use fixed,
  low-cardinality labels only. HTTP access controls remain a deployment
  responsibility until a separate implementation closes that gap.

## Rationale

TLS at the broker protects application traffic to the point where Runnel
checks credentials and avoids a trusted-proxy identity propagation path. A
random bearer token keeps client identity setup small; a one-way verifier
avoids storing a replayable raw client credential in the broker policy file.
Two fixed roles give ordinary clients data operations while keeping topology,
consumer policy, and health inspection behind operator credentials. This
protects operations on an untrusted network without adding an external IdP,
PKI enrollment, or a policy language that small-team operators must learn.

The TLS version constraint follows the current IETF default for new protocols
and can be implemented with a maintained Rust TLS library. Kafka's and NATS's
more permissive defaults reflect their established clients; adopting their
1.2 compatibility here without any supported older Runnel client would add a
second version path without current deployment evidence. Revisit that limit
before publishing non-Rust clients or if an adopter demonstrates a real
TLS-1.2-only requirement. The current TLS 1.3 specification describes weaker
replay guarantees for 0-RTT; Runnel's mutating operations cannot use early data
without an explicit replay-safe protocol profile. The extra handshake round
trip is accepted, and no performance claim is made.

The HTTP operations listener is intentionally not presented as protected by
the client credential. Giving it a separate security model now would require
designing probe bypasses, scrape identity, and route binding. A private
deployment network is a bounded first operating condition, but the current
development manifest does not enforce it; deployment isolation and later HTTP
hardening remain release work.

## Consequences

- A non-loopback application listener without complete TLS and auth
  configuration will stop starting. This intentionally changes today's
  permissive remote-bind behavior; the JSON protocol has no compatibility
  guarantee.
- The runtime needs TLS stream handling, policy validation, connection
  authentication, an exhaustive role check before dispatch, safe secret
  loading, and client/CLI credential and trust configuration. These are not
  implemented by this decision.
- Operators need one runtime token policy and at least one `operator`
  credential for administration. Ordinary `application` credentials cannot
  create streams, change consumer policy, or inspect broker health.
- All application credentials retain access to all stream and consumer data.
  Credential IDs permit independent revocation but not per-application
  authorization. Shared-role access is a conscious non-multi-tenant boundary.
- TLS 1.3 only excludes older clients. Future language bindings and deployments
  need an interoperability check before claiming they can use the secured
  listener. Disabling 0-RTT also gives up early-data latency savings.
- The HTTP routes and clustered peer listener remain separate security gaps.
  Client security does not imply peer security, cluster safety, or safe
  external metrics exposure.
- This is design evidence, not a performance claim. A runtime implementation
  may report TLS handshake and persistent-connection costs only with
  representative measurements.

## Alternatives considered

- **One shared token with all protocol permissions:** smallest configuration,
  but one client credential would also create streams, alter consumer policy,
  and inspect broker health. The two fixed roles retain a basic operator
  boundary without adding per-resource administration.
- **Named principals with exact stream/consumer ACLs:** enables least privilege
  between applications, but adds a policy matrix and operational surface for a
  product not initially aimed at multiple untrusted tenants. Defer until a
  concrete use case requires resource-level isolation.
- **Mutual TLS, OAuth/OIDC, LDAP, or managed workload identity:** useful where
  an existing identity provider or PKI is an explicit deployment dependency;
  certificate issuance, mapping, refresh, external availability, and identity
  lifecycle are not present in the current one-node product.
- **Terminate TLS at a proxy:** common in web deployments, but it adds a trusted
  hop and requires authenticating the proxy and preserving client identity to
  Runnel. TLS pass-through is allowed; proxy termination is not the initial
  contract.
- **Support TLS 1.2 alongside 1.3 initially:** Kafka and NATS demonstrate the
  utility for older clients. RFC 9852 permits 1.2 as a non-default option when
  deployment concerns warrant it; Runnel has no existing deployed client
  requiring it. Revisit only with such evidence.
- **Authenticate HTTP health and metrics in this slice:** would require
  separating unauthenticated probe access from scrape credentials or splitting
  listeners. This remains a concrete deployment risk and implementation
  follow-up rather than being silently covered by client authentication.

## Implementation evidence required

Before describing a secured build as ready for use, add protocol and
real-server coverage for:

- default loopback development access and startup rejection of non-loopback
  plaintext/no-auth binds; explicit development override behavior;
- successful TLS 1.3 client connection, rejection of TLS 1.2 and plaintext at
  the secure listener, trusted CA loading, certificate/name mismatch, and
  proof that neither the TLS library nor the server accepts 0-RTT application
  operations; incomplete handshakes, oversized pre-authentication input, and
  repeated failed authentication remain within configured resource/time
  bounds;
- valid, missing, unknown, and malformed credentials; all role-permitted
  operations; application-role denials for stream creation, consumer-policy
  changes, and protocol health before engine dispatch; and exhaustive handling
  when protocol operations are added;
- invalid or unreadable policy/certificate files failing before listener
  acceptance, and proof that tokens/digests never appear in client/server
  debug output, logs, errors, or metrics;
- overlapping credentials across a restart, successful client migration,
  retired-token rejection after restart, connection interruption, and explicit
  client handling of requests whose outcome may already be unknown;
- TLS certificate replacement and client trust-root overlap across a restart;
- HTTP route access restrictions in each supported deployment. In Kubernetes,
  test that health probes and the designated scraper work while untrusted
  namespaces/ingress cannot reach port 8080. Exercise any required NetworkPolicy
  with the supported CNI rather than treating its YAML presence as proof;
- if security configuration is supported in clustered mode, verify consistent
  role/token policy on every client-serving replica during startup, restart,
  and rotation. Otherwise reject or clearly disable the claimed cluster-secure
  mode until peer security and replica coordination are designed.

No runtime test is applicable to this docs-only decision change. The tests
above are release gates for implementation. The existing
[single-node deployment backlog outcome](../backlog.md#make-the-single-node-deployment-ready-for-real-use)
remains open until code, actual deployment isolation, client ergonomics,
documentation, and these tests are complete. The separate
[cluster peer security investigation](../research/cluster-peer-transport-security.md)
does not become accepted by this decision.
