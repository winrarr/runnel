# Application-client authentication and transport security

- Status: source-backed research supporting the accepted first client-security contract in [ADR 0035](../decisions/0035-first-application-client-security.md); runtime behavior is not implemented
- Observed baseline: `5d3625d9c63903809e2a026c997522cf182786e5`
- Last reviewed: 2026-10-06
- Scope: application-protocol connections and the adjacent HTTP health/metrics surface; peer transport identity is covered separately

This note distinguishes current source behavior, external reference designs,
inferences for Runnel, and the chosen planning outcome. It does not claim that
Runnel currently authenticates clients, encrypts connections, or limits access.
The protocol remains provisional and has no backward-compatibility promise.

## Observed Runnel behavior

At the baseline revision:

- The server binds the JSON-lines client listener directly as a `TcpListener`,
  defaulting to `127.0.0.1:4222`. It has no TLS, authentication, or
  authorization configuration. A parsed request goes from the connection
  loop to server dispatch and then to the engine ([bootstrap](../../crates/runnel-server/src/bootstrap.rs),
  [connection handling](../../crates/runnel-server/src/connection.rs),
  [dispatch](../../crates/runnel-server/src/dispatch.rs)).
- The reusable client uses Tokio `TcpStream::connect`; its current
  `ClientConfig` is `Copy` and `Debug` and contains only timeouts and a response
  limit. The CLI connects through the same client and has no credential or TLS
  options ([client](../../crates/runnel-client/src/lib.rs),
  [CLI](../../crates/runnel-cli/src/main.rs)). No TLS library is currently a
  workspace dependency.
- The protocol includes stream creation, publish and publish-batch, poll,
  grouped poll, replay, consumer configuration and inspection, acknowledgement,
  grouped acknowledgement, and `Health`. Dispatch currently sends each to the
  engine without an authorization check ([protocol types](../../crates/runnel-protocol/src/lib.rs),
  [dispatch](../../crates/runnel-server/src/dispatch.rs)).
- The separate HTTP listener defaults to `127.0.0.1:8080`, but all three
  routes share it without authentication or TLS. `/health/live` reports the
  process handler, `/health/ready` exposes stream count and logical storage
  bytes when the bounded health check succeeds, and `/metrics` exposes
  operational counters and gauges ([bootstrap](../../crates/runnel-server/src/bootstrap.rs),
  [observability](../../crates/runnel-server/src/observability.rs)).
- The development Kubernetes manifest binds the client and HTTP listeners to
  `0.0.0.0`, exposes port 8080 through both the regular and headless Services,
  and uses `/health/live` and `/health/ready` probes against that pod listener.
  The same cleartext HTTP listener serves `/metrics`. The manifest has no
  NetworkPolicy, TLS, or HTTP authentication and is explicitly development
  only ([manifest](../../deploy/kubernetes/runnel.yaml),
  [deployment notes](../../deploy/kubernetes/README.md#health-and-traffic-routing),
  [metrics notes](../../deploy/kubernetes/README.md#metrics-and-monitoring)).
- Runnel targets small teams without a dedicated messaging operations group,
  and does not initially target a hosted multi-tenant service
  ([product fit](../product-fit.md)). The single-node readiness outcome
  requires runtime-supplied secrets, optional client authentication and
  authorization, and documented TLS; implementation and repeatable security
  tests remain open ([backlog](../backlog.md#make-the-single-node-deployment-ready-for-real-use)).

These observations describe different network boundaries. TLS/authentication
on the application listener cannot secure the HTTP routes or Raft peer socket.
Likewise, a loopback default is a local development convenience, not a secure
remote-bind policy. Processes sharing the host can currently use all client
operations.

## Threat model and limits

The first application boundary protects message data and mutating client
operations from unauthenticated network clients and passive or active network
observers. It also addresses accidental non-loopback exposure by making remote
client binding conditional on security configuration. The broker host,
operator account, runtime-mounted key and policy files, and applications given
credentials remain trusted. Loopback development mode does not isolate local
users or processes; authenticated applications are not isolated from each
other by stream; transport security does not protect stored data, host
backups, the peer listener, or the HTTP routes. TLS/authentication alone does
not prevent connection floods or other volumetric denial of service.

## Reference designs and what transfers

| Primary source | Documented design | Relevance to Runnel | Limit when applied here |
| --- | --- | --- | --- |
| IETF [RFC 6750, Bearer Token Usage](https://www.rfc-editor.org/rfc/rfc6750.html) | A bearer is usable by any party possessing the token; the RFC requires protection in storage and transit and defines bearer use for HTTP. | Supports treating a static token as a secret and never transmitting it before TLS or logging it. | It specifies HTTP authorization, not Runnel's connection protocol, token lifetime, policy file, or authorization model. |
| IETF [RFC 9852, New Protocols Using TLS Must Require TLS 1.3](https://www.rfc-editor.org/rfc/rfc9852.html) | This 2026 Best Current Practice says a new TLS-using protocol must require TLS 1.3; TLS 1.2 may be added as a non-default option when deployment concerns justify it. | Runnel's client protocol is new and has no deployed compatibility contract, so TLS 1.3 only is a reasonable initial profile. | The recommendation does not establish that every future application client can use TLS 1.3; future non-Rust bindings need compatibility evidence. |
| IETF [RFC 9846, TLS 1.3](https://www.rfc-editor.org/info/rfc9846/) | The current TLS 1.3 specification says 0-RTT has weaker replay guarantees than 1-RTT and application protocols need a profile defining safe early-data use. | Runnel has mutating publish, acknowledgement, stream, and consumer operations but no early-data profile, so the broker protocol should reject/disable early data. | Disabling 0-RTT gives up its handshake latency savings; resumption without early application data remains possible. |
| Rustls [crate documentation](https://docs.rs/rustls/latest/rustls/) | Rustls implements TLS 1.2 and 1.3; the application can restrict the configured version to TLS 1.3. | Shows TLS 1.3 is feasible for a Rust client/server implementation without custom cryptography. | Rustls is not currently a Runnel dependency, and selecting a crate or configuration is implementation work. |
| NATS [TLS guide](https://docs.nats.io/learn/security/encryption), [TLS configuration reference](https://github.com/nats-io/nats.docs/blob/master/running-a-nats-service/configuration/securing_nats/tls.md), [authorization guide](https://docs.nats.io/learn/security/authorization), and [client ecosystem](https://docs.nats.io/concepts/ecosystem) | NATS treats client, monitoring, and cluster TLS as separate connection boundaries. It distinguishes encryption/server verification from client authentication, and authorization from identity. Its server configuration currently defaults to TLS 1.2 minimum and officially maintains clients for Go, JavaScript, Python, Java, Rust, .NET, and C. | Supports keeping public-client, peer, and operations security independent, and keeping authentication distinct from grants. Its broader ecosystem demonstrates the compatibility value of TLS 1.2 for existing clients. | NATS has server-first capabilities, accounts, and subject permissions. Runnel has one Rust client path, no accounts, and stream/consumer operations rather than subjects. |
| Apache Kafka [security overview](https://kafka.apache.org/43/security/), [TLS configuration](https://kafka.apache.org/43/configuration/broker-configs/#brokerconfigs_ssl.enabled.protocols), and [authorization/ACLs](https://kafka.apache.org/43/security/authorization-and-acls/) | Kafka separates TLS transport, connection authentication, and operation authorization; its current broker TLS configuration defaults to TLS 1.2 and TLS 1.3, with 1.3 preferred. It offers resource ACLs and several authentication mechanisms. | Confirms the value of explicit authorization beyond successful authentication and documents the compatibility benefit of supporting both TLS versions. | Kafka's JVM and third-party client ecosystem, resource ACL administration, and multi-tenant use are substantially broader than Runnel's current product and operator model. |
| IETF [RFC 9525, Service Identity in TLS](https://www.rfc-editor.org/rfc/rfc9525.html) | Clients verify a reference identity against an appropriate certificate identifier; DNS names and IP addresses are distinct identity types. | Supports requiring clients to verify the broker certificate name they actually connect to. | It does not define Runnel's certificate provisioning, trust store, or hostname configuration interface. |

The TLS version choice is deliberate, not a statement that TLS 1.2 is unsafe
in every deployment. Kafka and NATS preserve a 1.2 path for wider existing
client compatibility. Runnel has no such compatibility constraint at this
baseline, while RFC 9852 gives a current protocol-design default. The cost is
that older TLS-1.2-only clients cannot use the secured listener; add TLS 1.2
only if supported-client evidence demonstrates a real deployment need.

## Inferences and accepted disposition

**Inference:** A static bearer credential over a channel that authenticates the
broker is implementable without an identity service, directory, password
database, or certificate enrollment process. A cryptographically random token
is appropriate as a secret, not as a human password. Multiple token verifier
entries mapped to one of two fixed roles permit an operator-controlled overlap
rotation without live reload. The accepted contract stores verifier digests in
a runtime-only policy file and sends raw tokens only from protected client
secret sources after TLS succeeds.

**Inference:** Runnel's non-multi-tenant audience makes exact per-stream and
per-consumer ACLs optional for the first protection boundary. A coarse
`application` role still needs a bounded set of data-plane operations, while an
`operator` role can perform all current operations. Every application
credential can affect every stream; this is a material limitation, not tenant
isolation. Requiring operators to manage per-resource grants would import
configuration and lifecycle concepts that the current product does not yet
have.

**Accepted contract:** [ADR 0035](../decisions/0035-first-application-client-security.md)
requires broker-terminated TLS 1.3 and connection authentication on every
non-loopback application listener. It keeps unauthenticated plaintext access
only for the loopback local-development default; incomplete secure
configuration fails closed. Two fixed roles distinguish ordinary message
operations from broker configuration and health inspection. The existing
cleartext HTTP operations listener remains outside the application credential
boundary and must be isolated to trusted probes and monitoring at deployment
time. This does not secure the peer listener or establish a clustered security
guarantee.

The HTTP boundary has a concrete gap: the development Kubernetes example
exposes unauthenticated health and metrics routes on all pod interfaces and
does not install network filtering. Its current probes depend on remote
reachability of those endpoints. That example must not be treated as a secure
deployment pattern. A production deployment needs an allowlist for health
probe sources and an authorized private metrics path; exposing the shared port
through a public or untrusted ingress is outside the accepted contract. A
future runtime change may split route listeners or protect metrics directly,
but is not required to implement client authentication.

## Hypotheses, risks, and deferred work

- **Hypothesis:** Two fixed roles and a small file-managed token list provide
  enough separation for the initial small-team deployment. There is no user
  study of application/operator separation or multi-application credential
  administration.
- Shared-role credentials do not partition data. An application token can
  publish, read, replay, and acknowledge data across all streams; a leaked
  token remains valid until an operator removes it and restarts the process.
- The server TLS listener, authentication exchange, secure client config, and
  role gate do not exist yet. The decision's local-development bind policy
  changes current behavior for users who bind the listener remotely without
  credentials.
- Restart-based credential and certificate changes interrupt persistent
  clients. If client requests may have reached dispatch, reconnect does not
  make their outcomes known; current publish/ack ambiguity semantics still
  apply.
- The server does not distribute client credentials or TLS trust material
  across replicas. Applying the contract to clustered client listeners needs
  identical runtime policy on every served replica and a coordinated update;
  it is not a safe rolling-update procedure. Peer mTLS, cluster identity, and
  peer-policy rotation remain separate decisions.
- HTTP readiness and metrics remain unauthenticated cleartext. No current test
  proves that an external deployment restricts access to port 8080. The
  application TLS boundary does not mitigate that exposure.
- Authentication failure logging and metrics must stay bounded and omit
  token values, digests, message data, resource names, and untrusted principal
  labels. Implementation tests must verify this behavior.

**Disposition:** This is a near-term security outcome already present in the
single-node backlog, so no new tracker item is needed. The existing backlog
remains open for TLS/authentication runtime code, HTTP and deployment
isolation, real-server tests, operational documentation, and recovery and
rotation coverage. The separate [cluster peer security research](cluster-peer-transport-security.md)
continues to cover peer identity and transport; no peer-security conclusion is
inferred here.

## Source retrieval

The external references above were reviewed on 2026-10-06. RFC text is linked
through the RFC Editor; product behavior is linked to first-party NATS, Kafka,
and Rustls documentation. The observed Runnel behavior is pinned to the
repository baseline recorded at the top of this note.
