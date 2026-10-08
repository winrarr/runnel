# Kubernetes development cluster

`runnel.yaml` is an illustrative three-node deployment for exercising the
current static Multi-Raft backend. It is a development deployment, not a
production chart or an availability, security, backup, capacity, or upgrade
promise. It assumes a Kubernetes context that can provision three independent
`ReadWriteOnce` persistent-volume claims and can resolve the headless Service
names used by the broker.

> **Development security warning:** the manifest sets
> `--insecure-development-listen`, so the broker Service on port 4222 accepts
> unauthenticated plaintext. Use it only in a trusted, isolated development
> cluster. Never use this manifest on production or untrusted networks. The
> HTTP health and metrics listener on port 8080 is also cleartext and
> unauthenticated.

Every Raft node requires static peer mutual TLS credentials before it can
start. The manifest passes the peer credential file paths but does not create
or mount credentials. Supply an overlay or secret provider that projects the
files below `/var/run/runnel/peer-tls` separately into each pod before
applying it; without those files, startup fails closed and the probes remain
unready.

Build the `runnel:dev` image and make it available to the cluster, then apply
the manifest:

```text
kubectl apply -f deploy/kubernetes/runnel.yaml
kubectl get pods -l app.kubernetes.io/name=runnel -w
```

The manifest has no namespace, image registry, `StorageClass`, or cluster
credentials. Those are deliberately left to the development cluster. Verify
the current context and inspect the claims before sending application traffic:

```text
kubectl config current-context
kubectl get pods,pvc -l app.kubernetes.io/name=runnel
```

## Health and traffic routing

The client Service is `runnel:4222`. The same Service exposes the HTTP port as
`runnel:8080`; the headless Service exposes stable peer names and also selects
the HTTP and broker ports.

The probes have deliberately different meanings:

- `/health/live` is a liveness-only HTTP process check. It returns `200` when
  the HTTP handler responds; it does not check the engine, Raft leadership,
  quorum, replication progress, or disk space. The startup and liveness probes
  both use this endpoint.
- The startup probe allows 60 failures at five-second intervals (five
  minutes). Runnel opens the persistent engine before it starts the HTTP
  listener, so this window covers slow startup and recovery from the existing
  volume. It is a Kubernetes restart timeout, not a broker recovery guarantee.
- `/health/ready` performs a bounded one-second engine health check. The Raft
  engine reports ready only after the cluster is initialized and the metadata
  group has an elected leader; it returns `503` during shutdown or when the
  check fails or times out. Readiness does not prove that this pod is the
  leader, that every data group is ready, that replication lag is bounded, or
  that a durable publish will succeed. Followers can be ready and forward
  supported client operations to the appropriate leader.

Kubernetes removes a pod that fails readiness from Service endpoints, but the
manifest does not add a write-path health check or a separate operator-facing
cluster-health endpoint. Repeated startup-probe failures should be investigated
as recovery or storage problems; increasing the five-minute window is only
appropriate when the larger recovery time is understood.

## Persistence and identity

Each StatefulSet replica mounts `/var/lib/runnel` from its own
`data` claim. The claim requests 10 GiB, uses `ReadWriteOnce`, and leaves
`storageClassName` unset, so the cluster's default `StorageClass` chooses the
provisioner. The pod ordinal becomes the configured node ID (`0`, `1`, or `2`)
and the broker uses the stable names `runnel-0.runnel-headless` through
`runnel-2.runnel-headless` for its static peer list.

The claims preserve node-local state across an ordinary pod restart or
rescheduling when the storage provisioner can reattach them. They are not a
backup or restore mechanism. This manifest does not define volume snapshots,
backup retention, disk-full handling, or a supported migration from local
engine data. A claim must not be reused for another cluster identity or node
without an explicit recovery or replacement procedure; deleting or replacing a
claim can remove the only local copy held by that replica.

The command line uses the server's default cluster name, `runnel`. Keep that
identity and the pod-to-node mapping stable for the lifetime of these claims.

## Peer transport credentials

The peer listener requires TLS 1.3 mutual authentication before it reads any
Raft, snapshot, forwarding, or data-group setup frame. Each pod needs these
files at the paths already passed by the manifest:

| Path | Contents |
| --- | --- |
| `/var/run/runnel/peer-tls/ca.pem` | Explicit peer trust bundle for this cluster |
| `/var/run/runnel/peer-tls/tls.crt` | This pod's leaf certificate and any required intermediates |
| `/var/run/runnel/peer-tls/tls.key` | Matching private key for this pod only |

Issue a distinct key and certificate per ordinal. The certificate SAN must
bind the configured unsigned node ID and exact cluster name using the profile
in [ADR 0032](../../docs/decisions/0032-static-cluster-peer-mutual-tls.md); a
certificate for one ordinal cannot be reused by another. Do not project a
Secret containing every node's private key into every pod. Kubernetes Secret
objects do not provide a safe ordinal-to-key selection in this shared
StatefulSet template, so use a per-pod Secret/CSI projection or an overlay
that selects exactly one leaf and key for each ordinal. Keep the trust bundle
and private keys out of the image, command arguments, and `/var/lib/runnel`.
Mount credential material read-only with access restricted to the Runnel
process.

This is peer-only TLS. The broker Service on port 4222 and the HTTP
health/metrics listener on port 8080 remain plaintext and need separate
network isolation or a future public-listener security design. Peer TLS does
not make those listeners private.

This server does not communicate with earlier plaintext peer processes and
has no legacy-peer or mixed-transport mode. A cluster transition must be a
coordinated cutover: stop every old process before starting the credentialed
binary on all nodes with the same static membership. Preserve the durable data
directories; peer transport security does not change their recovery format or
provide a storage migration path. Leaf renewal and CA replacement require
process restarts because files are loaded only at startup. For CA replacement, first distribute a
bundle containing both old and new anchors, restart one pod at a time while
preserving quorum, replace leaves one at a time, then remove the old anchor
and restart one pod at a time again. Verify peer traffic and readiness after
each step. The manifest does not implement rotation automation, online
revocation, or rolling peer upgrades.

The 10 GiB request is a storage allocation request, not a broker retention
limit or a guarantee of available free space. The broker has no retention or
capacity settings in this manifest, so retained state can consume the claim.
The clustered `runnel_storage_bytes` metric is a logical sum of stored message
keys and payloads, not physical PVC usage; it excludes filesystem, journal,
snapshot, and other storage overhead.

## Disruption and shutdown

`podManagementPolicy: Parallel` allows the three static members to start
without waiting for ordinal order. The `runnel` PodDisruptionBudget selects the
same `app.kubernetes.io/name: runnel` label as the StatefulSet and sets
`minAvailable: 2`. The stable `policy/v1` API used here is available in
Kubernetes 1.21 and newer. When all three pods are Ready, the budget permits
one healthy member to be voluntarily evicted while two Ready members remain;
further healthy evictions are blocked until availability recovers. Check
`kubectl get pdb runnel` and confirm one disruption is allowed before starting
a node drain. If fewer than two pods are Ready, the budget does not allow
another healthy member to be evicted.

Kubernetes counts Pod `Ready` conditions for a disruption budget. Here,
readiness means that Raft metadata is initialized and has an elected leader;
it does not confirm that every data group can make progress, that replication
is caught up, or that two Ready members necessarily form a functioning quorum.
The budget therefore limits planned evictions based on the deployment's
readiness signal, but does not itself prove Runnel quorum health.

A PodDisruptionBudget is enforced for voluntary requests through the Kubernetes
Eviction API, such as a node drain. Direct deletion of a pod or its owning
workload, as well as workload-controller rolling updates, can bypass the
budget. Changing the StatefulSet replica count or the broker's static peer
list is unsupported; the PDB does not make a membership change safe. A
not-Ready member may also block a drain while the budget is already below its
minimum under Kubernetes' default unhealthy-pod eviction behavior. The
manifest has no pod anti-affinity or topology spread constraint, so multiple
pods may share one worker node. Involuntary worker, zone, storage, or network
failures cannot be prevented by the budget and are outside this manifest's
guarantees. These limits follow the [Kubernetes disruption budget behavior](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/).

On `SIGTERM` or `SIGINT`, Runnel marks readiness false, stops accepting new
broker connections, and drains existing broker and HTTP work for up to 25
seconds. The 30-second termination grace period leaves five seconds of margin
for process exit. It does not transfer leadership, make in-flight client
outcomes known, or protect against a forced kill.

## Control plane and peer membership

The broker does not use the Kubernetes API for Raft membership, elections,
forwarding, or recovery. Membership is the three-node list in the container
arguments, and peers communicate through the headless-Service DNS names. A
Kubernetes control-plane outage therefore does not itself change committed
Raft state in already-running pods, but it also cannot be expected to repair,
reschedule, replace, update, or converge Service endpoints for them. The
manifest has no tested control-plane-outage procedure; do not treat continued
data-plane traffic during such an outage as a supported availability
guarantee.

## Resources and security assumptions

The container requests 100 millicores and 128 MiB per pod, with limits of one
CPU and 1 GiB memory. These are illustrative development values, not measured
operating limits. CPU throttling, an out-of-memory kill, slow storage, or a
full PVC can prevent useful progress, and readiness does not detect all of
those conditions. No ephemeral-storage request or limit is set.

The command line leaves the server's current per-pod defaults in place: 1,024
client connections, 1 MiB request frames, 256 in-flight requests, 30-second
request timeouts, and a 30-second acknowledgement timeout. No maximum delivery
attempt count is configured. These bounds are per pod rather than cluster-wide
and are not a substitute for capacity planning.

The pod runs as a non-root user with the default runtime seccomp profile, no
Linux capabilities, privilege escalation disabled, and a read-only root
filesystem. Application TLS and bearer authentication are available for the
local engine, but the Raft backend rejects that configuration until credential
policy is consistent across replicas. This manifest therefore explicitly
allows unauthenticated plaintext broker traffic on port 4222. The HTTP Service
on port 8080 remains cleartext and unauthenticated. Keep both Services inside
a trusted, isolated development network; do not expose them publicly or to
untrusted namespaces.
filesystem. Application TLS and bearer authentication are available for the
local engine, but Raft rejects application credentials until policy is
consistent across replicas. The broker Service therefore accepts unauthenticated
plaintext on port 4222, and the HTTP Service on port 8080 remains cleartext and
unauthenticated. This manifest does not automate peer credential rotation;
follow the coordinated restart and trust-overlap procedure above. Keep both
Services inside a trusted, isolated development network; do not expose them
publicly or to untrusted namespaces.

## Upgrade and rollback

The StatefulSet uses `OnDelete`, so editing the image reference does not
automatically replace running pods. The image is the mutable `runnel:dev` tag
with `IfNotPresent`; cached nodes can therefore continue to run an older image.
Use an immutable tag or digest when an image identity matters, and explicitly
delete or restart only one pod at a time after confirming that at least two
members remain available. `OnDelete` does not make mixed binary versions
compatible: a crash or reschedule after the template changes can start a new
version while other pods still run the old one. PodDisruptionBudgets do not
constrain workload-controller rolling updates, so the PDB is not an upgrade
safety mechanism.

There is no supported rolling-upgrade, downgrade, or rollback procedure for
this deployment. Mixed-version Raft peers are unsupported, and no tested
upgrade procedure exists. Clustered storage layout and protocol-version
compatibility remain under development; a new binary can fail closed on an
unsupported volume, and reverting a binary after it has written incompatible
state is not defined. Use a disposable cluster for upgrade experiments and
preserve any claims needed for recovery before changing the image. `kubectl
rollout restart` is not a compatibility test or an upgrade procedure.

## Metrics and monitoring

Every pod serves Prometheus-compatible text at `http://<pod>:8080/metrics`.
The manifest does not install a `ServiceMonitor`, `PodMonitor`, scrape
annotations, TLS, or metrics authentication. Scraping `runnel:8080` through
the readiness-filtered client Service load-balances requests and does not give
a stable per-pod time series; configure per-pod discovery or an equivalent
monitoring resource outside this manifest if metrics are needed.

Current metrics cover process uptime, broker request counts and latency
buckets, connection and request admission, traffic bytes,
publish/delivery/acknowledgement counters, in-flight deliveries, redelivery
and dead-letter totals, health check failures, logical storage bytes, and
clustered snapshot lifecycle counters. `runnel_process_uptime_seconds` is a
label-free gauge based on a monotonic clock; it resets when the broker process
restarts and remains available during a stalled engine health check. Metrics
do not expose per-group leadership, replication progress or lag, forwarding or
peer error state, consumer lag, reclaimable storage, PVC free space, CPU or
memory pressure, or queue depth. A metrics scrape performs the bounded engine
health check, but returns `200` with process and admission metrics when that
check fails and omits unavailable engine-derived samples; metrics are not an
independent liveness path.

For the current implementation and its limitations, see [the operational
telemetry debt item](../../docs/tech-debt.md#td-006-operational-telemetry-remains-incomplete)
and [the clustered deployment backlog outcome](../../docs/backlog.md#make-the-clustered-deployment-operable).
