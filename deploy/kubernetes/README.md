# Kubernetes development cluster

`runnel.yaml` is an illustrative three-node deployment for exercising the
current static Multi-Raft backend. It is a development deployment, not a
production chart or an availability, security, backup, capacity, or upgrade
promise. It assumes a Kubernetes context that can provision three independent
`ReadWriteOnce` persistent-volume claims and can resolve the headless Service
names used by the broker.

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

The 10 GiB request is a storage allocation request, not a broker retention
limit or a guarantee of available free space. The broker has no retention or
capacity settings in this manifest, so retained state can consume the claim.
The clustered `runnel_storage_bytes` metric is a logical sum of stored message
keys and payloads, not physical PVC usage; it excludes filesystem, journal,
snapshot, and other storage overhead.

## Disruption and shutdown

`podManagementPolicy: Parallel` allows the three static members to start
without waiting for ordinal order. It does not provide quorum protection. The
manifest has no `PodDisruptionBudget`, pod anti-affinity, or topology spread
constraint, so voluntary disruption can remove multiple members and multiple
pods may share one worker node. Keep at least two members available and
disrupt one member at a time when testing this deployment. Involuntary worker,
zone, storage, or network failures are outside the guarantees of this
manifest.

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
filesystem. The broker and HTTP Service still have no TLS, authentication,
authorization, or credential rotation. Keep the Services inside a trusted
development network and do not expose them publicly without an external
security boundary.

## Upgrade and rollback

The StatefulSet uses `OnDelete`, so editing the image reference does not
automatically replace running pods. The image is the mutable `runnel:dev` tag
with `IfNotPresent`; cached nodes can therefore continue to run an older image.
Use an immutable tag or digest when an image identity matters, and explicitly
delete or restart only one pod at a time after confirming that at least two
members remain available. `OnDelete` does not make mixed binary versions
compatible: a crash or reschedule after the template changes can start a new
version while other pods still run the old one.

There is no supported rolling-upgrade, downgrade, or rollback procedure for
this deployment. Clustered storage layout and peer/protocol compatibility are
still under development; a new binary can fail closed on an unsupported
volume, and reverting a binary after it has written incompatible state is not
defined. Use a disposable cluster for upgrade experiments and preserve any
claims needed for recovery before changing the image. `kubectl rollout
restart` is not a compatibility test or an upgrade procedure.

## Metrics and monitoring

Every pod serves Prometheus-compatible text at `http://<pod>:8080/metrics`.
The manifest does not install a `ServiceMonitor`, `PodMonitor`, scrape
annotations, TLS, or metrics authentication. Scraping `runnel:8080` through
the readiness-filtered client Service load-balances requests and does not give
a stable per-pod time series; configure per-pod discovery or an equivalent
monitoring resource outside this manifest if metrics are needed.

Current metrics cover broker request counts and latency buckets, connection
and request admission, traffic bytes, publish/delivery/acknowledgement
counters, in-flight deliveries, redelivery and dead-letter totals, health
check failures, logical storage bytes, and clustered snapshot lifecycle
counters. They do not expose per-group leadership, replication progress or
lag, forwarding or peer error state, consumer lag, reclaimable storage, PVC
free space, CPU or memory pressure, or queue depth. A metrics scrape itself
performs the bounded engine health check and returns `503` when that check
fails, so metrics are not an independent liveness path.

For the current implementation and its limitations, see [the operational
telemetry debt item](../../docs/tech-debt.md#td-006-operational-telemetry-remains-incomplete)
and [the clustered deployment backlog outcome](../../docs/backlog.md#make-the-clustered-deployment-operable).
