# Kubernetes development cluster

The manifest starts three Runnel pods with independent persistent volumes and broker-owned static membership. It is a development deployment for validating the current Multi-Raft backend, not a production chart.

Build the image and make it available to the cluster, then apply:

```text
kubectl apply -f deploy/kubernetes/runnel.yaml
kubectl get pods -l app.kubernetes.io/name=runnel -w
```

The client Service is `runnel:4222`; readiness only becomes healthy for initialized nodes with an elected leader. The headless Service provides stable peer names used by the broker. The Kubernetes control plane is not part of Raft correctness after the pods and volumes have been created.

The startup probe checks `/health/live` every five seconds and allows 60 failures (five minutes) before Kubernetes restarts a pod. Runnel serves that endpoint only after opening the engine, so the window covers slow persistent-state recovery without weakening the readiness check. Five minutes is an operational timeout, not a broker recovery guarantee; increase it for substantially larger development volumes rather than treating repeated restarts as recovery.

The StatefulSet deliberately uses parallel pod management: all three static members must be able to start so the bootstrap node can form a quorum, while readiness still keeps the client Service closed until initialization and leadership succeed. Each replica receives an independent 10 GiB `ReadWriteOnce` claim from the cluster's default StorageClass because `storageClassName` is not set. Confirm that the cluster can provision that claim type before applying the manifest.

This deployment currently assumes the cluster is initialized with pod 0 as the bootstrap node. Do not reuse a volume for a different cluster identity without first applying an explicit recovery or replacement procedure.
