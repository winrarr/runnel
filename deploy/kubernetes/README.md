# Kubernetes development cluster

The manifest starts three Runnel pods with independent persistent volumes and broker-owned static membership. It is a development deployment for validating the current Multi-Raft backend, not a production chart.

Build the image and make it available to the cluster, then apply:

```text
kubectl apply -f deploy/kubernetes/runnel.yaml
kubectl get pods -l app.kubernetes.io/name=runnel -w
```

The client Service is `runnel:4222`; readiness only becomes healthy for initialized nodes with an elected leader. The headless Service provides stable peer names used by the broker. The Kubernetes control plane is not part of Raft correctness after the pods and volumes have been created.

This deployment currently assumes the cluster is initialized with pod 0 as the bootstrap node. Do not reuse a volume for a different cluster identity without first applying an explicit recovery or replacement procedure.
