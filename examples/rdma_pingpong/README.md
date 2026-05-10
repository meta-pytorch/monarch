# RDMA Pingpong

A two-node RDMA pingpong example that exchanges buffers between actors via `RDMABuffer` and reports per-iteration bandwidth. Supports `slurm`, `mast`, and `k8s` backends via the `--backend` flag.

## Slurm

```bash
python rdma_pingpong.py \
  --backend=slurm \
  --data_size_mb=1000 \
  --num_iterations=3
```

## Kubernetes

### Prerequisites

- A Kubernetes cluster with the [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created:
  ```bash
  kubectl create namespace monarch-tests
  ```
- Worker nodes that expose an `rdma/ib` device plugin and `nvidia.com/gpu` resources

### 1. Apply RBAC + driver pod

```bash
kubectl apply -f kubernetes_provision.yaml
```

### 2. Wait for the driver to be Ready

The driver `pip install`s `fire` and `xxhash` on startup; wait for its readiness probe:

```bash
kubectl -n monarch-tests wait --for=condition=Ready \
  pod/rdma-pingpong-driver --timeout=5m
```

### 3. Copy the script into the driver

```bash
kubectl -n monarch-tests cp rdma_pingpong.py \
  rdma-pingpong-driver:/tmp/rdma_pingpong.py
```

### 4. Launch

Worker pods provisioned by the MonarchMesh operator use the same image as the driver:

```bash
kubectl -n monarch-tests exec -it rdma-pingpong-driver -- \
  python -u /tmp/rdma_pingpong.py \
    --backend=k8s \
    --data_size_mb=1000 \
    --num_iterations=3
```

### 5. Cleanup

```bash
kubectl -n monarch-tests delete monarchmesh workers
kubectl delete -f kubernetes_provision.yaml
```

### Notes on cross-host fabric

The k8s backend attaches a hard `requiredDuringSchedulingIgnoredDuringExecution` pod anti-affinity keyed on the operator's `monarch.pytorch.org/mesh-name=workers` label with `topologyKey: kubernetes.io/hostname`. This forces the two replicas onto distinct nodes so the pingpong actually exercises the cross-host RDMA fabric instead of a same-HCA loopback path.

Requesting `rdma/ib: 1` only guarantees each pod gets an IB device; it does NOT guarantee the two pods can reach each other over IB. On clusters with multiple isolated IB fabrics, add a required `pod_affinity` term keyed on your provider's fabric label so the two replicas land on the same fabric. Without it, the example will fail.
