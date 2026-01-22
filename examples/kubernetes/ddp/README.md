# Distributed Data Parallel (DDP) on Kubernetes

This example demonstrates how to run PyTorch Distributed Data Parallel (DDP) training on Kubernetes using Monarch's `KubernetesJob` and the MonarchMesh CRD.

## Overview

The example:
- Provisions GPU-enabled worker pods using the MonarchMesh CRD
- Connects to the workers using `KubernetesJob`
- Runs a basic DDP training loop across multiple GPUs and hosts

## Prerequisites

- Kubernetes cluster with:
  - [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
  - GPU nodes with `nvidia.com/gpu` resources
  - NVIDIA device plugin deployed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created:
  ```bash
  kubectl create namespace monarch-tests
  ```

## Configuration

### Adjusting Resources

Edit `ddp_mesh.yaml` to match your cluster's GPU configuration:

```yaml
# Number of worker pods (hosts)
spec:
  replicas: 2  # Change to your desired number of hosts

# GPUs per pod
resources:
  limits:
    nvidia.com/gpu: 4  # Change to match available GPUs per node
  requests:
    nvidia.com/gpu: 4
```

Update the script arguments to match:

```bash
python kubernetes_ddp.py --num_hosts 2 --gpus_per_host 4 --mesh_name ddpmesh
```

Arguments:
- `--num_hosts`: Must match `spec.replicas` in YAML
- `--gpus_per_host`: Must match `nvidia.com/gpu` in YAML
- `--mesh_name`: Must match `metadata.name` in YAML

## Deployment

### 1. Deploy the MonarchMesh and Controller

```bash
kubectl apply -f manifests/ddp_mesh.yaml
```

### 2. Verify Pods are Running

```bash
# Check worker pods
kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker

# Check controller pod
kubectl get pods -n monarch-tests ddp-controller
```

Wait for all pods to show `Running` status.

### 3. Run the DDP Example

Copy and execute the script from the controller pod:

```bash
# Copy the script to the controller
kubectl cp kubernetes_ddp.py monarch-tests/ddp-controller:/tmp/kubernetes_ddp.py

# Get a shell into the controller
kubectl exec -it ddp-controller -n monarch-tests -- /bin/bash

# Inside the controller, run the DDP example
python /tmp/kubernetes_ddp.py --num_hosts 2 --gpus_per_host 4
```

Or run directly without shell:

```bash
kubectl exec -it ddp-controller -n monarch-tests -- python /tmp/kubernetes_ddp.py
```

## Expected Output

```bash
kubectl logs -n monarch-tests -l app.kubernetes.io/name=monarch-worker
```

Expected output:
```
self.rank=0 Initializing torch distributed
self.rank=1 Initializing torch distributed
...
self.rank=0 Finished initializing torch distributed
self.rank=0 Running basic DDP example
self.rank=0 local_rank=0
...
self.rank=0 Finished running basic DDP example
```

## Cleanup

```bash
kubectl delete -f manifests/ddp_mesh.yaml
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
│                                                             │
│  ┌──────────────┐                                           │
│  │  Controller  │  ← Runs KubernetesJob + DDP training      │
│  │     Pod      │    script                                 │
│  └──────┬───────┘                                           │
│         │                                                   │
│         │ Discovers pods via labels                         │
│         │ app.kubernetes.io/name=monarch-worker             │
│         │ monarch.pytorch.org/mesh-name=ddpmesh             │
│         ▼                                                   │
│  ┌──────────────┐  ┌──────────────┐                         │
│  │   Worker 0   │  │   Worker 1   │  ← MonarchMesh pods     │
│  │   (4 GPUs)   │  │   (4 GPUs)   │    with GPU resources   │
│  └──────────────┘  └──────────────┘                         │
│         │                  │                                │
│         └────── NCCL ──────┘                                │
│              (GPU-to-GPU)                                   │
└─────────────────────────────────────────────────────────────┘
```
