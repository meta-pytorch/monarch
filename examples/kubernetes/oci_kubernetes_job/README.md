# Running Monarch on OKE (OCI Kubernetes)

This directory contains examples for running Monarch on OKE (OCI Kubernetes). As example we use a simple Distributed Data Parallel Trainig (DDP) of a CNN on a publicly available play cards data set.

## Prerequisite: OKE Cluster with GPU Nodes

- Follow [official documentation to provision OKE Cluster](https://docs.oracle.com/en/engineered-systems/private-cloud-appliance/3.0-latest/oke/oke-cluster-create.html)
- Add GPU Nodes (optionally with RDMA support) to the cluster via [OCI AI Blueprints](https://github.com/oracle-quickstart/oci-ai-blueprints)

## Install MonarchMesh CRD and operator using Helm

```bash
helm repo add monarch-operator https://meta-pytorch.github.io/monarch-kubernetes

helm repo update

helm install monarch-operator monarch-operator/monarch-operator \
  --namespace monarch-system \
  --create-namespace
```

## Install Controller

The controller manifests are managed via Kustomize. A shared [base](./deployment_files/base/) defines the common manifests (Namespace, ServiceAccounts, Role, RoleBindings, controller Pod skeleton), and two overlays patch the controller Pod with GPU-vendor-specific node affinity, toleration, and image:

- [AMD overlay](./deployment_files/amd/)
- [NVIDIA overlay](./deployment_files/nvidia/)

The overlays have been tested with A100 Nvidia GPUs and Mi300X AMD GPUs, but they should also work with other GPU shapes in OCI — adjust the `nodeSelectorTerms` in the relevant overlay's `patch.yaml`.

### Provision Controller Infra

```bash
cd deployment_files

kubectl apply -k nvidia
# or
kubectl apply -k amd
```

### Deploy Controller App

A single [controller script](deployment_files/controller.py) is shared between AMD and NVIDIA. The worker container image is passed via `--image`, the K8s GPU resource key is selected via `--gpu-vendor` (`nvidia` or `amd`), and `--rdma` enables RDMA/InfiniBand on NVIDIA worker pods.

```bash
cd deployment_files

kubectl cp controller.py monarch-tests/monarch-controller:/tmp/controller.py
kubectl cp train.py monarch-tests/monarch-controller:/tmp/train.py
```

For the **RDMA scenario (NVIDIA only)**, install the SRIOV plugin before launching the controller.

## Launch Controller

**Non-RDMA scenario (for AMD and NVIDIA):**

```bash
# For Nvidia:
kubectl exec -it monarch-controller -n monarch-tests -- \
    python /tmp/controller.py --provision --num_hosts 2 --gpus_per_host 8 \
    --gpu-vendor nvidia \
    --image ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-cuda12.8-rdma-01

# For AMD:
kubectl exec -it monarch-controller -n monarch-tests -- \
    python /tmp/controller.py --provision --num_hosts 2 --gpus_per_host 8 \
    --gpu-vendor amd \
    --image ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-rocm7.2.1-02
```

**RDMA scenario (for NVIDIA):**

```bash
kubectl exec -it monarch-controller -n monarch-tests -- \
    python /tmp/controller.py --provision --rdma --num_hosts 2 --gpus_per_host 8 \
    --gpu-vendor nvidia \
    --image ghcr.io/dochakov-oci/monarch-oci:monarch0.4.1-cuda12.8-rdma-01
```

## Expected output

```bash
============================================================
Kubernetes DDP Example
Configuration: 4 hosts, 4 GPUs/host
============================================================
No cached job found at path: .monarch/job_state.pkl
Applying current job
Created MonarchMesh 'monarchmesh'
Job has started, connecting to current state
[monarchmesh-0 tcp:10.244.1.230:26600,anon_7-15h6ggB5ZxfV] [7] [GPU7] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_6-1fsNJRrGS5qH] [6] [GPU6] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_3-1D7r8Pte4Sty] [3] [GPU3] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_1-1swFu757FPah] [1] [GPU1] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_2-1GewzfC3tKnz] [2] [GPU2] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_1-1bWDXCme1XVR] [9] [GPU9] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_6-1tMsumG8kx56] [14] [GPU14] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_2-1u8M2piD571X] [10] [GPU10] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_4-1c22fJNPmmPi] [12] [GPU12] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_5-1tqcv6vsgDrP] [13] [GPU13] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_3-12wHDrSHkow5] [11] [GPU11] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_7-1ARnnUk5Wo39] [15] [GPU15] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_4-1wNCWQvfpsDJ] [4] [GPU4] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_5-1F5kjiukThXU] [5] [GPU5] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] monarchmesh-0:263:5786 [0] NCCL INFO ChEpoch 0 | Training checkpoint saved at snapshot.pt
[monarchmesh-1 tcp:10.244.2.16:26600,anon_0-1Asx99KrGXA2] [8] Epoch 0 | Training checkpoint saved at snapshot.pt
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] [GPU0] Epoch 1 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_0-1Asx99KrGXA2] [8] [GPU8] Epoch 1 | Batchsize: 32 | Steps: 15
...
[monarchmesh-1 tcp:10.244.2.16:26600,anon_5-1tqcv6vsgDrP] [13] [GPU13] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_6-1tMsumG8kx56] [14] [GPU14] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_3-12wHDrSHkow5] [11] [GPU11] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_2-1u8M2piD571X] [10] [GPU10] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_2-1GewzfC3tKnz] [2] [GPU2] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_1-1swFu757FPah] [1] [GPU1] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_4-1wNCWQvfpsDJ] [4] [GPU4] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_7-1ARnnUk5Wo39] [15] [GPU15] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_5-1F5kjiukThXU] [5] [GPU5] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_1-1bWDXCme1XVR] [9] [GPU9] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_4-1c22fJNPmmPi] [12] [GPU12] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_7-15h6ggB5ZxfV] [7] [GPU7] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_3-1D7r8Pte4Sty] [3] [GPU3] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_6-1fsNJRrGS5qH] [6] [GPU6] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.16:26600,anon_0-1Asx99KrGXA2] [8] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-1 tcp:10.244.2.16:26600,anon_0-1Asx99KrGXA2] [8] [GPU8] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] [GPU0] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] Accuracy:  0.8100
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] Precision: 0.8669
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] Recall:    0.8100
[monarchmesh-0 tcp:10.244.1.230:26600,anon_0-1enEvfCXmBdp] [0] F1 Score:  0.8018
============================================================
DDP example completed successfully!
============================================================
Deleted MonarchMesh 'monarchmesh'
```

## Cleanup

```bash
kubectl delete -k deployment_files/nvidia
# or
kubectl delete -k deployment_files/amd

helm uninstall monarch-operator monarch-operator/monarch-operator --namespace monarch-system
```

## Dockerfiles

Dockerfiles that were used to build Docker images for this example can be found in [docker dir](./docker).
