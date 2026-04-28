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

There are 2 sets of files in this example:
- [Files for AMD](monarch-oci/examples/kubernetes/oci_kubernetes_job/deployment_files/amd)
- [Files for NVIDA](monarch-oci/examples/kubernetes/oci_kubernetes_job/deployment_files/amd)

The files have been tested with A100 Nvidia GPUs and Mi300X AMD GPUs, but they should also work with other GPU shapes in OCI.

### Provision Controller Infra

```bash
cd examples/k8s_ddp_cnn/deployment_files/nvidia
# or
cd examples/k8s_ddp_cnn/deployment_files/amd

k apply -f provision.yaml
```

### Deploy Controller App

**Non-RDMA scenario (for AMD and NVIDIA):**

```bash
cd examples/k8s_ddp_cnn/deployment_files/nvidia
# or
cd examples/k8s_ddp_cnn/deployment_files/amd

k cp controller.py monarch-tests/monarch-controller:/tmp/controller.py
```

**RDMA scenario (for NVIDIA):**

Step 1: Install SRIOV Plugin.

Step 2: Copy [controller script with additional RDMA configuration](deployment_files/nvidia/controller-rdma.py) to the pod.

```bash
cd examples/k8s_ddp_cnn/deployment_files/nvidia

k cp controller-rdma.py monarch-tests/monarch-controller:/tmp/controller.py
```

## Launch Controller

```bash
k exec -it monarch-controller -n monarch-tests -- \
    python /tmp/controller.py --provision --num_hosts 4 --gpus_per_host 4
```

## Expected output

```bash
Unable to use a TTY - input is not a terminal or the right kind of file
No cached job found at path: .monarch/job_state.pkl
Applying current job
Created MonarchMesh 'monarchmesh'
Job has started, connecting to current state
Monarch internal logs are being written to /tmp/root/monarch_log.log; execution id root_Apr-28_21:20_550
Saving job to cache at .monarch/job_state.pkl
[monarchmesh-1 tcp:10.244.2.84:26600,anon_0-1eBKi4fpAzpf] [4] [GPU4] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_1-1cehXApChcYq] [5] [GPU5] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_3-1buhuxgi6hp9] [15] [GPU15] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_0-1JpmNBwbcq6Y] [12] [GPU12] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_2-1tNT7w8ovs9a] [14] [GPU14] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_1-133cwrtPNsbu] [13] [GPU13] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_2-1vJFZEe8WL5E] [10] [GPU10] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_1-1F54eeEDTeT1] [1] [GPU1] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_2-1xUwtuscTQi2] [2] [GPU2] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_3-1ci25vnP8q7X] [3] [GPU3] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_0-1vKgEAdkLx7C] [8] [GPU8] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_2-1evnAhXrSPN8] [6] [GPU6] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_3-1vtKxYf75b57] [7] [GPU7] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_3-14gwqD6P9BVP] [11] [GPU11] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_1-1FbY9eLqiZJW] [9] [GPU9] Epoch 0 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] [GPU0] Epoch 0 | Batchsize: 32 | Steps: 15
...
[monarchmesh-0 tcp:10.244.2.225:26600,anon_2-1xUwtuscTQi2] [2] [GPU2] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_2-1tNT7w8ovs9a] [14] [GPU14] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_1-133cwrtPNsbu] [13] [GPU13] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_1-1cehXApChcYq] [5] [GPU5] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_1-1F54eeEDTeT1] [1] [GPU1] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_1-1FbY9eLqiZJW] [9] [GPU9] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-3 tcp:10.244.2.85:26600,anon_3-1buhuxgi6hp9] [15] [GPU15] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_3-1ci25vnP8q7X] [3] [GPU3] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_2-1evnAhXrSPN8] [6] [GPU6] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_3-1vtKxYf75b57] [7] [GPU7] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_3-14gwqD6P9BVP] [11] [GPU11] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_2-1vJFZEe8WL5E] [10] [GPU10] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-2 tcp:10.244.2.226:26600,anon_0-1vKgEAdkLx7C] [8] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-3 tcp:10.244.2.85:26600,anon_0-1JpmNBwbcq6Y] [12] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-2 tcp:10.244.2.226:26600,anon_0-1vKgEAdkLx7C] [8] [GPU8] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] [GPU0] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_0-1eBKi4fpAzpf] [4] Epoch 48 | Training checkpoint saved at snapshot.pt
[monarchmesh-3 tcp:10.244.2.85:26600,anon_0-1JpmNBwbcq6Y] [12] [GPU12] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-1 tcp:10.244.2.84:26600,anon_0-1eBKi4fpAzpf] [4] [GPU4] Epoch 49 | Batchsize: 32 | Steps: 15
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] Accuracy:  0.9000
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] Precision: 0.9323
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] Recall:    0.9000
[monarchmesh-0 tcp:10.244.2.225:26600,anon_0-1Etr1u4B8H6v] [0] F1 Score:  0.8891
Deleted MonarchMesh 'monarchmesh'
============================================================
Kubernetes DDP Example
Configuration: 4 hosts, 4 GPUs/host
============================================================
============================================================
DDP example completed successfully!
============================================================
```

## Cleanup

```bash
k delete -f provision.yaml

helm uninstall monarch-operator monarch-operator/monarch-operator --namespace monarch-system
```

## Dockerfiles

Dockerfiles that were used to build Docker images for this example can be found in [docker dir](./docker).
