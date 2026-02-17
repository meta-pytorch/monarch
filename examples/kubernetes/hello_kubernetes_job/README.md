# Running Monarch on Kubernetes

This directory contains examples for running Monarch on Kubernetes. Monarch provides native Kubernetes integration via MonarchMesh CRD and Operator.
It is also vendor-independent and lets users decide how they want to schedule and orchestrate Monarch hosts.

## Provision Monarch Hosts from Python (Recommended)

The simplest way to run Monarch on Kubernetes: create MonarchMesh CRDs
directly from Python. No YAML manifests for worker pods are needed.

### Prerequisites

- A Kubernetes cluster with [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created:
  ```bash
  kubectl create namespace monarch-tests
  ```

### Deploy the Controller

```bash
kubectl apply -f manifests/hello_provision.yaml
```

This creates a controller pod with RBAC permissions to create MonarchMesh CRDs and watch pods.

### Running the Example

```bash
# Copy the script to the controller
kubectl cp hello_kubernetes_job.py monarch-tests/hello-controller:/tmp/hello_kubernetes_job.py

# Run with --provision to create MonarchMesh CRDs from Python
kubectl exec -it hello-controller -n monarch-tests -- python /tmp/hello_kubernetes_job.py --provision
```

The `--provision` flag tells `KubernetesJob` to create the MonarchMesh CRDs via the K8s API.
When the script finishes, it cleans up by deleting the CRDs.

### Expected Output

```
From MonarchMesh mesh1: hello from mesh1-0
From MonarchMesh mesh2: hello from mesh2-0
```

### Cleanup

```bash
kubectl delete -f manifests/hello_provision.yaml
```

## Provision Monarch Hosts with MonarchMesh YAML Manifests

### Prerequisites

- A Kubernetes cluster with [Monarch CRD and operator](https://github.com/meta-pytorch/monarch-kubernetes/) installed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created:
  ```bash
  kubectl create namespace monarch-tests
  ```

### Deploy the MonarchMesh and Controller

```bash
kubectl apply -f manifests/hello_mesh.yaml
```

### Verify Pods are Running

```bash
# Check worker pods for mesh1
kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name=mesh1

# Check worker pods for mesh2
kubectl get pods -n monarch-tests -l app.kubernetes.io/name=monarch-worker,monarch.pytorch.org/mesh-name=mesh2

# Check controller pod
kubectl get pods -n monarch-tests hello-controller
```

Wait for all pods to show `Running` status.

### Running the Example

Copy and execute the script from the controller pod:

```bash
# Copy the script to the controller
kubectl cp hello_kubernetes_job.py monarch-tests/hello-controller:/tmp/hello_kubernetes_job.py

# Get a shell into the controller
kubectl exec -it hello-controller -n monarch-tests -- /bin/bash

# Inside the controller, run the example
python /tmp/hello_kubernetes_job.py
```

Or run directly without shell:

```bash
kubectl exec -it hello-controller -n monarch-tests -- python /tmp/hello_kubernetes_job.py
```

### Expected Output

```
From MonarchMesh mesh1: hello from mesh1-worker-0
From MonarchMesh mesh2: hello from mesh2-worker-0
```

### Cleanup

```bash
kubectl delete -f manifests/hello_mesh.yaml
```

## Provision Monarch Hosts with Third-Party Scheduler

### Volcano Scheduler

#### Prerequisites

- A Kubernetes cluster with [Volcano scheduler](https://volcano.sh/en/docs/installation/) installed
- `kubectl` configured to access the cluster
- The `monarch-tests` namespace created

#### Provisioning Monarch Hosts with Volcano Scheduler

The `volcano_workers.yaml` manifest launches Monarch worker pods using Volcano's gang scheduling. This ensures all pods in a mesh are scheduled together or not at all.

Volcano automatically adds labels to pods:
- `volcano.sh/job-name` - the Volcano Job name (used for pod discovery)
- `volcano.sh/task-index` - ordinal index (0, 1, 2, ...) for ordering workers

#### Deploy the workers

```bash
kubectl apply -f manifests/volcano_workers.yaml
```

#### Verify pods are running

```bash
kubectl get pods -n monarch-tests -l volcano.sh/job-name=mesh1
kubectl get pods -n monarch-tests -l volcano.sh/job-name=mesh2
```

#### Running the Example

Once the workers are running, execute the example script from a pod within the cluster:

```bash
python hello_kubernetes_job.py --volcano
```

The `--volcano` flag configures `KubernetesJob` to use Volcano's labels:
- `volcano.sh/job-name=<mesh-name>` for pod discovery
- `volcano.sh/task-index` for pod ordering

#### Cleanup

```bash
kubectl delete -f manifests/volcano_workers.yaml
```
