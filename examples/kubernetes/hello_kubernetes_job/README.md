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

### Running the Example In-Cluster

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

## Out-of-Cluster Execution

You can also run the same examples shown here from outside the cluster! The client runs locally
and sends messages to the mesh inside the cluster. This relies on opening a port
on a single host in your cluster to use as a proxy for the client's mailbox.
Make sure at least one of your hosts does `run_worker_loop_forever(..., duplex_address=f"tcp://{hostname}:{port}")`.
Let's say this port is 34000. You can choose any port you like.
Make sure that port is exposed. The easiest way is with `kubectl port-forward`:
```
kubectl port-forward -n monarch-tests pod/mesh1-0 34000:34000
```
This will forward localhost port 34000 to the port 34000 on the pod.
You can use any port id locally, it doesn't need to match the port on the pod.

```bash
uv run --no-build-isolation hello_kubernetes_job.py --out-of-cluster --attach-to tcp://localhost:34000
```

Or, for provisioning mode, use:
```bash
uv run --no-build-isolation hello_kubernetes_job.py --out-of-cluster --provision
```

The `--out-of-cluster` flag tells `KubernetesJob` to attach the client's mailbox
to the host. The `--attach-to` flag tells it which host to attach to. In provisioning
mode, we automatically expose the port on the pods, do a port-forward for you,
and attach to that pod.

Some caveats with this approach:
* Your version of monarch locally and on the cluster must match exactly. If they
  don't you may get timeouts or errors on the server decoding messages that may have
  skew between the two versions. If you make local changes to monarch, you must
  send them out in a new container for the pods
* Your local python code may not exist on the remote machines. When we send requests
  to PythonActor, we use `cloudpickle`, which may end up trying to import your
  modules on the mesh. For this reason, it's also best to ensure your container
  contains the same versions of all your local modules and source code.
* Your local client and remote mesh may have different hardware, for example on
  the cluster you may have access to RDMA and on the client you do not. Same goes
  for other hardware like GPUs and CPUs. Be careful not to run things on the client
  that may assume certain hardware. Even functions on torch.Tensor like
  `tensor.to("cuda:0")` will fail if your client doesn't have that device.



## Updating monarch build for images

To update the version of monarch used in the cluster, which includes changes to both
Rust and Python:
```bash
# Make sure to build for python 3.12 since the pytorch base image uses that python version
uv python pin 3.12
# Build the binary distribution, outputs to "dist/" directory.
# --no-build-isolation allows using cached rust builds which speeds up subsequent
# iterations.
uv build --no-build-isolation --wheel

# With docker:
# Build and tag a docker image with your build of monarch. You can update the
# PYTORCH_TAG to use a different base image depending on your needs.
# The nightly dockerfile is used because it uses the package you already built,
# rather than downloading from PyPI.
docker build -f Dockerfile.nightly \
  -t $USER/monarch:local-tag \
  --build-arg PYTORCH_TAG=2.12.0.dev20260224-cuda12.8-cudnn9-runtime \
  --build-arg MONARCH_WHEELS=dist \
  .

# Push so it's available to the kubernetes cluster.
# Either (a) push to a container registry so your cluster can access it.
# Might be slow based on your upload speed and the size of the container.
docker push $USER/monarch:latest
# Or (b) if you have a fully local kubernetes cluster you can change it to
# imagePullPolicy: Never in the manifest and it'll use the image locally. This
# is the fastest iteration speed.

# With podman + kind:
# Same build command, replace "docker" with "podman"
# Save image to archive
podman save localhost/$USER/monarch:local-tag -o /tmp/monarch-image
# Then push to your kind cluster for local dev:
KIND_EXPERIMENTAL_PROVIDER=podman kind load image-archive /tmp/monarch-image -n monarch-cluster

```

Then update the docker images from ghcr.io/meta-pytorch/monarch:latest to use
your new image. Make sure to prepend the service you used for docker login like
ghcr.io or docker.io, and that you have pushed the image first.
In `--provision` mode, you can pass `--image` to customize which docker image is
used for the provisioned hosts.
