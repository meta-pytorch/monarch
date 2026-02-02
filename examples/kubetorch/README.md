# Running Monarch on Kubernetes with Kubetorch

This directory contains examples for running Monarch workloads on Kubernetes using [Kubetorch](https://github.com/kubetorch/kubetorch).

## Overview

`kubetorch.monarch.KubernetesJob` is a drop-in replacement for Monarch's `KubernetesJob` that enables three key capabilities:

### 1. Use Monarch from Outside the Cluster

Kubetorch solves the networking complexity of using Monarch primitives (Process Meshes, Actors) from outside the cluster. This isn't trivial because Monarch assumes a stable, fast connection between the driver and HostMeshes - you can't simply port-forward to the hosts.

**How it works:**
- Kubetorch creates an in-cluster gateway that maintains fast communication with the Monarch hosts
- Communication flows over any ingress you have to the cluster (kubectl port-forward by default)
- Configure a load-balancer URL by setting the `KT_API_URL` environment variable
- Unlike Monarch's code serialization (which breaks due to environment differences), Kubetorch uses fast code sync to deploy your code to the gateway and hosts

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Your Laptop (Outside Cluster)                                           │
│                                                                          │
│  from monarch_kubetorch import KubernetesJob                            │
│                                                                          │
│  job = KubernetesJob(compute=kt.Compute(gpu=8, replicas=4))             │
│  state = job.state()                                                     │
│  actors = state.workers.spawn_procs(...).spawn("trainers", MyActor)     │
│  result = actors.train.call(config).get()                               │
│                              │                                           │
└──────────────────────────────┼───────────────────────────────────────────┘
                               │ Any ingress (port-forward, load-balancer)
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                                      │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  MonarchGateway Pod (maintains fast host communication)            │ │
│  │                                                                    │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │ │
│  │  │  Worker 0    │  │  Worker 1    │  │  Worker N    │             │ │
│  │  │  (8 GPUs)    │  │  (8 GPUs)    │  │  (8 GPUs)    │             │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2. Fast Image and Code Sync (Scalable to Thousands of Nodes)

Iterate from your local IDE with instant code updates across your cluster.

- **Differential sync**: Only changed files are transferred, not the entire codebase
- **Peer-to-peer distribution**: Updates propagate through a scalable tree topology, not point-to-point
- **Image updates**: Deploy arbitrary image changes via Dockerfile
- **Works at scale**: Tested with thousands of nodes

This is a significant improvement over Monarch's `sync_workspace`, which was built for Slurm environments and depends on Conda and point-to-point rsyncs.

### 3. Programmatic Compute Provisioning

Launch new compute on-the-fly from inside Python with `kt.Compute`:

```python
import kubetorch as kt
from monarch_kubetorch import KubernetesJob

# Convenient but fully customizable compute specification
compute = kt.Compute(
    gpu=8,
    cpu="32",
    memory="256Gi",
    replicas=4,
    image=kt.Image(name="my-training").pip_install(["torch", "transformers"]),
)

job = KubernetesJob(compute=compute)
```

**Why this matters:**
- Outside the cluster, users are generously authorized via kubeconfig
- Inside the cluster (e.g., launching a training mesh from an RL job), pods have zero authorization by default
- Kubetorch's operator centralizes authorization, allowing fine-grained control while enabling programmatic provisioning
- Works with any scheduling, autoscaling, or queuing you're already using (e.g., Kueue)

## Quickstart

### Prerequisites

1. A Kubernetes cluster with [Kubetorch](https://github.com/kubetorch/kubetorch) installed
2. `kubectl` configured to access the cluster
3. Python environment with kubetorch installed: `pip install kubetorch`
4. Add this directory to your Python path (or copy `monarch_kubetorch/` to your project)

### Basic Example

```python
import kubetorch as kt
from monarch_kubetorch import KubernetesJob
from monarch.actor import Actor, endpoint

# Define your actor
class Trainer(Actor):
    @endpoint
    def train_step(self, batch_size: int) -> str:
        import torch
        device = torch.cuda.current_device() if torch.cuda.is_available() else "cpu"
        return f"Training step on {device} with batch_size={batch_size}"

    @endpoint
    def get_rank(self) -> dict:
        return {"rank": self.rank}

# Create job with compute specification
job = KubernetesJob(
    compute=kt.Compute(
        gpu=1,
        replicas=2,
        image=kt.Image(name="monarch-worker").pip_install(["torchmonarch"]),
    )
)

# Get job state with host mesh
state = job.state()
host_mesh = state.workers

# Spawn processes (one per GPU)
proc_mesh = host_mesh.spawn_procs(per_host={"gpus": 1})

# Spawn actors on processes
trainers = proc_mesh.spawn("trainers", Trainer)

# Call actor methods
results = trainers.train_step.call(batch_size=32).get()
print(results)  # Results from all actors

# Get individual ranks
ranks = trainers.get_rank.call().get()
print(ranks)

# Clean up
job.kill()
```

### Using Pre-allocated Pods

If you have existing pods running Monarch workers, use a label selector:

```python
job = KubernetesJob(
    selector={"app": "my-monarch-workers"},
    namespace="my-namespace",
)
state = job.state()
# ... use state.workers as normal
```

## API Reference

### KubernetesJob

```python
KubernetesJob(
    compute: kt.Compute = None,      # Compute specification for new pods
    selector: dict = None,           # Label selector for existing pods
    name: str = None,                # Job name (auto-generated if not provided)
    namespace: str = "default",      # Kubernetes namespace
    monarch_port: int = 26600,       # Port for Monarch worker communication
    use_websocket: bool = True,      # Use WebSocket for gateway communication
    sync_dirs: list[str] = None,     # Directories to sync (defaults to git root)
)
```

**Methods:**
- `state() -> JobState`: Get job state with HostMesh proxies
- `kill()`: Terminate the job and clean up resources
- Context manager support: `with KubernetesJob(...) as job:`

### JobState

Provides attribute access to named HostMeshes:
- `state.workers`: The default worker HostMesh

### Proxy Classes

The proxy classes mirror Monarch's API:
- `HostMeshProxy.spawn_procs(per_host, name)` → `ProcMeshProxy`
- `ProcMeshProxy.spawn(name, actor_class, *args, **kwargs)` → `ActorMeshProxy`
- `ActorMeshProxy.<endpoint>.call(*args, **kwargs)` → `FutureProxy`
- `FutureProxy.get(timeout)` → result

## Comparison with SkyPilotJob

| Feature | KubernetesJob (Kubetorch) | SkyPilotJob |
|---------|---------------------------|-------------|
| Works from outside cluster | Yes | No (requires driver pod) |
| Code sync | Differential P2P | SkyPilot rsync |
| Compute provisioning | kt.Compute (arbitrary K8s) | SkyPilot Resources |
| Requires SkyPilot | No | Yes |
| Network requirement | Any ingress | Direct pod connectivity |

## Troubleshooting

**Connection issues:**
- Ensure Kubetorch is installed and running: `kubectl get pods -n kubetorch-system`
- Check port forwarding is working: `kubectl port-forward -n kubetorch-system svc/kubetorch-controller 32300:32300`

**Pod scheduling:**
- Check if pods are pending: `kubectl get pods -n <namespace> -l kubetorch.com/service=<job-name>`
- View pod events: `kubectl describe pod <pod-name> -n <namespace>`

**Monarch worker issues:**
- Check worker logs: `kubectl logs <pod-name> -n <namespace>`
- Verify torchmonarch is installed in the image
