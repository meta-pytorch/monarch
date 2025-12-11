# Monarch SkyPilot Integration

This directory contains a standalone integration for running Monarch workloads on **Kubernetes and cloud VMs** via [SkyPilot](https://github.com/skypilot-org/skypilot).

## Overview

`SkyPilotJob` provisions cloud instances (or K8s pods) and starts Monarch workers on them, allowing you to run distributed Monarch actors across multiple machines.

**Supported platforms:**
- Kubernetes (any cluster)
- AWS, GCP, Azure
- Lambda Labs, CoreWeave, RunPod, and [20+ other clouds](https://docs.skypilot.co/en/latest/getting-started/installation.html)

## Installation

```bash
# Install Monarch
pip install torchmonarch-nightly

# Install SkyPilot with your preferred backend
pip install skypilot[kubernetes]  # For Kubernetes
pip install skypilot[aws]         # For AWS
pip install skypilot[gcp]         # For GCP
pip install skypilot[all]         # For all clouds

# Verify SkyPilot setup
sky check
```

## Quick Start

```python
import sky
from skypilot_job import SkyPilotJob
from monarch.actor import Actor, endpoint

class MyActor(Actor):
    @endpoint
    def hello(self) -> str:
        return "Hello from the cloud!"

# Create a SkyPilot job with 2 nodes
job = SkyPilotJob(
    meshes={"workers": 2},
    resources=sky.Resources(
        cloud=sky.Kubernetes(),  # or sky.AWS(), sky.GCP(), etc.
        accelerators="H100:1",
    ),
    cluster_name="my-monarch-cluster",
    idle_minutes_to_autostop=10,
    down_on_autostop=True,
)

# Launch and connect
state = job.state()
hosts = state.workers

# Spawn processes and actors
procs = hosts.spawn_procs(per_host={"gpus": 1})
actors = procs.spawn("my_actors", MyActor)

# Use your actors
results = actors.hello.call().get()
print(results)  # ["Hello from the cloud!", "Hello from the cloud!"]

# Clean up
job.kill()
```

## Running the Example

```bash
cd examples/skypilot

# Run on Kubernetes
python getting_started.py --cloud kubernetes --num-hosts 2

# Run on AWS
python getting_started.py --cloud aws --num-hosts 2 --accelerator "A100:1"

# Run on GCP
python getting_started.py --cloud gcp --num-hosts 2 --accelerator "A100:1"
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `meshes` | Dict mapping mesh names to node counts | Required |
| `resources` | SkyPilot Resources specification | None (SkyPilot defaults) |
| `cluster_name` | Name for the cluster | Auto-generated |
| `monarch_port` | Port for Monarch TCP communication | 22222 |
| `idle_minutes_to_autostop` | Auto-stop after idle time | None |
| `down_on_autostop` | Tear down on autostop vs just stop | False |
| `setup_commands` | Custom setup script | Installs torchmonarch-nightly |
| `workdir` | Local directory to sync to cluster | None |
| `file_mounts` | Additional files to mount | None |

## Default Image

By default, `SkyPilotJob` uses the `pytorch/pytorch:2.9.1-cuda12.8-cudnn9-runtime` Docker image which has compatible system libraries for `torchmonarch-nightly`. Setup time is ~1-2 minutes (just pip install).

## Faster Cold Starts

For faster cold starts (<30s):

**Option 1: Use a pre-built Docker image**
```python
resources = sky.Resources(
    image_id="docker:your-registry/monarch-image:tag",
    accelerators="H100:1",
)
```

**Option 2: Use SkyPilot's cluster reuse**
```python
job = SkyPilotJob(
    ...,
    idle_minutes_to_autostop=30,  # Keep cluster alive
    down_on_autostop=False,       # Just stop, don't terminate
)
```

## Network Requirements

The client must have direct network connectivity to the worker nodes:
- **Kubernetes**: Run the client inside the same cluster (e.g., in a pod)
- **Cloud VMs**: Ensure security groups allow inbound traffic on port 22222

## Troubleshooting

**Check SkyPilot setup:**
```bash
sky check
sky show-gpus
```

**View cluster logs:**
```bash
sky logs <cluster-name>
```

**SSH into a worker:**
```bash
sky ssh <cluster-name>
```

**Clean up clusters:**
```bash
sky down <cluster-name>
sky down --all  # Remove all clusters
```

