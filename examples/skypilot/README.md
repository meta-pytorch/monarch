# Monarch SkyPilot Integration

This directory contains a standalone integration for running Monarch workloads on **Kubernetes and cloud VMs** via [SkyPilot](https://github.com/skypilot-org/skypilot).

## Overview

`SkyPilotJob` provisions cloud instances (or K8s pods) and starts Monarch workers on them, allowing you to run distributed Monarch actors across multiple machines.

**Supported platforms:**
- Kubernetes (any cluster)
- Hyperscalers: AWS, GCP, Azure
- Neoclouds: CoreWeave, Nebius, and [20+ other clouds](https://docs.skypilot.co/en/latest/getting-started/installation.html)

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

TODO(romilb): Link to SkyPilot docs for k8s setup

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

Example output:
```
$ python skypilot_getting_started.py --num-hosts 2 --gpus-per-host 1 --cluster-name monarch-skypilot-test

============================================================
Monarch Getting Started with SkyPilot
============================================================

Configuration:
  Cloud: kubernetes
  Hosts: 2
  GPUs per host: 1
  Accelerator: H200:1
  Cluster name: monarch-skypilot-test

[1] Creating SkyPilot job...

[2] Launching cluster and starting Monarch workers...
No cached job found at path: .monarch/job_state.pkl
Applying current job
Launching SkyPilot cluster 'monarch-skypilot-test' with 2 nodes
Running on cluster: monarch-skypilot-test
SkyPilot cluster 'monarch-skypilot-test' launched successfully
Waiting for job 1 setup to complete (timeout=300s)...
Job 1 status: JobStatus.SETTING_UP (waited 5s)
Job 1 is now RUNNING (setup complete)
Saving job to cache at .monarch/job_state.pkl
Job has started, connecting to current state
Found 2 nodes ready
Connecting to workers for mesh 'trainers': ['tcp://10.0.4.22:22222', 'tcp://10.0.4.112:22222']
Monarch internal logs are being written to /tmp/sky/monarch_log.log; execution id sky_Dec-11_01:31_653
Waiting for host mesh 'trainers' to initialize...
Host mesh 'trainers' initialized successfully
Host mesh 'trainers' ready
    Got host mesh with extent: {hosts: 2}

[3] Spawning processes on cloud hosts...
    Process mesh extent: {hosts: 2, gpus: 1}

[4] Spawning Counter actors...

[5] Broadcasting increment to all counters...

[6] Getting counter values...
    Counter values: ValueMesh({hosts: 2, gpus: 1}):
  (({'hosts': 0/2, 'gpus': 0/1}, 3), ({'hosts': 1/2, 'gpus': 0/1}, 3))

[7] Spawning Trainer actors...

[8] Performing distributed training step...
    ({'hosts': 0/2, 'gpus': 0/1}, "Trainer {'hosts': 0/2, 'gpus': 0/1} taking a step.")
    ({'hosts': 1/2, 'gpus': 0/1}, "Trainer {'hosts': 1/2, 'gpus': 0/1} taking a step.")

[9] Getting trainer info...
    ({'hosts': 0/2, 'gpus': 0/1}, "Trainer at rank {'hosts': 0/2, 'gpus': 0/1}")
    ({'hosts': 1/2, 'gpus': 0/1}, "Trainer at rank {'hosts': 1/2, 'gpus': 0/1}")

============================================================
Success! Monarch actors ran on SkyPilot cluster!
============================================================

[10] Cleaning up SkyPilot cluster...
Tearing down SkyPilot cluster 'monarch-skypilot-test'
Cluster 'monarch-skypilot-test' terminated
    Cluster terminated.
```

## Default Image

By default, `SkyPilotJob` uses the `pytorch/pytorch:2.9.1-cuda12.8-cudnn9-runtime` Docker image which has compatible system libraries for `torchmonarch-nightly`. TODO(romilb): mention image requirements.

## Faster Cold Starts with SkyPilot's cluster reuse

TODO(romilb): Validate if this works:
```python
job = SkyPilotJob(
    ...,
    idle_minutes_to_autostop=30,  # Keep cluster alive
)
```

TODO(romilb): Benchmark pre-baked container images

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

